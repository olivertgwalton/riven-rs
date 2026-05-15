use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use reqwest::StatusCode;
use riven_core::events::{EventType, HookResponse, ScrapeRequest};
use riven_core::http::{HttpServiceProfile, RateLimitedError};
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::{MediaItemType, ScrapeEntry, ScrapeResponse};
use sha1::{Digest, Sha1};

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("newznab").with_rate_limit(60, Duration::from_secs(60));

const NZB_URL_TTL_SECS: u64 = 60 * 60 * 24 * 7;
const NZB_INFO_HASH_PREFIX: &str = "nzb-";

#[derive(Default)]
pub struct NewznabPlugin;

register_plugin!(NewznabPlugin);

/// Build the synthetic info_hash used to identify an NZB throughout the rest
/// of the pipeline. The pipeline treats `info_hash` as opaque, so a stable
/// SHA-1 of the NZB URL is enough to dedupe and round-trip.
pub fn nzb_info_hash(nzb_url: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(nzb_url.as_bytes());
    format!("{NZB_INFO_HASH_PREFIX}{}", hex::encode(hasher.finalize()))
}

pub fn is_nzb_info_hash(info_hash: &str) -> bool {
    info_hash.starts_with(NZB_INFO_HASH_PREFIX)
}

pub fn nzb_url_redis_key(info_hash: &str) -> String {
    format!("riven:nzb:url:{info_hash}")
}

/// One configured Newznab/Torznab-compatible indexer.
#[derive(Debug, Clone)]
struct Indexer {
    /// Short label from the dictionary key. Used purely for log lines.
    name: String,
    url: String,
    apikey: String,
    categories: String,
}

/// Read the configured indexer list out of the `indexers` dictionary
/// setting. The dictionary maps a short label to `{ url, apikey,
/// categories? }`; the user adds entries via the "Add indexer" button in
/// the UI. Returns an empty Vec when nothing is configured.
fn indexers_from_settings(settings: &PluginSettings) -> Vec<Indexer> {
    settings
        .get("indexers")
        .and_then(parse_indexers_str)
        .unwrap_or_default()
}

fn parse_indexers_str(raw: &str) -> Option<Vec<Indexer>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(trimmed).ok()?;
    let map = v.as_object()?;
    let mut indexers = Vec::with_capacity(map.len());
    for (name, entry) in map.iter() {
        let obj = match entry.as_object() {
            Some(o) => o,
            None => continue,
        };
        let url = obj
            .get("url")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let apikey = obj
            .get("apikey")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if url.is_empty() || apikey.is_empty() {
            continue;
        }
        let categories = obj
            .get("categories")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "2000,5000".to_string());
        indexers.push(Indexer {
            name: name.to_string(),
            url,
            apikey,
            categories,
        });
    }
    (!indexers.is_empty()).then_some(indexers)
}

/// Build the (search_type, query_params) tuple for one scrape request. The
/// returned params have NO indexer-specific bits (apikey, cat) so the same
/// base can be reused across every indexer in the fan-out.
fn build_query(
    request: &ScrapeRequest<'_>,
) -> Option<(&'static str, Vec<(&'static str, String)>)> {
    let imdb_numeric = request.imdb_id.map(|s| s.trim_start_matches("tt"));

    // For TV searches prefer `tvdbid` — NZBGeek and most public newznab
    // indexers tag TV releases by TVDB and have spotty IMDb coverage, so
    // an imdbid-only query frequently returns zero results even when the
    // show is well-indexed. Movies stay on imdbid (the standard there).
    let tv_id_param: Option<(&'static str, String)> = request
        .tvdb_id
        .map(|v| ("tvdbid", v.to_string()))
        .or_else(|| imdb_numeric.map(|v| ("imdbid", v.to_string())));

    match request.item_type {
        MediaItemType::Movie => {
            let imdb_numeric = imdb_numeric?;
            Some(("movie", vec![("imdbid", imdb_numeric.to_string())]))
        }
        MediaItemType::Show => {
            let id = tv_id_param?;
            Some(("tvsearch", vec![id]))
        }
        MediaItemType::Season => {
            let id = tv_id_param?;
            Some((
                "tvsearch",
                vec![id, ("season", request.season_or_1().to_string())],
            ))
        }
        MediaItemType::Episode => {
            let id = tv_id_param?;
            Some((
                "tvsearch",
                vec![
                    id,
                    ("season", request.season_or_1().to_string()),
                    ("ep", request.episode_or_1().to_string()),
                ],
            ))
        }
    }
}

/// Outcome of one indexer's scrape. Separates rate-limit (transient,
/// retryable) from generic errors so the caller can promote an
/// all-indexers-rate-limited outcome into `RateLimitedError`.
#[derive(Debug)]
enum ScrapeOutcome {
    Ok(Vec<NewznabItem>),
    RateLimited(String),
    Failed(anyhow::Error),
}

/// Issue one scrape against one indexer and return its items. Errors are
/// returned to the caller so the fan-out can log per-indexer failures
/// without poisoning the rest.
async fn scrape_one(
    indexer: &Indexer,
    request: &ScrapeRequest<'_>,
    search_type: &'static str,
    base_params: &[(&'static str, String)],
    http: &riven_core::http::HttpClient,
) -> ScrapeOutcome {
    let base_url = indexer.url.trim_end_matches('/');
    let url = format!("{base_url}/api");

    let mut params: Vec<(&'static str, String)> = base_params.to_vec();
    params.push(("t", search_type.to_string()));
    params.push(("apikey", indexer.apikey.clone()));
    params.push(("cat", indexer.categories.clone()));
    params.push(("limit", "100".to_string()));

    tracing::debug!(
        indexer = %indexer.name,
        url = %url,
        search_type,
        imdb_id = request.imdb_id,
        tvdb_id = request.tvdb_id,
        "requesting newznab"
    );

    // Dedupe key must reflect the actual query, not just the base URL —
    // otherwise the in-flight deduper collapses every concurrent episode
    // scrape onto whichever (season, ep) arrived first, and every other
    // episode receives the wrong-episode results. Apikey is excluded for
    // log hygiene (the key surfaces in error paths) and because it's
    // invariant across one indexer's lifetime.
    let dedupe_key = {
        let mut key = String::with_capacity(url.len() + params.len() * 16);
        key.push_str(&url);
        let mut first = true;
        for (k, v) in &params {
            if k.eq_ignore_ascii_case("apikey") {
                continue;
            }
            key.push(if first { '?' } else { '&' });
            first = false;
            key.push_str(k);
            key.push('=');
            key.push_str(v);
        }
        key
    };

    let resp = match http
        .send_data(PROFILE, Some(dedupe_key), |client| {
            client.get(&url).query(&params)
        })
        .await
    {
        Ok(r) => r,
        Err(error) => return ScrapeOutcome::Failed(error),
    };
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().unwrap_or_default();
        let snippet = body.chars().take(200).collect::<String>();
        if status == StatusCode::TOO_MANY_REQUESTS {
            return ScrapeOutcome::RateLimited(snippet);
        }
        return ScrapeOutcome::Failed(anyhow::anyhow!(
            "newznab returned HTTP {status}: {snippet}"
        ));
    }
    let body = resp.text().unwrap_or_default();
    let items = parse_newznab_xml(&body);
    if items.is_empty() {
        let logged_query = params
            .iter()
            .map(|(k, v)| {
                if k.eq_ignore_ascii_case("apikey") {
                    format!("{k}=REDACTED")
                } else {
                    format!("{k}={v}")
                }
            })
            .collect::<Vec<_>>()
            .join("&");
        tracing::debug!(
            indexer = %indexer.name,
            status = %status,
            query = %logged_query,
            body_len = body.len(),
            imdb_id = request.imdb_id,
            tvdb_id = request.tvdb_id,
            snippet = %body.chars().take(500).collect::<String>(),
            "newznab returned no items; response snippet",
        );
    }
    ScrapeOutcome::Ok(items)
}

#[async_trait]
impl Plugin for NewznabPlugin {
    fn name(&self) -> &'static str {
        "newznab"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemScrapeRequested]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(!indexers_from_settings(settings).is_empty())
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("indexers", "Indexers", "dictionary")
                .with_key_placeholder("indexer_name")
                .with_add_label("Add indexer")
                .with_description(
                    "One or more Newznab/Torznab-compatible indexers (NZBGeek, \
                     NZBfinder, Prowlarr, NZBHydra2, etc.). Each entry is named \
                     (any short label) and configures one indexer. Scrapes fan \
                     out to every indexer in parallel and results are merged \
                     by NZB URL — duplicates across indexers count once.",
                )
                .with_item_fields(vec![
                    SettingField::new("url", "Indexer URL", "url")
                        .required()
                        .with_placeholder("https://nzbgeek.info"),
                    SettingField::new("apikey", "API Key", "password").required(),
                    SettingField::new("categories", "Categories", "text")
                        .with_default("2000,5000")
                        .with_description(
                            "Comma-separated Newznab category IDs. 2000 = Movies, 5000 = TV.",
                        ),
                ]),
        ]
    }

    async fn on_scrape_requested(
        &self,
        request: &ScrapeRequest<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let indexers = indexers_from_settings(&ctx.settings);
        if indexers.is_empty() {
            return Ok(HookResponse::Empty);
        }
        let Some((search_type, base_params)) = build_query(request) else {
            return Ok(HookResponse::Empty);
        };

        // Fire every indexer in parallel. One indexer's failure (auth,
        // network, malformed XML) is logged and skipped — the others still
        // contribute. Without parallelism, scrape latency would scale
        // linearly with indexer count, and that adds up fast for fan-in
        // flows (a 33-season show with 5 indexers = 165 sequential
        // round-trips).
        let http = &ctx.http;
        let scrape_futures = indexers.iter().map(|indexer| {
            let base_params = base_params.clone();
            async move {
                let result =
                    scrape_one(indexer, request, search_type, &base_params, http).await;
                (indexer, result)
            }
        });
        let outcomes = futures::future::join_all(scrape_futures).await;

        let mut results = ScrapeResponse::new();
        let mut redis_conn = ctx.redis.clone();
        let mut per_indexer_counts: Vec<(String, usize)> = Vec::with_capacity(outcomes.len());
        let mut indexer_count = 0usize;
        let mut rate_limited_count = 0usize;
        let mut ok_count = 0usize;
        for (indexer, outcome) in outcomes {
            indexer_count += 1;
            match outcome {
                ScrapeOutcome::Ok(items) => {
                    ok_count += 1;
                    let mut added = 0usize;
                    for item in items {
                        if item.title.is_empty() || item.nzb_url.is_empty() {
                            continue;
                        }
                        let info_hash = nzb_info_hash(&item.nzb_url);
                        // Same NZB URL hashed by multiple indexers collapses
                        // to the same info_hash, so duplicates across
                        // indexers are deduped here.
                        let was_new = !results.contains_key(&info_hash);
                        // Store the NZB URL in Redis so the SAB downloader
                        // can recover it later. The pipeline only carries
                        // `info_hash` + opaque `magnet`; this sidecar
                        // bridges the indexer → downloader handoff.
                        let _result: Result<(), _> = redis_conn
                            .set_ex(nzb_url_redis_key(&info_hash), &item.nzb_url, NZB_URL_TTL_SECS)
                            .await;

                        let mut entry = ScrapeEntry::new(item.title);
                        if let Some(size) = item.size {
                            entry.file_size_bytes = Some(size);
                        }
                        if was_new {
                            added += 1;
                        }
                        results.insert(info_hash, entry);
                    }
                    per_indexer_counts.push((indexer.name.clone(), added));
                }
                ScrapeOutcome::RateLimited(snippet) => {
                    rate_limited_count += 1;
                    tracing::warn!(
                        indexer = %indexer.name,
                        imdb_id = request.imdb_id,
                        tvdb_id = request.tvdb_id,
                        snippet = %snippet,
                        "newznab indexer rate-limited (429); skipping",
                    );
                    per_indexer_counts.push((indexer.name.clone(), 0));
                }
                ScrapeOutcome::Failed(error) => {
                    tracing::warn!(
                        indexer = %indexer.name,
                        %error,
                        imdb_id = request.imdb_id,
                        tvdb_id = request.tvdb_id,
                        "newznab indexer scrape failed; skipping",
                    );
                    per_indexer_counts.push((indexer.name.clone(), 0));
                }
            }
        }

        // Promote "every indexer rate-limited, no successes" into a
        // RateLimitedError so the scrape framework schedules a delayed retry
        // instead of recording a permanent "no streams" failure. We require
        // zero successful indexers to avoid failing the whole scrape when some
        // indexers contributed results even if others 429'd.
        if rate_limited_count > 0 && ok_count == 0 && indexer_count > 0 {
            tracing::warn!(
                rate_limited_count,
                indexer_count,
                imdb_id = request.imdb_id,
                tvdb_id = request.tvdb_id,
                "all newznab indexers rate-limited; deferring scrape",
            );
            return Err(RateLimitedError.into());
        }

        tracing::info!(
            count = results.len(),
            indexers = ?per_indexer_counts,
            imdb_id = request.imdb_id,
            tvdb_id = request.tvdb_id,
            "newznab scrape complete"
        );
        Ok(HookResponse::Scrape(results))
    }
}

#[derive(Debug, Default)]
struct NewznabItem {
    title: String,
    nzb_url: String,
    size: Option<u64>,
}

/// Hand-rolled lightweight parser for Newznab RSS. Avoids adding a heavy XML
/// dependency just to pluck three fields per `<item>`.
fn parse_newznab_xml(body: &str) -> Vec<NewznabItem> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);

    let mut items = Vec::new();
    let mut current: Option<NewznabItem> = None;
    let mut text_target: Option<&'static str> = None;

    loop {
        match reader.read_event() {
            Err(_) | Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = e.name();
                let local = name.as_ref();
                match local {
                    b"item" => current = Some(NewznabItem::default()),
                    b"title" if current.is_some() => text_target = Some("title"),
                    _ => {}
                }
            }
            Ok(Event::Empty(e)) => {
                let name = e.name();
                let local = name.as_ref();
                let Some(item) = current.as_mut() else { continue };
                match local {
                    b"enclosure" => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"url"
                                && let Ok(v) = attr.unescape_value()
                            {
                                item.nzb_url = v.into_owned();
                            }
                        }
                    }
                    // Some indexers emit <link> as the NZB url; only adopt
                    // when we don't already have one from <enclosure>.
                    b"link" if item.nzb_url.is_empty() => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"href"
                                && let Ok(v) = attr.unescape_value()
                            {
                                item.nzb_url = v.into_owned();
                            }
                        }
                    }
                    b"newznab:attr" | b"torznab:attr" => {
                        let mut name_val: Option<String> = None;
                        let mut value_val: Option<String> = None;
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"name" => {
                                    name_val =
                                        attr.unescape_value().ok().map(|v| v.into_owned());
                                }
                                b"value" => {
                                    value_val =
                                        attr.unescape_value().ok().map(|v| v.into_owned());
                                }
                                _ => {}
                            }
                        }
                        if let (Some(n), Some(v)) = (name_val, value_val)
                            && n.eq_ignore_ascii_case("size")
                        {
                            item.size = v.parse().ok();
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(t)) => {
                if let (Some(target), Some(item)) = (text_target.take(), current.as_mut()) {
                    let text = String::from_utf8_lossy(t.as_ref()).into_owned();
                    if target == "title" && item.title.is_empty() {
                        item.title = text;
                    }
                }
            }
            Ok(Event::CData(t)) => {
                if let (Some(target), Some(item)) = (text_target.take(), current.as_mut()) {
                    let text = String::from_utf8_lossy(t.as_ref()).into_owned();
                    if target == "title" && item.title.is_empty() {
                        item.title = text;
                    }
                }
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                if name.as_ref() == b"item"
                    && let Some(item) = current.take()
                {
                    items.push(item);
                }
                text_target = None;
            }
            _ => {}
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_rss() {
        let body = r#"<?xml version="1.0"?>
        <rss><channel>
        <item>
          <title>Example.Movie.2024.1080p.WEB.x264</title>
          <enclosure url="https://idx.example/get/abc.nzb" length="1234" type="application/x-nzb"/>
          <newznab:attr name="size" value="2147483648"/>
        </item>
        </channel></rss>"#;
        let items = parse_newznab_xml(body);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "Example.Movie.2024.1080p.WEB.x264");
        assert_eq!(items[0].nzb_url, "https://idx.example/get/abc.nzb");
        assert_eq!(items[0].size, Some(2147483648));
    }

    #[test]
    fn info_hash_is_stable() {
        let a = nzb_info_hash("https://example/x.nzb");
        let b = nzb_info_hash("https://example/x.nzb");
        assert_eq!(a, b);
        assert!(is_nzb_info_hash(&a));
    }

    #[test]
    fn parses_indexer_dictionary() {
        let raw = r#"{
            "geek": {"url": "https://nzbgeek.info", "apikey": "abc"},
            "finder": {"url": "https://nzbfinder.ws/", "apikey": "def", "categories": "5000"}
        }"#;
        let mut parsed = parse_indexers_str(raw).expect("non-empty");
        parsed.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].name, "finder");
        assert_eq!(parsed[0].categories, "5000");
        assert_eq!(parsed[1].name, "geek");
        // Default categories when omitted.
        assert_eq!(parsed[1].categories, "2000,5000");
    }

    #[test]
    fn ignores_indexer_entries_missing_credentials() {
        // Entry without apikey is silently skipped, not a hard failure —
        // the user is mid-edit and the half-configured entry shouldn't
        // crash scrapes from the entries that are complete.
        let raw = r#"{
            "good": {"url": "https://nzbgeek.info", "apikey": "abc"},
            "blank": {"url": "https://example.com", "apikey": ""},
            "no-url": {"url": "", "apikey": "k"}
        }"#;
        let parsed = parse_indexers_str(raw).expect("at least one valid");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].name, "good");
    }

    #[test]
    fn empty_or_invalid_dictionary_returns_none() {
        assert!(parse_indexers_str("").is_none());
        assert!(parse_indexers_str("   ").is_none());
        assert!(parse_indexers_str("not json").is_none());
        assert!(parse_indexers_str("{}").is_none());
        // Every entry missing required fields.
        assert!(parse_indexers_str(r#"{"x":{"url":"a"}}"#).is_none());
    }
}
