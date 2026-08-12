use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use reqwest::StatusCode;
use riven_core::events::{EventType, HookResponse, ScrapeRequest};
use riven_core::http::{HttpServiceProfile, RateLimitedError};
use riven_core::indexer_stats::{QueryKind, record_query};
use riven_core::nzb::{
    NZB_URL_TTL_SECS, NewznabCaps, NewznabItem, newznab_text_query, nzb_indexer_redis_key,
    nzb_info_hash, nzb_url_redis_key, parse_newznab_caps, parse_newznab_xml,
};
use riven_core::plugin::{FieldType, Plugin, PluginContext, SettingField};
use riven_core::settings::PluginSettings;
use riven_core::types::{MediaItemType, ScrapeEntry, ScrapeResponse};

/// Per-indexer rate-limit profile. Each configured indexer gets its own token
/// bucket keyed by name, so N indexers provide N× the budget instead of all
/// sharing one global "newznab" bucket (which funnelled the entire library
/// through a single 60/min limiter and was the dominant scrape-throughput cap).
/// Newznab indexers enforce limits per API key, so isolating them per indexer
/// matches reality and lets throughput scale with indexer count.
fn indexer_profile(indexer: &Indexer) -> HttpServiceProfile {
    HttpServiceProfile::new_owned(format!("newznab:{}", indexer.name))
        .with_rate_limit(60, Duration::from_secs(60))
}

/// Results asked for per request. Newznab's own default is 100 and most
/// indexers cap a page there regardless of what is asked.
const PAGE_SIZE: usize = 100;
/// Results taken from one indexer for one query before paging stops.
///
/// A single page was the whole search until now, which quietly truncated every
/// query with more than [`PAGE_SIZE`] matches — a long-running show's `tvsearch`
/// routinely does, and the releases that fell off the end were never ranked.
/// Five pages is deep enough to cover those without turning one scrape into an
/// unbounded crawl of an indexer that reports thousands of loose matches.
const MAX_RESULTS: usize = 5 * PAGE_SIZE;
/// How long an indexer's `t=caps` document is trusted. Capabilities change when
/// an indexer is reconfigured, which is rare; comet uses the same six hours.
const CAPS_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Process-local `t=caps` cache, keyed by indexer URL.
///
/// Not in Redis on purpose: caps are cheap to refetch, and a cache that can
/// fail is a cache that can take an indexer out of rotation for reasons that
/// have nothing to do with the indexer.
fn caps_cache() -> &'static std::sync::Mutex<
    std::collections::HashMap<String, (std::time::Instant, std::sync::Arc<NewznabCaps>)>,
> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<String, (std::time::Instant, std::sync::Arc<NewznabCaps>)>,
        >,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// Fetch (or reuse) what this indexer says it supports.
///
/// Never fails: a caps document that cannot be fetched or parsed becomes empty
/// caps, which [`NewznabCaps`] reads as "no constraints known" and which
/// therefore reproduces the previous unconditional behaviour exactly.
async fn caps_for(
    indexer: &Indexer,
    http: &riven_core::http::HttpClient,
) -> std::sync::Arc<NewznabCaps> {
    if let Ok(cache) = caps_cache().lock()
        && let Some((fetched, caps)) = cache.get(&indexer.url)
        && fetched.elapsed() < CAPS_TTL
    {
        return caps.clone();
    }

    let url = format!("{}/api", indexer.url.trim_end_matches('/'));
    let params = [
        ("t", "caps".to_string()),
        ("apikey", indexer.apikey.clone()),
    ];
    record_query(&indexer.name, QueryKind::Caps);
    let body = match http
        .send_data(indexer_profile(indexer), None, |client| {
            client.get(&url).query(&params)
        })
        .await
    {
        Ok(response) if response.status().is_success() => response.text().unwrap_or_default(),
        Ok(response) => {
            tracing::debug!(
                indexer = %indexer.name,
                status = %response.status(),
                "newznab caps unavailable; querying without capability constraints"
            );
            String::new()
        }
        Err(error) => {
            tracing::debug!(
                indexer = %indexer.name,
                %error,
                "newznab caps request failed; querying without capability constraints"
            );
            String::new()
        }
    };

    let caps = std::sync::Arc::new(parse_newznab_caps(&body));
    if let Ok(mut cache) = caps_cache().lock() {
        cache.insert(
            indexer.url.clone(),
            (std::time::Instant::now(), caps.clone()),
        );
    }
    caps
}

/// Narrow a query to what this indexer advertises.
///
/// Returns `None` when the mode itself is unavailable, so the caller can fall
/// straight through to the text search instead of issuing a request the indexer
/// will answer with an error or, worse, with the unconstrained result set.
/// Parameters the indexer does not list are dropped rather than sent: an
/// ignored `tvdbid` does not narrow anything, it just makes an unrelated
/// answer look like a matched one.
fn constrain_to_caps<'a>(
    caps: &NewznabCaps,
    search_type: &'a str,
    params: &[(&'static str, String)],
) -> Option<(&'a str, Vec<(&'static str, String)>)> {
    if !caps.supports_search(search_type) {
        return None;
    }
    let kept: Vec<(&'static str, String)> = params
        .iter()
        .filter(|(key, _)| caps.supports_param(search_type, key))
        .cloned()
        .collect();
    // Every constraint dropped would turn a search for one title into a
    // request for the indexer's entire category.
    if kept.is_empty() && !params.is_empty() {
        return None;
    }
    Some((search_type, kept))
}

#[derive(Default)]
pub struct NewznabPlugin;

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

/// Raw shape of one dictionary entry. Half-configured entries (the user is
/// mid-edit) deserialize fine but are filtered out below rather than failing
/// the whole dictionary.
#[derive(serde::Deserialize)]
struct IndexerJson {
    #[serde(default)]
    url: String,
    #[serde(default)]
    apikey: String,
    #[serde(default)]
    categories: Option<String>,
}

fn parse_indexers_str(raw: &str) -> Option<Vec<Indexer>> {
    let map: std::collections::BTreeMap<String, IndexerJson> =
        serde_json::from_str(raw.trim()).ok()?;
    let indexers: Vec<Indexer> = map
        .into_iter()
        .filter_map(|(name, entry)| {
            let url = entry.url.trim().to_string();
            let apikey = entry.apikey.trim().to_string();
            if url.is_empty() || apikey.is_empty() {
                return None;
            }
            let categories = entry
                .categories
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "2000,5000".to_string());
            Some(Indexer {
                name,
                url,
                apikey,
                categories,
            })
        })
        .collect();
    (!indexers.is_empty()).then_some(indexers)
}

/// Build the (search_type, query_params) tuple for one scrape request. The
/// returned params have NO indexer-specific bits (apikey, cat) so the same
/// base can be reused across every indexer in the fan-out.
fn build_query(request: &ScrapeRequest<'_>) -> Option<(&'static str, Vec<(&'static str, String)>)> {
    let imdb_numeric = request.imdb_id.map(|s| s.trim_start_matches("tt"));

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

/// How long to sit out when an indexer says the daily quota is gone but not
/// when it comes back. The reset is usually midnight in the indexer's own
/// timezone, which it never tells us, so re-probe hourly instead of guessing
/// a wall-clock time — 24 wasted hits a day, against thousands spent
/// rediscovering the same spent key on every scrape.
const QUOTA_PROBE_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Ceiling on a self-reported wait. `LimiterState::pause_for` only ever
/// extends a pause, so one absurd value would park an indexer for good.
const MAX_QUOTA_PAUSE: Duration = Duration::from_secs(12 * 60 * 60);

/// The pause an indexer's error body is asking for, or `None` when the body
/// is not a spent-quota report.
///
/// Newznab signals an exhausted daily API quota as HTTP 500 carrying an
/// `<error>` document — `Request limit reached (5000/5000)`, or
/// `Request limit reached. Retry in 380 minutes.` when it is willing to say
/// how long. Only 429 is a status code we can act on generically, so without
/// this the body reads as a plain 5xx: `execute_with_retry` re-sends it as a
/// transient upstream failure, and every later scrape spends more of a
/// budget that is already gone.
fn quota_exhausted_pause(body: &str) -> Option<Duration> {
    let lower = body.to_ascii_lowercase();
    if !lower.contains("request limit reached") {
        return None;
    }
    let minutes = lower
        .split_once("retry in ")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok());
    Some(minutes.map_or(QUOTA_PROBE_INTERVAL, |m| {
        Duration::from_secs(m * 60).min(MAX_QUOTA_PAUSE)
    }))
}

/// Issue one scrape against one indexer and return its items, paging until the
/// indexer runs out of matches or [`MAX_RESULTS`] is reached. Errors are
/// returned to the caller so the fan-out can log per-indexer failures
/// without poisoning the rest.
///
/// A short page ends the walk. Newznab reports a total in
/// `<newznab:response total=…>`, but not every implementation does and the ones
/// that do are not always honest about it, whereas "this page came back with
/// fewer items than we asked for" is true on every implementation. The cost of
/// not reading the total is one extra request per indexer on an exactly-full
/// last page.
async fn scrape_one(
    indexer: &Indexer,
    request: &ScrapeRequest<'_>,
    search_type: &str,
    base_params: &[(&'static str, String)],
    http: &riven_core::http::HttpClient,
) -> ScrapeOutcome {
    let mut collected: Vec<NewznabItem> = Vec::new();
    let mut paging = Paging::default();
    while let Some(page) = paging.next_page() {
        match scrape_page(indexer, request, search_type, base_params, http, page).await {
            ScrapeOutcome::Ok(items) => {
                paging.absorb(items.len());
                collected.extend(items);
            }
            // A page that fails mid-walk still yields what came before it: a
            // truncated result set beats discarding pages that did arrive.
            outcome if collected.is_empty() => return outcome,
            _ => break,
        }
    }
    ScrapeOutcome::Ok(collected)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Page {
    offset: usize,
    limit: usize,
}

/// Where the walk over one indexer's result set has got to.
///
/// Split out from the loop so the stopping rules are testable without an HTTP
/// server: they are the whole substance of paging, and getting one wrong means
/// either a truncated search or an unbounded crawl.
#[derive(Debug, Default)]
struct Paging {
    taken: usize,
    exhausted: bool,
}

impl Paging {
    /// The next request to issue, or `None` once the indexer is out of matches
    /// or [`MAX_RESULTS`] has been reached.
    fn next_page(&self) -> Option<Page> {
        if self.exhausted || self.taken >= MAX_RESULTS {
            return None;
        }
        Some(Page {
            offset: self.taken,
            limit: PAGE_SIZE.min(MAX_RESULTS - self.taken),
        })
    }

    /// Record what a page actually returned. A page shorter than the one asked
    /// for is the last one — including an empty page, which also protects the
    /// walk from spinning against an indexer that ignores `offset` entirely.
    fn absorb(&mut self, received: usize) {
        let asked = self.next_page().map_or(0, |page| page.limit);
        self.taken += received;
        if received < asked {
            self.exhausted = true;
        }
    }
}

async fn scrape_page(
    indexer: &Indexer,
    request: &ScrapeRequest<'_>,
    search_type: &str,
    base_params: &[(&'static str, String)],
    http: &riven_core::http::HttpClient,
    page: Page,
) -> ScrapeOutcome {
    let Page { offset, limit } = page;
    let base_url = indexer.url.trim_end_matches('/');
    let url = format!("{base_url}/api");

    let mut params: Vec<(&'static str, String)> = base_params.to_vec();
    params.push(("t", search_type.to_string()));
    params.push(("apikey", indexer.apikey.clone()));
    params.push(("cat", indexer.categories.clone()));
    params.push(("limit", limit.to_string()));
    if offset > 0 {
        params.push(("offset", offset.to_string()));
    }

    tracing::debug!(
        indexer = %indexer.name,
        url = %url,
        search_type,
        imdb_id = request.imdb_id,
        tvdb_id = request.tvdb_id,
        "requesting newznab"
    );

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

    record_query(&indexer.name, QueryKind::Search);
    let resp = match http
        .send_data(indexer_profile(indexer), Some(dedupe_key), |client| {
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
        if let Some(pause) = quota_exhausted_pause(&body) {
            tracing::warn!(
                indexer = %indexer.name,
                pause_secs = pause.as_secs(),
                snippet = %snippet,
                "newznab indexer is out of daily API quota; pausing it until the quota resets",
            );
            http.pause_service(&indexer_profile(indexer), pause);
            return ScrapeOutcome::RateLimited(snippet);
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            return ScrapeOutcome::RateLimited(snippet);
        }
        return ScrapeOutcome::Failed(anyhow::anyhow!("newznab returned HTTP {status}: {snippet}"));
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

    fn category(&self) -> &'static str {
        "sources"
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
            SettingField::new("indexers", "Indexers", FieldType::Dictionary)
                .with_key_placeholder("indexer_name")
                .with_add_label("Add indexer")
                .with_description(
                    "Your NZB indexers (NZBGeek, NZBfinder, Prowlarr, etc.). \
                     All indexers are searched at the same time and duplicate results are removed.",
                )
                .with_item_fields(vec![
                    SettingField::new("url", "Indexer URL", FieldType::Url)
                        .required()
                        .with_placeholder("https://nzbgeek.info"),
                    SettingField::new("apikey", "API Key", FieldType::Password).required(),
                    SettingField::new("categories", "Categories", FieldType::Text)
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
        let text_query = newznab_text_query(request);
        let ((search_type, base_params), fallback_query) = match build_query(request) {
            Some(id_query) => (id_query, Some(text_query)),
            None => (text_query, None),
        };

        let http = &ctx.http;
        let scrape_futures = indexers.iter().map(|indexer| {
            let base_params = base_params.clone();
            let fallback_query = fallback_query.clone();
            async move {
                // What this indexer will actually honour. The ID search is only
                // worth issuing if it can be constrained; otherwise go straight
                // to the text query rather than asking a question the indexer
                // answers by ignoring half of it.
                let caps = caps_for(indexer, http).await;
                let mut result = match constrain_to_caps(&caps, search_type, &base_params) {
                    Some((search_type, params)) => {
                        scrape_one(indexer, request, search_type, &params, http).await
                    }
                    None => {
                        tracing::debug!(
                            indexer = %indexer.name,
                            search_type,
                            "indexer does not advertise this search; skipping to text query",
                        );
                        ScrapeOutcome::Ok(Vec::new())
                    }
                };
                if let Some((fb_type, fb_params)) = &fallback_query
                    && matches!(&result, ScrapeOutcome::Ok(items) if items.is_empty())
                    && let Some((fb_type, fb_params)) = constrain_to_caps(&caps, fb_type, fb_params)
                {
                    tracing::debug!(
                        indexer = %indexer.name,
                        imdb_id = request.imdb_id,
                        tvdb_id = request.tvdb_id,
                        q = %fb_params.first().map(|(_, v)| v.as_str()).unwrap_or_default(),
                        "ID search returned no items; retrying with text query",
                    );
                    result = scrape_one(indexer, request, fb_type, &fb_params, http).await;
                }
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
                        let was_new = !results.contains_key(&info_hash);
                        let _result: Result<(), _> = redis_conn
                            .set_ex(
                                nzb_url_redis_key(&info_hash),
                                &item.nzb_url,
                                NZB_URL_TTL_SECS,
                            )
                            .await;
                        // Attribution for the grab this release may earn later.
                        let _result: Result<(), _> = redis_conn
                            .set_ex(
                                nzb_indexer_redis_key(&info_hash),
                                &indexer.name,
                                NZB_URL_TTL_SECS,
                            )
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
                        "newznab indexer rate-limited; skipping",
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

        tracing::debug!(
            count = results.len(),
            indexers = ?per_indexer_counts,
            imdb_id = request.imdb_id,
            tvdb_id = request.tvdb_id,
            "newznab scrape complete"
        );
        Ok(HookResponse::Scrape(results))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both bodies are verbatim from live indexers. The first is the common
    /// one and carries no wait, the second is the only form that does.
    #[test]
    fn reads_a_spent_quota_out_of_a_500_body() {
        assert_eq!(
            quota_exhausted_pause(
                r#"<error code="500" description="Request limit reached (5000/5000)"/>"#
            ),
            Some(QUOTA_PROBE_INTERVAL),
        );
        assert_eq!(
            quota_exhausted_pause(
                r#"<error code="500" description="Request limit reached. Retry in 380 minutes."/>"#
            ),
            Some(Duration::from_secs(380 * 60)),
        );
    }

    /// A self-reported wait is capped: `pause_for` only ever extends a pause,
    /// so an absurd value would otherwise park the indexer permanently.
    #[test]
    fn caps_an_absurd_self_reported_wait() {
        assert_eq!(
            quota_exhausted_pause(
                r#"<error code="500" description="Request limit reached. Retry in 999999 minutes."/>"#
            ),
            Some(MAX_QUOTA_PAUSE),
        );
    }

    /// Ordinary upstream failures must stay `Failed` so they keep their
    /// transient-5xx retry instead of parking the indexer for an hour.
    #[test]
    fn leaves_unrelated_errors_alone() {
        assert_eq!(
            quota_exhausted_pause("<error code=\"200\" description=\"Missing parameter\"/>"),
            None
        );
        assert_eq!(quota_exhausted_pause("502 Bad Gateway"), None);
        assert_eq!(quota_exhausted_pause(""), None);
    }

    /// The regression paging exists for: a query with more matches than one
    /// page must not stop at the first page, as every scrape did before.
    #[test]
    fn paging_walks_full_pages_until_a_short_one() {
        let mut paging = Paging::default();
        assert_eq!(
            paging.next_page(),
            Some(Page {
                offset: 0,
                limit: PAGE_SIZE
            })
        );
        paging.absorb(PAGE_SIZE);
        assert_eq!(
            paging.next_page(),
            Some(Page {
                offset: PAGE_SIZE,
                limit: PAGE_SIZE
            })
        );
        paging.absorb(PAGE_SIZE - 1);
        assert_eq!(paging.next_page(), None, "a short page is the last page");
    }

    #[test]
    fn paging_stops_at_the_result_ceiling() {
        let mut paging = Paging::default();
        let mut requests = 0;
        while let Some(page) = paging.next_page() {
            requests += 1;
            assert!(page.offset + page.limit <= MAX_RESULTS);
            paging.absorb(page.limit);
            assert!(requests <= 32, "paging failed to terminate");
        }
        assert_eq!(paging.taken, MAX_RESULTS);
        assert_eq!(requests, MAX_RESULTS / PAGE_SIZE);
    }

    /// An indexer that ignores `offset` answers every request with the same
    /// full page. The ceiling bounds that, but an empty page must stop the
    /// walk immediately rather than burning the whole budget.
    #[test]
    fn paging_stops_on_an_empty_page() {
        let mut paging = Paging::default();
        paging.absorb(0);
        assert_eq!(paging.next_page(), None);
    }

    fn caps(body: &str) -> NewznabCaps {
        riven_core::nzb::parse_newznab_caps(body)
    }

    #[test]
    fn caps_drop_parameters_an_indexer_does_not_advertise() {
        let caps = caps(
            r#"<caps><searching>
                 <tv-search available="yes" supportedParams="q,tvdbid,season,ep"/>
               </searching></caps>"#,
        );
        let params = vec![
            ("imdbid", "1234567".to_string()),
            ("season", "2".to_string()),
        ];
        let (search_type, kept) = constrain_to_caps(&caps, "tvsearch", &params).unwrap();
        assert_eq!(search_type, "tvsearch");
        assert_eq!(kept, vec![("season", "2".to_string())]);
    }

    /// Dropping every constraint would turn a search for one show into a
    /// request for the indexer's whole TV category — the shape that returns
    /// another series' episodes and makes them look like matches.
    #[test]
    fn caps_refuse_a_query_left_with_no_constraints() {
        let caps = caps(
            r#"<caps><searching>
                 <tv-search available="yes" supportedParams="q"/>
               </searching></caps>"#,
        );
        let params = vec![("tvdbid", "999".to_string())];
        assert!(constrain_to_caps(&caps, "tvsearch", &params).is_none());
    }

    #[test]
    fn caps_refuse_an_unavailable_search_mode() {
        let caps = caps(r#"<caps><searching><search available="yes"/></searching></caps>"#);
        let params = vec![("imdbid", "1234567".to_string())];
        assert!(constrain_to_caps(&caps, "movie", &params).is_none());
    }

    /// Unknown caps must reproduce the previous behaviour exactly, or a caps
    /// endpoint being down would stop scraping altogether.
    #[test]
    fn unknown_caps_pass_the_query_through_untouched() {
        let caps = caps("");
        let params = vec![
            ("tvdbid", "999".to_string()),
            ("season", "2".to_string()),
            ("ep", "4".to_string()),
        ];
        let (search_type, kept) = constrain_to_caps(&caps, "tvsearch", &params).unwrap();
        assert_eq!(search_type, "tvsearch");
        assert_eq!(kept, params);
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
        assert_eq!(parsed[1].categories, "2000,5000");
    }

    #[test]
    fn ignores_indexer_entries_missing_credentials() {
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
        assert!(parse_indexers_str(r#"{"x":{"url":"a"}}"#).is_none());
    }
}
