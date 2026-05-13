//! NZB scraping via StremThru's Newznab-compatible aggregator endpoint
//! (`/v0/newznab/api`). Emits synthetic `nzb-<sha1(url)>` info_hashes and
//! parks the NZB URL in Redis so the download path can recover it when
//! StremThru's `/v0/store/newz` is called.

use redis::AsyncCommands;
use riven_core::events::ScrapeRequest;
use riven_core::http::HttpClient;
use riven_core::types::{MediaItemType, ScrapeEntry, ScrapeResponse};
use sha1::{Digest, Sha1};

use crate::PROFILE;

const NZB_INFO_HASH_PREFIX: &str = "nzb-";
const NZB_URL_TTL_SECS: u64 = 60 * 60 * 24 * 7;

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

pub async fn scrape_newznab(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    apikey: &str,
    categories: &str,
    req: &ScrapeRequest<'_>,
) -> anyhow::Result<ScrapeResponse> {
    let Some(imdb_id) = req.imdb_id else {
        return Ok(ScrapeResponse::new());
    };
    let imdb_numeric = imdb_id.trim_start_matches("tt");

    let (search_type, mut params) = match req.item_type {
        MediaItemType::Movie => ("movie", vec![("imdbid", imdb_numeric.to_string())]),
        MediaItemType::Show => ("tvsearch", vec![("imdbid", imdb_numeric.to_string())]),
        MediaItemType::Season => (
            "tvsearch",
            vec![
                ("imdbid", imdb_numeric.to_string()),
                ("season", req.season_or_1().to_string()),
            ],
        ),
        MediaItemType::Episode => (
            "tvsearch",
            vec![
                ("imdbid", imdb_numeric.to_string()),
                ("season", req.season_or_1().to_string()),
                ("ep", req.episode_or_1().to_string()),
            ],
        ),
    };
    params.push(("t", search_type.to_string()));
    params.push(("apikey", apikey.to_string()));
    params.push(("cat", categories.to_string()));
    params.push(("limit", "100".to_string()));

    let url = format!("{}v0/newznab/api", base_url);
    tracing::debug!(url = %url, search_type, imdb_id, "requesting stremthru newznab");

    let dedupe = params
        .iter()
        .filter(|(k, _)| *k != "apikey")
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let resp = http
        .send_data(PROFILE, Some(format!("{url}?{dedupe}")), |client| {
            client.get(&url).query(&params)
        })
        .await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().unwrap_or_default();
        anyhow::bail!(
            "stremthru newznab returned HTTP {status}: {}",
            body.chars().take(200).collect::<String>()
        );
    }
    let body = resp.text().unwrap_or_default();
    let items = parse_newznab_xml(&body);

    let mut results = ScrapeResponse::new();
    let mut redis_conn = redis.clone();
    for item in items {
        if item.title.is_empty() || item.nzb_url.is_empty() {
            continue;
        }
        let info_hash = nzb_info_hash(&item.nzb_url);
        // Park the NZB URL in Redis so the download path can submit it to
        // /v0/store/newz later.
        let _result: Result<(), _> = redis_conn
            .set_ex(nzb_url_redis_key(&info_hash), &item.nzb_url, NZB_URL_TTL_SECS)
            .await;

        let mut entry = ScrapeEntry::new(item.title);
        if let Some(size) = item.size {
            entry.file_size_bytes = Some(size);
        }
        results.insert(info_hash, entry);
    }

    tracing::info!(count = results.len(), imdb_id, "stremthru newznab scrape complete");
    Ok(results)
}

#[derive(Debug, Default)]
struct NewznabItem {
    title: String,
    nzb_url: String,
    size: Option<u64>,
}

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
}
