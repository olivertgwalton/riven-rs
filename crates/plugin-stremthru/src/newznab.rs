//! NZB scraping via StremThru's Newznab-compatible aggregator endpoint
//! (`/v0/newznab/api`). Emits synthetic `nzb-<sha1(url)>` info_hashes and
//! parks the NZB URL in Redis so the download path can recover it when
//! StremThru's `/v0/store/newz` is called.

use redis::AsyncCommands;
use riven_core::events::ScrapeRequest;
use riven_core::http::HttpClient;
use riven_core::nzb::{NZB_URL_TTL_SECS, nzb_info_hash, parse_newznab_xml};
pub use riven_core::nzb::{is_nzb_info_hash, nzb_url_redis_key};
use riven_core::types::{MediaItemType, ScrapeEntry, ScrapeResponse};

use crate::PROFILE;

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
