//! NZB scraping via StremThru's Newznab-compatible aggregator endpoint
//! (`/v0/newznab/api`). Emits synthetic `nzb-<sha1(url)>` info_hashes and
//! parks the NZB URL in Redis so the download path can recover it when
//! StremThru's `/v0/store/newz` is called.

use redis::AsyncCommands;
use riven_core::events::ScrapeRequest;
use riven_core::http::HttpClient;
use riven_core::nzb::{
    NZB_URL_TTL_SECS, NewznabItem, newznab_text_query, nzb_info_hash, parse_newznab_xml,
};
pub use riven_core::nzb::{is_nzb_info_hash, nzb_url_redis_key};
use riven_core::types::{MediaItemType, ScrapeEntry, ScrapeResponse};

use crate::PROFILE;

/// Build the ID-based (imdbid) search for a scrape request, mirroring the
/// shapes in `riven_core::nzb::newznab_text_query`.
fn build_id_query(req: &ScrapeRequest<'_>) -> Option<(&'static str, Vec<(&'static str, String)>)> {
    let imdb_numeric = req.imdb_id?.trim_start_matches("tt");
    Some(match req.item_type {
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
    })
}

/// Issue one search against the StremThru newznab endpoint and parse the
/// resulting RSS into items.
async fn search_one(
    http: &HttpClient,
    base_url: &str,
    apikey: &str,
    categories: &str,
    search_type: &'static str,
    base_params: &[(&'static str, String)],
) -> anyhow::Result<Vec<NewznabItem>> {
    let mut params = base_params.to_vec();
    params.push(("t", search_type.to_string()));
    params.push(("apikey", apikey.to_string()));
    params.push(("cat", categories.to_string()));
    params.push(("limit", "100".to_string()));

    let url = format!("{}v0/newznab/api", base_url);

    let dedupe = params
        .iter()
        .filter(|(k, _)| *k != "apikey")
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    tracing::debug!(url = %url, query = %dedupe, "requesting stremthru newznab");
    let resp = http
        .send_data(PROFILE, Some(format!("{url}?{dedupe}")), |client| {
            client.get(&url).query(&params)
        })
        .await?;
    resp.error_for_status_ref()
        .map_err(|e| e.context("stremthru newznab"))?;
    let body = resp.text().unwrap_or_default();
    Ok(parse_newznab_xml(&body))
}

pub async fn scrape_newznab(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    apikey: &str,
    categories: &str,
    req: &ScrapeRequest<'_>,
) -> anyhow::Result<ScrapeResponse> {
    // ID-based search is preferred; the text (`q=`) query is the primary when
    // the item has no IMDb id, and a fallback when the ID search returns
    // nothing — sports/yearly content is frequently not ID-mapped on indexers
    // and only reachable by title.
    let text_query = newznab_text_query(req);
    let ((search_type, params), fallback) = match build_id_query(req) {
        Some(id_query) => (id_query, Some(text_query)),
        None => (text_query, None),
    };

    let mut items = search_one(http, base_url, apikey, categories, search_type, &params).await?;
    if items.is_empty()
        && let Some((fb_type, fb_params)) = fallback
    {
        tracing::debug!(
            imdb_id = req.imdb_id,
            q = %fb_params.first().map(|(_, v)| v.as_str()).unwrap_or_default(),
            "stremthru newznab ID search returned no items; retrying with text query",
        );
        items = search_one(http, base_url, apikey, categories, fb_type, &fb_params).await?;
    }

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
            .set_ex(
                nzb_url_redis_key(&info_hash),
                &item.nzb_url,
                NZB_URL_TTL_SECS,
            )
            .await;

        let mut entry = ScrapeEntry::new(item.title);
        if let Some(size) = item.size {
            entry.file_size_bytes = Some(size);
        }
        results.insert(info_hash, entry);
    }

    tracing::info!(
        count = results.len(),
        imdb_id = req.imdb_id,
        "stremthru newznab scrape complete"
    );
    Ok(results)
}
