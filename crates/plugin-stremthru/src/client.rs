use std::sync::Arc;

use redis::AsyncCommands;

use riven_core::events::ScrapeRequest;
use riven_core::http::{HttpClient, HttpResponseData};
use riven_core::types::{
    CacheCheckFile, CacheCheckResult, DownloadFile, DownloadResult, MediaItemType, build_magnet_uri,
};

use std::time::Duration;

use crate::models::{
    StremthruCacheCheck, StremthruErrorResponse, StremthruFile, StremthruLink, StremthruNewz,
    StremthruNewzAdd, StremthruResponse, StremthruTorz, StremthruTorznabResponse, StremthruUser,
    parse_torrent_status,
};
use crate::{PROFILE, debrid_service};

pub const CACHE_CHECK_TTL_SECS: u64 = 60 * 60 * 24;

/// Attach StremThru's per-store routing headers (`x-stremthru-store-name` and
/// the `Bearer` authorization) to a request builder.
trait StoreHeaders {
    fn store_headers(self, store: &str, api_key: &str) -> Self;
}

impl StoreHeaders for reqwest::RequestBuilder {
    fn store_headers(self, store: &str, api_key: &str) -> Self {
        self.header("x-stremthru-store-name", store).header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
    }
}
const CACHE_CHECK_BATCH_SIZE: usize = 500;

fn file_name_or_path(name: String, path: String) -> String {
    if path.is_empty() { name } else { path }
}

pub async fn check_cache(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    hashes: &[String],
) -> anyhow::Result<Vec<CacheCheckResult>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }

    let mut normalized_hashes: Vec<String> =
        hashes.iter().map(|hash| hash.to_lowercase()).collect();
    normalized_hashes.sort_unstable();

    let mut conn = redis.clone();
    let mut cached_results = Vec::with_capacity(normalized_hashes.len());
    let mut missing_hashes = Vec::new();

    for hash in &normalized_hashes {
        let cache_key = cache_check_key(store, hash);
        let cached: Option<String> = AsyncCommands::get(&mut conn, &cache_key).await.ok();
        match cached {
            Some(payload) => match serde_json::from_str::<CacheCheckResult>(&payload) {
                Ok(result) => cached_results.push(result),
                Err(error) => {
                    tracing::warn!(
                        store,
                        hash,
                        error = %error,
                        "invalid cached stremthru cache-check payload"
                    );
                    missing_hashes.push(hash.clone());
                }
            },
            None => missing_hashes.push(hash.clone()),
        }
    }

    if missing_hashes.is_empty() {
        return Ok(cached_results);
    }

    tracing::debug!(
        store,
        hashes = missing_hashes.len(),
        cached = cached_results.len(),
        "checking debrid cache via stremthru torz endpoint"
    );

    let mut fetched_results = Vec::new();
    for batch in missing_hashes.chunks(CACHE_CHECK_BATCH_SIZE) {
        tracing::debug!(
            store,
            batch_hashes = batch.len(),
            "requesting stremthru cache-check batch"
        );
        fetched_results
            .extend(fetch_cache_check(http, redis, base_url, store, api_key, batch).await?);
    }

    for result in &fetched_results {
        if !matches!(
            result.status,
            riven_core::types::TorrentStatus::Cached
                | riven_core::types::TorrentStatus::Downloaded
                | riven_core::types::TorrentStatus::Unknown
        ) {
            continue;
        }
        match serde_json::to_string(result) {
            Ok(payload) => {
                let cache_key = cache_check_key(store, &result.hash.to_lowercase());
                let _result: Result<(), _> =
                    AsyncCommands::set_ex(&mut conn, &cache_key, payload, CACHE_CHECK_TTL_SECS)
                        .await;
            }
            Err(error) => {
                tracing::warn!(
                    store,
                    hash = result.hash,
                    error = %error,
                    "failed to serialize stremthru cache-check payload"
                );
            }
        }
    }

    cached_results.extend(fetched_results);
    for r in &mut cached_results {
        r.store = store.to_string();
    }
    Ok(cached_results)
}

/// Default per-store cooldown when a 429 carries no parseable quota message.
/// Longer than the HTTP layer's 10 s service pause because store 429s here are
/// quota-style (e.g. TorBox's 60 adds/hour), not burst throttles.
const DEFAULT_STORE_COOLDOWN_SECS: u64 = 60;

fn store_cooldown_key(store: &str) -> String {
    format!("plugin:stremthru:store-cooldown:{store}")
}

/// Pause a single store after it rate-limited us: only the throttled store
/// sits out; the rest keep serving requests.
async fn set_store_cooldown(
    redis: &redis::aio::ConnectionManager,
    store: &str,
    duration: Duration,
) {
    let mut conn = redis.clone();
    let _result: Result<(), _> = AsyncCommands::set_ex(
        &mut conn,
        store_cooldown_key(store),
        1u8,
        duration.as_secs().max(1),
    )
    .await;
}

/// Remaining cooldown for a store, if one is active.
pub async fn store_cooldown_remaining(
    redis: &redis::aio::ConnectionManager,
    store: &str,
) -> Option<u64> {
    let mut conn = redis.clone();
    let ttl: i64 = redis::cmd("TTL")
        .arg(store_cooldown_key(store))
        .query_async(&mut conn)
        .await
        .ok()?;
    u64::try_from(ttl).ok().filter(|t| *t > 0)
}

/// A store-scoped request was withheld or rejected because the store is
/// rate-limiting us. Not a store failure: callers should leave the store's
/// health score alone and move on.
#[derive(Debug)]
pub struct StoreRateLimited {
    pub retry_after: Duration,
    /// True when this request triggered the cooldown (fresh 429); false when
    /// an already-active cooldown short-circuited the request before sending.
    pub fresh: bool,
}

impl std::fmt::Display for StoreRateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "store rate limited; retry in {}s",
            self.retry_after.as_secs()
        )
    }
}

impl std::error::Error for StoreRateLimited {}

/// A store-scoped response that cleared the rate-limit gate.
enum StoreSend {
    /// 2xx — body not yet consumed.
    Ok(reqwest::Response),
    /// Non-2xx, non-rate-limit; body already read for diagnostics.
    Rejected {
        status: reqwest::StatusCode,
        body: String,
    },
}

/// Detect a rate-limit rejection and compute its cooldown. Matches both a
/// 429 status and the proxied `TOO_MANY_REQUESTS` error code, since StremThru
/// relays the upstream store's envelope:
/// `{"error":{"code":"TOO_MANY_REQUESTS","message":"60 per 1 hour"}}`.
fn rate_limit_cooldown(status: reqwest::StatusCode, body: &str) -> Option<Duration> {
    let error = StremthruErrorResponse::parse(body).error;
    if status != reqwest::StatusCode::TOO_MANY_REQUESTS && error.code != "TOO_MANY_REQUESTS" {
        return None;
    }
    Some(
        parse_quota_interval(&error.message)
            .unwrap_or(Duration::from_secs(DEFAULT_STORE_COOLDOWN_SECS)),
    )
}

/// Gate every store-scoped request behind the per-store rate-limit cooldown:
/// skip the call while the store is cooling down, and start a cooldown when
/// the store answers with a quota rejection.
async fn send_store<F>(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    store: &str,
    make_request: F,
) -> anyhow::Result<StoreSend>
where
    F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
{
    if let Some(remaining) = store_cooldown_remaining(redis, store).await {
        return Err(StoreRateLimited {
            retry_after: Duration::from_secs(remaining),
            fresh: false,
        }
        .into());
    }

    let response = http.send(PROFILE, make_request).await?;
    if response.status().is_success() {
        return Ok(StoreSend::Ok(response));
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if let Some(retry_after) = rate_limit_cooldown(status, &body) {
        set_store_cooldown(redis, store, retry_after).await;
        return Err(StoreRateLimited {
            retry_after,
            fresh: true,
        }
        .into());
    }

    Ok(StoreSend::Rejected { status, body })
}

/// Dedupe-keyed variant of [`send_store`] for cacheable GETs. Non-2xx,
/// non-rate-limit responses are returned for the caller to interpret, same
/// as before the gate existed.
async fn send_store_data<F>(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    store: &str,
    dedupe_key: String,
    make_request: F,
) -> anyhow::Result<Arc<HttpResponseData>>
where
    F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
{
    if let Some(remaining) = store_cooldown_remaining(redis, store).await {
        return Err(StoreRateLimited {
            retry_after: Duration::from_secs(remaining),
            fresh: false,
        }
        .into());
    }

    let response = http
        .send_data(PROFILE, Some(dedupe_key), make_request)
        .await?;
    if !response.status().is_success()
        && let Some(retry_after) =
            rate_limit_cooldown(response.status(), &response.text().unwrap_or_default())
    {
        set_store_cooldown(redis, store, retry_after).await;
        return Err(StoreRateLimited {
            retry_after,
            fresh: true,
        }
        .into());
    }

    Ok(response)
}

/// Outcome of an `add_torrent` attempt against a single store. Expected store
/// responses are modeled as variants so the dispatch loop can react per class;
/// `Err` is reserved for network/protocol failures and rate limits
/// ([`StoreRateLimited`]).
pub enum AddTorrentOutcome {
    /// Files are present and ready for link generation.
    Ready(StremthruTorz),
    /// Store doesn't have the torrent in a ready state.
    Unavailable,
    /// A previous add already queued this hash at the store; it is being
    /// fetched and isn't a failure (TorBox: HTTP 400 "Download already queued.").
    AlreadyQueued,
    /// Store rejected the request outright (e.g. Debrid-Link `notAddTorrent`).
    Rejected { reason: String },
}

/// Classify a non-2xx torz add response (rate limits never reach here — the
/// send gate converts them to [`StoreRateLimited`]).
fn classify_add_torrent_rejection(status: reqwest::StatusCode, body: &str) -> AddTorrentOutcome {
    let error = StremthruErrorResponse::parse(body).error;

    if error
        .message
        .to_ascii_lowercase()
        .contains("already queued")
    {
        return AddTorrentOutcome::AlreadyQueued;
    }

    AddTorrentOutcome::Rejected {
        reason: format!("HTTP {status} - {body}"),
    }
}

/// Parse a quota message like "60 per 1 hour" into the average refill
/// interval (period / limit) — the soonest a slot is likely to free up.
fn parse_quota_interval(message: &str) -> Option<Duration> {
    let mut parts = message.split_whitespace();
    let limit: u64 = parts.next()?.parse().ok()?;
    if parts.next()? != "per" {
        return None;
    }
    let count: u64 = parts.next()?.parse().ok()?;
    let unit_secs = match parts.next()?.trim_end_matches('s') {
        "second" => 1,
        "minute" => 60,
        "hour" => 3600,
        "day" => 86400,
        _ => return None,
    };
    if limit == 0 {
        return None;
    }
    Some(Duration::from_secs((count * unit_secs / limit).max(1)))
}

/// Adds a torrent to the store and returns the downloaded payload with file links.
/// The torrent must report `downloaded` immediately or it is removed and treated as
/// unavailable for this attempt.
pub async fn add_torrent(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    hash: &str,
) -> anyhow::Result<AddTorrentOutcome> {
    let hash = hash.to_lowercase();
    if hash.is_empty() {
        tracing::warn!(store, "skipping add_torrent: empty info_hash");
        return Ok(AddTorrentOutcome::Unavailable);
    }
    let magnet = build_magnet_uri(&hash);
    let url = format!("{base_url}v0/store/torz");
    tracing::debug!(store, url = %url, "adding torrent via stremthru torz endpoint");

    let response = match send_store(http, redis, store, |client| {
        client
            .post(&url)
            .store_headers(store, api_key)
            .json(&serde_json::json!({ "link": magnet }))
    })
    .await?
    {
        StoreSend::Ok(response) => response,
        StoreSend::Rejected { status, body } => {
            return Ok(classify_add_torrent_rejection(status, &body));
        }
    };

    let text = response.text().await?;
    let resp: StremthruResponse<StremthruTorz> = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid torz response: {e}; body={text}"))?;

    let Some(data) = resp.data else {
        return Ok(AddTorrentOutcome::Unavailable);
    };

    // "cached" is treated as ready: TorBox returns it on the ADD response for
    // items whose files are already accessible (DownloadFinished/Present flags unset).
    if !matches!(data.status.as_str(), "downloaded" | "cached") {
        let torrent_id = data.id;
        tracing::debug!(
            store,
            hash,
            torrent_id,
            status = %data.status,
            "torrent not in ready state; deleting torz item"
        );
        if let Err(error) = delete_torrent(http, redis, base_url, store, api_key, &torrent_id).await
        {
            tracing::warn!(store, hash, torrent_id, error = %error, "failed to delete torz item");
        }
        return Ok(AddTorrentOutcome::Unavailable);
    }

    Ok(AddTorrentOutcome::Ready(data))
}

/// Submits an NZB URL to a Newz-capable store via StremThru, polls until the
/// item is ready, and returns the parsed file list. Returns `Ok(None)` when
/// the store accepted the NZB but never reached a ready state within the
/// poll window — caller treats this as "unavailable" the same way the torz
/// path treats `add_torrent` failures.
pub async fn add_newz(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    nzb_url: &str,
    poll_timeout: std::time::Duration,
) -> anyhow::Result<Option<StremthruNewz>> {
    if nzb_url.is_empty() {
        anyhow::bail!("add_newz: empty nzb_url");
    }
    let url = format!("{base_url}v0/store/newz");
    tracing::debug!(store, url = %url, "adding newz via stremthru");

    let response = match send_store(http, redis, store, |client| {
        client
            .post(&url)
            .store_headers(store, api_key)
            .json(&serde_json::json!({ "link": nzb_url }))
    })
    .await?
    {
        StoreSend::Ok(response) => response,
        StoreSend::Rejected { status, body } => {
            anyhow::bail!("store newz add rejected: HTTP {} - {}", status, body)
        }
    };

    let text = response.text().await?;
    let resp: StremthruResponse<StremthruNewzAdd> = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid newz add response: {e}; body={text}"))?;
    let Some(added) = resp.data else {
        return Ok(None);
    };

    poll_newz(
        http,
        redis,
        base_url,
        store,
        api_key,
        &added.id,
        poll_timeout,
    )
    .await
}

async fn poll_newz(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    newz_id: &str,
    timeout: std::time::Duration,
) -> anyhow::Result<Option<StremthruNewz>> {
    let url = format!("{base_url}v0/store/newz/{newz_id}");
    let started = std::time::Instant::now();
    let mut interval = std::time::Duration::from_secs(3);
    loop {
        let response = match send_store(http, redis, store, |client| {
            client.get(&url).store_headers(store, api_key)
        })
        .await?
        {
            StoreSend::Ok(response) => response,
            StoreSend::Rejected { status, body } => {
                anyhow::bail!("store newz get rejected: HTTP {} - {}", status, body)
            }
        };

        let text = response.text().await?;
        let resp: StremthruResponse<StremthruNewz> = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("invalid newz get response: {e}; body={text}"))?;
        let Some(data) = resp.data else {
            return Ok(None);
        };

        if matches!(data.status.as_str(), "downloaded" | "cached") {
            return Ok(Some(data));
        }

        if matches!(data.status.as_str(), "failed" | "invalid") {
            tracing::debug!(store, newz_id, status = %data.status, "newz item ended in terminal state");
            return Ok(None);
        }

        if started.elapsed() > timeout {
            tracing::debug!(
                store,
                newz_id,
                status = %data.status,
                "newz poll timed out before item became ready"
            );
            return Ok(None);
        }
        tokio::time::sleep(interval).await;
        if interval < std::time::Duration::from_secs(30) {
            interval = (interval * 2).min(std::time::Duration::from_secs(30));
        }
    }
}

pub fn download_result_from_newz(
    store: &str,
    info_hash: &str,
    newz: StremthruNewz,
) -> DownloadResult {
    download_result_from_files(store, info_hash, newz.files)
}

/// Build a `DownloadResult` from the file list shared by the torz and newz
/// store endpoints.
fn download_result_from_files(
    store: &str,
    info_hash: &str,
    files: Vec<StremthruFile>,
) -> DownloadResult {
    let files = files
        .into_iter()
        .map(|f| DownloadFile {
            filename: file_name_or_path(f.name, f.path),
            file_size: f.size.max(0).cast_unsigned(),
            download_url: if f.link.is_empty() {
                None
            } else {
                Some(f.link)
            },
            stream_url: None,
            usenet_info_hash: None,
            usenet_file_index: None,
        })
        .collect();

    DownloadResult {
        info_hash: info_hash.to_string(),
        files,
        provider: Some(store.to_string()),
        plugin_name: "stremthru".to_string(),
    }
}

async fn fetch_cache_check(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    hashes: &[String],
) -> anyhow::Result<Vec<CacheCheckResult>> {
    let hash_str = hashes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(",");
    let url = format!("{base_url}v0/store/torz/check");
    tracing::debug!(store, url = %url, "requesting stremthru torz cache check");
    let response = send_store_data(
        http,
        redis,
        store,
        format!("{store}:{url}?hash={hash_str}"),
        |client| {
            client
                .get(&url)
                .query(&[("hash", hash_str.as_str())])
                .store_headers(store, api_key)
        },
    )
    .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        anyhow::bail!("store cache check rejected: HTTP {} - {}", status, body);
    }

    let text = response.text()?;
    let resp =
        serde_json::from_str::<StremthruResponse<StremthruCacheCheck>>(&text).map_err(|e| {
            anyhow::anyhow!("store returned invalid torz cache-check data: {e}; body={text}")
        })?;
    let items = resp
        .data
        .ok_or_else(|| anyhow::anyhow!("store returned no cache-check data; body={text}"))?
        .items;

    Ok(items
        .into_iter()
        .map(|item| {
            let status = parse_torrent_status(&item.status);
            let files = item
                .files
                .into_iter()
                .map(|f| CacheCheckFile {
                    index: 0,
                    path: if f.path.is_empty() {
                        f.name.clone()
                    } else {
                        f.path.clone()
                    },
                    name: f.name,
                    size: (f.size > 0).then_some(f.size.cast_unsigned()),
                    link: if f.link.is_empty() {
                        None
                    } else {
                        Some(f.link)
                    },
                })
                .collect();
            CacheCheckResult {
                hash: item.hash,
                store: String::new(),
                status,
                files,
            }
        })
        .collect())
}

async fn delete_torrent(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    torrent_id: &str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}v0/store/torz/{torrent_id}");
    match send_store(http, redis, store, |client| {
        client.delete(&url).store_headers(store, api_key)
    })
    .await?
    {
        StoreSend::Ok(_) => Ok(()),
        StoreSend::Rejected { status, body } => {
            anyhow::bail!("store torz delete rejected: HTTP {} - {}", status, body)
        }
    }
}

pub async fn scrape_torznab(
    http: &HttpClient,
    base_url: &str,
    req: &ScrapeRequest<'_>,
) -> anyhow::Result<riven_core::types::ScrapeResponse> {
    let url = format!("{base_url}v0/torznab/api");

    let mut params: Vec<(&str, String)> = vec![("o", "json".to_string())];

    match req.item_type {
        MediaItemType::Movie => {
            params.push(("t", "movie".to_string()));
            params.push(("cat", "2000".to_string()));
        }
        _ => {
            params.push(("t", "tvsearch".to_string()));
            params.push(("cat", "5000".to_string()));
            params.push(("season", req.season_or_1().to_string()));
            if let Some(ep) = req.episode {
                params.push(("ep", ep.to_string()));
            }
        }
    }

    if let Some(imdb_id) = req.imdb_id {
        params.push(("imdbid", imdb_id.to_string()));
    } else {
        params.push(("q", req.title.to_string()));
    }

    tracing::debug!(
        url = %url,
        imdb_id = req.imdb_id,
        title = req.title,
        season = req.season,
        episode = req.episode,
        "requesting stremthru torznab scrape"
    );

    let dedupe_params = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let response = http
        .send_data(PROFILE, Some(format!("{url}?{dedupe_params}")), |client| {
            client.get(&url).query(&params)
        })
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        anyhow::bail!("torznab request rejected: HTTP {} - {}", status, body);
    }

    let text = response.text()?;
    let resp: StremthruTorznabResponse = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid torznab response: {e}; body={text}"))?;

    let mut results = riven_core::types::ScrapeResponse::new();
    for item in resp.channel.items {
        let Some(info_hash) = item.attr.iter().find_map(|a| {
            if a.attributes.name == "infohash" {
                Some(a.attributes.value.to_lowercase())
            } else {
                None
            }
        }) else {
            continue;
        };
        if info_hash.is_empty() || item.title.is_empty() {
            continue;
        }
        let file_size_bytes = item.size.or_else(|| {
            item.attr.iter().find_map(|a| {
                if a.attributes.name == "size" {
                    a.attributes.value.parse::<u64>().ok()
                } else {
                    None
                }
            })
        });
        let entry = match file_size_bytes {
            Some(size) => riven_core::types::ScrapeEntry::with_size(item.title, size),
            None => riven_core::types::ScrapeEntry::new(item.title),
        };
        results.insert(info_hash, entry);
    }

    tracing::info!(
        count = results.len(),
        imdb_id = req.imdb_id,
        title = req.title,
        "torznab scrape complete"
    );
    Ok(results)
}

fn cache_check_key(store: &str, hash: &str) -> String {
    format!("plugin:stremthru:cache-check:{store}:{hash}")
}

pub fn download_result_from_torz(
    store: &str,
    info_hash: &str,
    torz: StremthruTorz,
) -> DownloadResult {
    download_result_from_files(store, info_hash, torz.files)
}

/// Outcome of a stream-link generation attempt against a single store.
pub enum GeneratedLink {
    /// The store minted a fresh stream URL.
    Link(String),
    /// The store reported the torrent is permanently gone (fatal HTTP status).
    /// Distinct from a transient error — the caller should blacklist, not retry.
    Dead,
}

pub async fn generate_link(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> anyhow::Result<GeneratedLink> {
    let kind = if magnet.contains("/store/newz/") {
        "newz"
    } else {
        "torz"
    };
    let url = format!("{base_url}v0/store/{kind}/link/generate");
    tracing::debug!(store, kind, url = %url, "generating stremthru link");
    let response = match send_store(http, redis, store, |client| {
        client
            .post(&url)
            .store_headers(store, api_key)
            .json(&serde_json::json!({ "link": magnet }))
    })
    .await?
    {
        StoreSend::Ok(response) => response,
        StoreSend::Rejected { status, body } => {
            if riven_core::stream_link::is_fatal_status_code(status.as_u16()) {
                tracing::warn!(store, %status, "store reports torrent is dead");
                return Ok(GeneratedLink::Dead);
            }
            anyhow::bail!("store rejected link generation: HTTP {} - {}", status, body);
        }
    };

    let text = response.text().await?;
    let resp: StremthruResponse<StremthruLink> = serde_json::from_str(&text)
        .map_err(|error| anyhow::anyhow!("invalid generate-link response: {error}; body={text}"))?;

    Ok(GeneratedLink::Link(
        resp.data
            .ok_or_else(|| anyhow::anyhow!("{}", describe_empty_link_response(&text)))?
            .link,
    ))
}

fn describe_empty_link_response(body: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(value) => {
            let code = value
                .pointer("/error/code")
                .and_then(serde_json::Value::as_str);
            let message = value
                .pointer("/error/message")
                .and_then(serde_json::Value::as_str);

            match (code, message) {
                (Some(code), Some(message)) => {
                    format!("store returned no link data: {code} - {message}")
                }
                (Some(code), None) => format!("store returned no link data: {code}; body={body}"),
                (None, Some(message)) => format!("store returned no link data: {message}"),
                (None, None) => format!("store returned no link data; body={body}"),
            }
        }
        Err(_) => format!("store returned no link data; body={body}"),
    }
}

#[cfg(test)]
mod tests;

pub async fn fetch_user_info(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
) -> anyhow::Result<riven_core::types::DebridUserInfo> {
    let url = format!("{base_url}v0/store/user");
    let response = send_store_data(http, redis, store, format!("{store}:{url}"), |client| {
        client.get(&url).store_headers(store, api_key)
    })
    .await?;
    if !response.status().is_success() {
        anyhow::bail!(
            "store user request rejected: HTTP {} - {}",
            response.status(),
            response.text().unwrap_or_default()
        );
    }
    let resp: StremthruResponse<StremthruUser> = response.json()?;
    let user = resp
        .data
        .ok_or_else(|| anyhow::anyhow!("store returned no user data"))?;

    let extra = fetch_debrid_extra(http, store, api_key)
        .await
        .inspect_err(|e| tracing::debug!(store, error = %e, "could not fetch debrid extra info"))
        .ok()
        .unwrap_or_default();

    Ok(riven_core::types::DebridUserInfo {
        store: store.to_string(),
        email: user.email,
        username: extra.username,
        subscription_status: user.subscription_status,
        premium_until: extra.premium_until,
        cooldown_until: extra.cooldown_until,
        total_downloaded_bytes: extra.total_downloaded_bytes,
        points: extra.points,
    })
}

#[derive(Default)]
struct DebridExtra {
    premium_until: Option<String>,
    cooldown_until: Option<String>,
    total_downloaded_bytes: Option<i64>,
    username: Option<String>,
    points: Option<i64>,
}

async fn fetch_debrid_extra(
    http: &HttpClient,
    store: &str,
    api_key: &str,
) -> anyhow::Result<DebridExtra> {
    if store == "torbox" {
        let body: serde_json::Value = http
            .get_json(
                debrid_service(store),
                format!("{store}:https://api.torbox.app/v1/api/user/me"),
                |client| {
                    client
                        .get("https://api.torbox.app/v1/api/user/me")
                        .header("Authorization", format!("Bearer {api_key}"))
                },
            )
            .await?;
        let data = &body["data"];
        return Ok(DebridExtra {
            premium_until: data["premium_expires_at"].as_str().map(str::to_owned),
            cooldown_until: data["cooldown_until"].as_str().map(str::to_owned),
            total_downloaded_bytes: data["total_downloaded"].as_i64(),
            ..Default::default()
        });
    }

    if store == "realdebrid" {
        let body: serde_json::Value = http
            .get_json(
                debrid_service(store),
                format!("{store}:https://api.real-debrid.com/rest/1.0/user"),
                |client| {
                    client
                        .get("https://api.real-debrid.com/rest/1.0/user")
                        .header("Authorization", format!("Bearer {api_key}"))
                },
            )
            .await?;
        return Ok(DebridExtra {
            premium_until: body["expiration"].as_str().map(str::to_owned),
            username: body["username"].as_str().map(str::to_owned),
            points: body["points"].as_i64(),
            ..Default::default()
        });
    }

    let (url, bearer, pointer, is_unix): (String, Option<String>, &str, bool) = match store {
        "alldebrid" => (
            "https://api.alldebrid.com/v4/user".into(),
            Some(format!("Bearer {api_key}")),
            "/data/user/premiumUntil",
            true,
        ),
        "debridlink" => (
            "https://debrid-link.com/api/v2/account/infos".into(),
            Some(format!("Bearer {api_key}")),
            "/value/accountExpirationDate",
            true,
        ),
        "premiumize" => (
            format!("https://www.premiumize.me/api/account/info?apikey={api_key}"),
            None,
            "/premium_until",
            true,
        ),
        _ => return Ok(DebridExtra::default()),
    };

    let body: serde_json::Value = http
        .get_json(debrid_service(store), format!("{store}:{url}"), |client| {
            let request = client.get(&url);
            if let Some(token) = bearer.clone() {
                request.header("Authorization", token)
            } else {
                request
            }
        })
        .await?;

    let premium_until = body.pointer(pointer).and_then(|v| {
        if is_unix {
            v.as_i64()
                .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        } else {
            v.as_str().map(str::to_owned)
        }
    });

    Ok(DebridExtra {
        premium_until,
        ..Default::default()
    })
}
