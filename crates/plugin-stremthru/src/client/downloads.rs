use super::*;

const CACHE_CHECK_TTL_SECS: u64 = 60 * 60 * 24;

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
pub(super) fn classify_add_torrent_rejection(
    status: reqwest::StatusCode,
    body: &str,
) -> AddTorrentOutcome {
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
pub(super) fn parse_quota_interval(message: &str) -> Option<Duration> {
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

pub(super) fn cache_check_key(store: &str, hash: &str) -> String {
    format!("plugin:stremthru:cache-check:{store}:{hash}")
}

pub fn download_result_from_torz(
    store: &str,
    info_hash: &str,
    torz: StremthruTorz,
) -> DownloadResult {
    download_result_from_files(store, info_hash, torz.files)
}
