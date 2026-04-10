use redis::AsyncCommands;

use riven_core::types::{
    CacheCheckFile, CacheCheckQuery, CacheCheckResult, DownloadFile, DownloadResult, TorrentStatus,
};

use crate::models::{
    StremthruCacheCheck, StremthruLink, StremthruResponse, StremthruTorrent, StremthruUser,
    parse_torrent_status,
};

pub const CACHE_CHECK_TTL_SECS: u64 = 60 * 60 * 24;
const DEFAULT_RETRY_AFTER_SECS: u64 = 5;

#[derive(Debug)]
pub enum AddTorrentError {
    RateLimited { retry_after_secs: u64 },
    Other(anyhow::Error),
}

impl std::fmt::Display for AddTorrentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited { retry_after_secs } => {
                write!(f, "rate limited; retry after {retry_after_secs}s")
            }
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for AddTorrentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Other(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    value.trim().parse::<u64>().ok()
}

pub async fn add_torrent(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> Result<StremthruTorrent, AddTorrentError> {
    let url = format!("{base_url}v0/store/torz");
    tracing::debug!(store, url = %url, "adding torrent via stremthru torz endpoint");
    let response = client
        .post(&url)
        .header("x-stremthru-store-name", store)
        .header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
        .json(&serde_json::json!({ "link": magnet.to_lowercase() }))
        .send()
        .await
        .map_err(|e| AddTorrentError::Other(e.into()))?;

    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_after_secs =
            parse_retry_after(response.headers()).unwrap_or(DEFAULT_RETRY_AFTER_SECS);
        return Err(AddTorrentError::RateLimited { retry_after_secs });
    }

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AddTorrentError::Other(anyhow::anyhow!(
            "store rejected torrent: HTTP {} - {}",
            status,
            body
        )));
    }

    let text = response
        .text()
        .await
        .map_err(|e| AddTorrentError::Other(e.into()))?;
    let resp: StremthruResponse<StremthruTorrent> = serde_json::from_str(&text).map_err(|e| {
        AddTorrentError::Other(anyhow::anyhow!(
            "invalid add-torrent response: {e}; body={text}"
        ))
    })?;
    let Some(data) = resp.data else {
        return Err(AddTorrentError::Other(anyhow::anyhow!(
            "{}",
            describe_empty_add_torrent_response(&text)
        )));
    };

    if data.status != "downloaded" {
        if let Some(ref torrent_id) = data.id {
            let _ = remove_torrent(client, base_url, store, api_key, torrent_id).await;
        }
        return Err(AddTorrentError::Other(anyhow::anyhow!(
            "torrent was in {} state on {}; skipping to avoid empty file list",
            data.status,
            store
        )));
    }

    Ok(data)
}

fn describe_empty_add_torrent_response(body: &str) -> String {
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
                    format!("store returned no add-torrent data: {code} - {message}")
                }
                (Some(code), None) => {
                    format!("store returned no add-torrent data: {code}; body={body}")
                }
                (None, Some(message)) => {
                    format!("store returned no add-torrent data: {message}")
                }
                (None, None) => format!("store returned no add-torrent data; body={body}"),
            }
        }
        Err(_) => format!("store returned no add-torrent data; body={body}"),
    }
}

pub async fn remove_torrent(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    id: &str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}v0/store/torz/{id}");
    tracing::debug!(store, url = %url, torrent_id = id, "removing torrent via stremthru torz endpoint");
    let response = client
        .delete(&url)
        .header("x-stremthru-store-name", store)
        .header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("failed to remove torrent: HTTP {}", response.status());
    }

    Ok(())
}

pub async fn check_cache(
    client: &reqwest::Client,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    queries: &[CacheCheckQuery],
) -> anyhow::Result<Vec<CacheCheckResult>> {
    if queries.is_empty() {
        return Ok(Vec::new());
    }

    let mut normalized_queries: Vec<CacheCheckQuery> = queries
        .iter()
        .map(|query| CacheCheckQuery {
            hash: query.hash.to_lowercase(),
            magnet: query.magnet.clone(),
        })
        .collect();
    normalized_queries.sort_unstable_by(|a, b| a.hash.cmp(&b.hash));

    let mut conn = redis.clone();
    let mut cached_results = Vec::with_capacity(normalized_queries.len());
    let mut missing_queries = Vec::new();

    for query in &normalized_queries {
        let cache_key = cache_check_key(store, &query.hash);
        let cached: Option<String> = AsyncCommands::get(&mut conn, &cache_key).await.ok();
        match cached {
            Some(payload) => match serde_json::from_str::<CacheCheckResult>(&payload) {
                Ok(result) => cached_results.push(result),
                Err(error) => {
                    tracing::warn!(
                        store,
                        hash = query.hash,
                        error = %error,
                        "invalid cached stremthru cache-check payload"
                    );
                    missing_queries.push(query.clone());
                }
            },
            None => missing_queries.push(query.clone()),
        }
    }

    if missing_queries.is_empty() {
        return Ok(cached_results);
    }

    let hash_str = missing_queries
        .iter()
        .map(|query| query.hash.as_str())
        .collect::<Vec<_>>()
        .join(",");
    tracing::debug!(
        store,
        hashes = missing_queries.len(),
        cached = cached_results.len(),
        "checking debrid cache via stremthru torz endpoint"
    );

    let url = format!("{base_url}v0/store/torz/check?hash={hash_str}");
    tracing::debug!(store, url = %url, "requesting stremthru torz cache check");
    let response = client
        .get(&url)
        .header("x-stremthru-store-name", store)
        .header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("store cache check rejected: HTTP {} - {}", status, body);
    }

    let text = response.text().await?;
    let resp =
        serde_json::from_str::<StremthruResponse<StremthruCacheCheck>>(&text).map_err(|e| {
            anyhow::anyhow!("store returned invalid cache-check data: {e}; body={text}")
        })?;
    let cache_items = resp
        .data
        .ok_or_else(|| anyhow::anyhow!("store returned no cache-check data; body={text}"))?
        .items;

    let fetched_results: Vec<CacheCheckResult> = cache_items
        .into_iter()
        .map(|item| CacheCheckResult {
            hash: item.hash,
            status: parse_torrent_status(&item.status),
            files: item
                .files
                .into_iter()
                .map(|f| CacheCheckFile {
                    index: f.index.max(0) as u32,
                    name: f.name,
                    size: f.size,
                })
                .collect(),
        })
        .collect();

    for result in &fetched_results {
        match serde_json::to_string(result) {
            Ok(payload) => {
                let cache_key = cache_check_key(store, &result.hash.to_lowercase());
                let _: Result<(), _> =
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
    Ok(cached_results)
}

fn cache_check_key(store: &str, hash: &str) -> String {
    format!("plugin:stremthru:cache-check:{store}:{hash}")
}

pub async fn generate_link(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> anyhow::Result<String> {
    let url = format!("{base_url}v0/store/torz/link/generate");
    tracing::debug!(store, url = %url, "generating stremthru torz link");
    let response = client
        .post(&url)
        .header("x-stremthru-store-name", store)
        .header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
        .json(&serde_json::json!({ "link": magnet }))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("store rejected link generation: HTTP {} - {}", status, body);
    }

    let text = response.text().await?;
    let resp: StremthruResponse<StremthruLink> = serde_json::from_str(&text)
        .map_err(|error| anyhow::anyhow!("invalid generate-link response: {error}; body={text}"))?;

    Ok(resp
        .data
        .ok_or_else(|| anyhow::anyhow!("{}", describe_empty_link_response(&text)))?
        .link)
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

pub async fn fetch_user_info(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
) -> anyhow::Result<riven_core::types::DebridUserInfo> {
    let url = format!("{base_url}v0/store/user");
    let resp: StremthruResponse<StremthruUser> = client
        .get(&url)
        .header("x-stremthru-store-name", store)
        .header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
        .send()
        .await?
        .json()
        .await?;
    let user = resp
        .data
        .ok_or_else(|| anyhow::anyhow!("store returned no user data"))?;

    let premium_until = fetch_premium_until(client, store, api_key)
        .await
        .inspect_err(|e| tracing::debug!(store, error = %e, "could not fetch premium_until"))
        .ok()
        .flatten();

    Ok(riven_core::types::DebridUserInfo {
        store: store.to_string(),
        email: user.email,
        subscription_status: user.subscription_status,
        premium_until,
    })
}

async fn fetch_premium_until(
    client: &reqwest::Client,
    store: &str,
    api_key: &str,
) -> anyhow::Result<Option<String>> {
    let (url, bearer, pointer, is_unix): (String, Option<String>, &str, bool) = match store {
        "realdebrid" => (
            "https://api.real-debrid.com/rest/1.0/user".into(),
            Some(format!("Bearer {api_key}")),
            "/expiration",
            false,
        ),
        "torbox" => (
            "https://api.torbox.app/v1/api/user/me".into(),
            Some(format!("Bearer {api_key}")),
            "/data/expiration",
            false,
        ),
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
        _ => return Ok(None),
    };

    let mut req = client.get(&url);
    if let Some(token) = bearer {
        req = req.header("Authorization", token);
    }
    let body: serde_json::Value = req.send().await?.json().await?;

    let expiry = body.pointer(pointer).and_then(|v| {
        if is_unix {
            v.as_i64()
                .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        } else {
            v.as_str().map(str::to_owned)
        }
    });

    Ok(expiry)
}

pub fn download_result_from_torrent(
    store: &str,
    info_hash: &str,
    torrent: StremthruTorrent,
) -> DownloadResult {
    let files = torrent
        .files
        .into_iter()
        .map(|file| DownloadFile {
            filename: file.name,
            file_size: file.size,
            download_url: Some(file.link),
            stream_url: None,
        })
        .collect();

    DownloadResult {
        info_hash: info_hash.to_string(),
        files,
        provider: Some(store.to_string()),
        plugin_name: "stremthru".to_string(),
    }
}

pub fn has_cached_hash(results: &[CacheCheckResult], info_hash: &str) -> bool {
    results.iter().any(|result| {
        result.hash.eq_ignore_ascii_case(info_hash)
            && matches!(
                result.status,
                TorrentStatus::Cached | TorrentStatus::Downloaded
            )
    })
}
