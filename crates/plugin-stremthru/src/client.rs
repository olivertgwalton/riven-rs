use redis::AsyncCommands;

use riven_core::types::{
    CacheCheckFile, CacheCheckResult, DownloadFile, DownloadResult, TorrentStatus,
};

use crate::models::{
    StremthruCacheCheck, StremthruLink, StremthruResponse, StremthruTorrent, StremthruUser,
    parse_torrent_status,
};

pub const CACHE_CHECK_TTL_SECS: u64 = 60 * 60 * 24;

pub async fn add_torrent(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> anyhow::Result<StremthruTorrent> {
    let url = format!("{base_url}v0/store/torz");
    let response = client
        .post(&url)
        .header("x-stremthru-store-name", store)
        .header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
        .json(&serde_json::json!({ "link": magnet.to_lowercase() }))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("store rejected torrent: HTTP {} - {}", status, body);
    }

    let text = response.text().await?;
    let resp: StremthruResponse<StremthruTorrent> = serde_json::from_str(&text)
        .map_err(|error| anyhow::anyhow!("invalid add-torrent response: {error}; body={text}"))?;
    let Some(data) = resp.data else {
        anyhow::bail!("{}", describe_empty_add_torrent_response(&text));
    };

    if data.status != "downloaded" {
        if let Some(ref torrent_id) = data.id {
            let _ = remove_torrent(client, base_url, store, api_key, torrent_id).await;
        }
        anyhow::bail!(
            "torrent was in {} state on {}; skipping to avoid empty file list",
            data.status,
            store
        );
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
    hashes: &[String],
) -> anyhow::Result<Vec<CacheCheckResult>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }

    const CHUNK_SIZE: usize = 50;

    let mut futures = Vec::new();
    for chunk in hashes.chunks(CHUNK_SIZE) {
        futures.push(check_cache_chunk(
            client, redis, base_url, store, api_key, chunk,
        ));
    }

    let results = futures::future::join_all(futures).await;
    let mut all_results = Vec::new();
    for res in results {
        all_results.extend(res?);
    }

    Ok(all_results)
}

async fn check_cache_chunk(
    client: &reqwest::Client,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    hashes: &[String],
) -> anyhow::Result<Vec<CacheCheckResult>> {
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

    let hash_str = missing_hashes.join(",");
    let url = format!("{base_url}v0/store/torz/check?hash={hash_str}");
    tracing::debug!(
        store,
        hashes = missing_hashes.len(),
        url_len = url.len(),
        cached = cached_results.len(),
        "checking debrid cache"
    );

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
    let resp: StremthruResponse<StremthruCacheCheck> = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(store, text = %text, error = %e, "stremthru cache check JSON parse error");
            return Err(e.into());
        }
    };

    let fetched_results: Vec<CacheCheckResult> = resp
        .data
        .ok_or_else(|| anyhow::anyhow!("store returned no cache-check data"))?
        .items
        .into_iter()
        .map(|item| CacheCheckResult {
            hash: item.hash,
            status: parse_torrent_status(&item.status),
            files: item
                .files
                .into_iter()
                .enumerate()
                .map(|(i, f)| CacheCheckFile {
                    index: i as u32,
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
