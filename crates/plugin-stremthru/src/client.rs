use redis::AsyncCommands;
use reqwest::{RequestBuilder, Version, header};

use riven_core::types::{
    CacheCheckFile, CacheCheckResult, DownloadFile, DownloadResult, build_magnet_uri,
};

use crate::models::{
    StremthruCacheCheck, StremthruLink, StremthruResponse, StremthruTorz, StremthruUser,
    parse_torrent_status,
};

pub const CACHE_CHECK_TTL_SECS: u64 = 60 * 60 * 24;

fn file_name_or_path(name: String, path: String) -> String {
    if path.is_empty() { name } else { path }
}

fn stremthru_request(builder: RequestBuilder, store: &str, api_key: &str) -> RequestBuilder {
    builder
        .version(Version::HTTP_2)
        .header(header::ACCEPT, "application/json")
        .header("x-stremthru-store-name", store)
        .header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
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

    let fetched_results =
        fetch_cache_check(client, base_url, store, api_key, &missing_hashes).await?;

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

/// Adds a torrent to the store and returns the downloaded payload with file links.
/// The torrent must report `downloaded` immediately or it is removed and treated as
/// unavailable for this attempt.
pub async fn add_torrent(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    hash: &str,
) -> anyhow::Result<Option<StremthruTorz>> {
    let hash = hash.to_lowercase();
    let magnet = build_magnet_uri(&hash);
    let url = format!("{base_url}v0/store/torz");
    tracing::debug!(store, url = %url, "adding torrent via stremthru torz endpoint");

    let response = riven_core::http::send(|| {
        stremthru_request(client.post(&url), store, api_key).json(&serde_json::json!({
            "link": magnet
        }))
    })
    .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("store torz request rejected: HTTP {} - {}", status, body);
    }

    let text = response.text().await?;
    let resp: StremthruResponse<StremthruTorz> = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid torz response: {e}; body={text}"))?;

    let Some(data) = resp.data else {
        return Ok(None);
    };

    if data.status != "downloaded" {
        let torrent_id = data.id;
        tracing::debug!(
            store,
            hash,
            torrent_id,
            status = %data.status,
            "torrent not in downloaded state; deleting torz item"
        );
        if let Err(error) = delete_torrent(client, base_url, store, api_key, &torrent_id).await {
            tracing::warn!(store, hash, torrent_id, error = %error, "failed to delete torz item");
        }
        return Ok(None);
    }

    Ok(Some(data))
}

async fn fetch_cache_check(
    client: &reqwest::Client,
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
    let response = riven_core::http::send(|| {
        stremthru_request(client.get(&url), store, api_key).query(&[("hash", hash_str.as_str())])
    })
    .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("store cache check rejected: HTTP {} - {}", status, body);
    }

    let text = response.text().await?;
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
                    size: f.size,
                    link: if f.link.is_empty() {
                        None
                    } else {
                        Some(f.link)
                    },
                })
                .collect();
            CacheCheckResult {
                hash: item.hash,
                status,
                files,
            }
        })
        .collect())
}

async fn delete_torrent(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    torrent_id: &str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}v0/store/torz/{torrent_id}");
    let response =
        riven_core::http::send(|| stremthru_request(client.delete(&url), store, api_key)).await?;

    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("store torz delete rejected: HTTP {} - {}", status, body);
    }
}

fn cache_check_key(store: &str, hash: &str) -> String {
    format!("plugin:stremthru:cache-check:{store}:{hash}")
}

pub fn download_result_from_torz(
    store: &str,
    info_hash: &str,
    torz: StremthruTorz,
) -> DownloadResult {
    let files = torz
        .files
        .into_iter()
        .map(|f| DownloadFile {
            filename: file_name_or_path(f.name, f.path),
            file_size: f.size,
            download_url: if f.link.is_empty() {
                None
            } else {
                Some(f.link)
            },
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

pub async fn generate_link(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> anyhow::Result<String> {
    let url = format!("{base_url}v0/store/torz/link/generate");
    tracing::debug!(store, url = %url, "generating stremthru torz link");
    let response = riven_core::http::send(|| {
        stremthru_request(client.post(&url), store, api_key).json(&serde_json::json!({
            "link": magnet
        }))
    })
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
    let resp: StremthruResponse<StremthruUser> =
        stremthru_request(client.get(&url), store, api_key)
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
