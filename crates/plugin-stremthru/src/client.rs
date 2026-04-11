use redis::AsyncCommands;

use riven_core::types::{
    CacheCheckFile, CacheCheckQuery, CacheCheckResult, DownloadFile, DownloadResult, TorrentStatus,
    build_magnet_uri,
};

use crate::models::{
    StremthruCacheCheck, StremthruLink, StremthruResponse, StremthruTorz, StremthruUser,
    parse_torrent_status,
};

pub const CACHE_CHECK_TTL_SECS: u64 = 60 * 60 * 24;

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

    let fetched_results = fetch_cache_check(client, base_url, store, api_key, &hash_str).await?;

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

/// Adds a torrent to the store and returns the downloaded result with file links.
/// Mirrors the TypeScript plugin's torz flow: a newly-added torrent must report
/// `downloaded` immediately or it is removed and treated as unavailable.
pub async fn check_cache_live(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    hash: &str,
) -> anyhow::Result<Option<CacheCheckResult>> {
    let hash = hash.to_lowercase();
    let magnet = build_magnet_uri(&hash);
    let url = format!("{base_url}v0/store/torz");
    tracing::debug!(store, url = %url, "adding torrent via stremthru torz endpoint");

    let response = riven_core::http::send(|| {
        client
            .post(&url)
            .header("x-stremthru-store-name", store)
            .header(
                "x-stremthru-store-authorization",
                format!("Bearer {api_key}"),
            )
            .json(&serde_json::json!({ "link": magnet }))
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
        if let Err(error) = delete_torz(client, base_url, store, api_key, &torrent_id).await {
            tracing::warn!(store, hash, torrent_id, error = %error, "failed to delete torz item");
        }
        return Ok(None);
    }

    let files = data
        .files
        .into_iter()
        .map(|f| CacheCheckFile {
            index: f.index.max(0) as u32,
            name: f.name,
            size: f.size,
            link: if f.link.is_empty() {
                None
            } else {
                Some(f.link)
            },
        })
        .collect();

    Ok(Some(CacheCheckResult {
        hash,
        status: TorrentStatus::Cached,
        files,
    }))
}

async fn fetch_cache_check(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    hash_str: &str,
) -> anyhow::Result<Vec<CacheCheckResult>> {
    let url = format!("{base_url}v0/store/torz/check?hash={hash_str}");
    tracing::debug!(store, url = %url, "requesting stremthru torz cache check");
    let response = riven_core::http::send(|| {
        client
            .get(&url)
            .header("x-stremthru-store-name", store)
            .header(
                "x-stremthru-store-authorization",
                format!("Bearer {api_key}"),
            )
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
                    index: f.index.max(0) as u32,
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

async fn delete_torz(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    torrent_id: &str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}v0/store/torz/{torrent_id}");
    let response = riven_core::http::send(|| {
        client
            .delete(&url)
            .header("x-stremthru-store-name", store)
            .header(
                "x-stremthru-store-authorization",
                format!("Bearer {api_key}"),
            )
    })
    .await?;

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

pub fn download_result_from_cache_check(
    store: &str,
    info_hash: &str,
    result: CacheCheckResult,
) -> DownloadResult {
    let files = result
        .files
        .into_iter()
        .map(|f| DownloadFile {
            filename: f.name,
            file_size: f.size,
            download_url: f.link,
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
        client
            .post(&url)
            .header("x-stremthru-store-name", store)
            .header(
                "x-stremthru-store-authorization",
                format!("Bearer {api_key}"),
            )
            .json(&serde_json::json!({ "link": magnet }))
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
