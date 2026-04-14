use std::collections::HashMap;

use redis::AsyncCommands;

use riven_core::events::ScrapeRequest;
use riven_core::http::{HttpClient, profiles};
use riven_core::types::{
    CacheCheckFile, CacheCheckResult, DownloadFile, DownloadResult, MediaItemType, build_magnet_uri,
};

use crate::models::{
    StremthruCacheCheck, StremthruLink, StremthruResponse, StremthruTorz, StremthruTorznabResponse,
    StremthruUser, parse_torrent_status,
};

pub const CACHE_CHECK_TTL_SECS: u64 = 60 * 60 * 24;
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
        fetched_results.extend(fetch_cache_check(http, base_url, store, api_key, batch).await?);
    }

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
    http: &HttpClient,
    base_url: &str,
    store: &str,
    api_key: &str,
    hash: &str,
) -> anyhow::Result<Option<StremthruTorz>> {
    let hash = hash.to_lowercase();
    let magnet = build_magnet_uri(&hash);
    let url = format!("{base_url}v0/store/torz");
    tracing::debug!(store, url = %url, "adding torrent via stremthru torz endpoint");

    let response = http
        .send(profiles::STREMTHRU, |client| {
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
        if let Err(error) = delete_torrent(http, base_url, store, api_key, &torrent_id).await {
            tracing::warn!(store, hash, torrent_id, error = %error, "failed to delete torz item");
        }
        return Ok(None);
    }

    Ok(Some(data))
}

async fn fetch_cache_check(
    http: &HttpClient,
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
    let response = http
        .send_data(
            profiles::STREMTHRU,
            Some(format!("{store}:{url}?hash={hash_str}")),
            |client| {
                client
                    .get(&url)
                    .query(&[("hash", hash_str.as_str())])
                    .header("x-stremthru-store-name", store)
                    .header(
                        "x-stremthru-store-authorization",
                        format!("Bearer {api_key}"),
                    )
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
                    size: (f.size > 0).then_some(f.size as u64),
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
    http: &HttpClient,
    base_url: &str,
    store: &str,
    api_key: &str,
    torrent_id: &str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}v0/store/torz/{torrent_id}");
    let response = http
        .send(profiles::STREMTHRU, |client| {
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

pub async fn scrape_torznab(
    http: &HttpClient,
    base_url: &str,
    req: &ScrapeRequest<'_>,
) -> anyhow::Result<HashMap<String, String>> {
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
        .send_data(
            profiles::STREMTHRU,
            Some(format!("{url}?{dedupe_params}")),
            |client| client.get(&url).query(&params),
        )
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        anyhow::bail!("torznab request rejected: HTTP {} - {}", status, body);
    }

    let text = response.text()?;
    let resp: StremthruTorznabResponse = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid torznab response: {e}; body={text}"))?;

    let mut results = HashMap::new();
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
        if !info_hash.is_empty() && !item.title.is_empty() {
            results.insert(info_hash, item.title);
        }
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
    let files = torz
        .files
        .into_iter()
        .map(|f| DownloadFile {
            filename: file_name_or_path(f.name, f.path),
            file_size: f.size.max(0) as u64,
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
    http: &HttpClient,
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> anyhow::Result<String> {
    let url = format!("{base_url}v0/store/torz/link/generate");
    tracing::debug!(store, url = %url, "generating stremthru torz link");
    let response = http
        .send(profiles::STREMTHRU, |client| {
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
    http: &HttpClient,
    base_url: &str,
    store: &str,
    api_key: &str,
) -> anyhow::Result<riven_core::types::DebridUserInfo> {
    let url = format!("{base_url}v0/store/user");
    let resp: StremthruResponse<StremthruUser> = http
        .get_json(profiles::STREMTHRU, format!("{store}:{url}"), |client| {
            client
                .get(&url)
                .header("x-stremthru-store-name", store)
                .header(
                    "x-stremthru-store-authorization",
                    format!("Bearer {api_key}"),
                )
        })
        .await?;
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
    // TorBox: single API call returns all extra fields
    if store == "torbox" {
        let body: serde_json::Value = http
            .get_json(
                profiles::debrid_service(store),
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
            total_downloaded_bytes: data["total_bytes_downloaded"].as_i64(),
            ..Default::default()
        });
    }

    // Real-Debrid: single API call returns all extra fields
    if store == "realdebrid" {
        let body: serde_json::Value = http
            .get_json(
                profiles::debrid_service(store),
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

    // Other stores: fetch only premium_until
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
        .get_json(
            profiles::debrid_service(store),
            format!("{store}:{url}"),
            |client| {
                let request = client.get(&url);
                if let Some(token) = bearer.clone() {
                    request.header("Authorization", token)
                } else {
                    request
                }
            },
        )
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
