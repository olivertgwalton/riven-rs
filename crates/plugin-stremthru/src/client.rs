use redis::AsyncCommands;

use riven_core::events::ScrapeRequest;
use riven_core::http::HttpClient;
use riven_core::types::{
    CacheCheckFile, CacheCheckResult, DownloadFile, DownloadResult, MediaItemType, build_magnet_uri,
};

use crate::{PROFILE, debrid_service};
use crate::models::{
    StremthruCacheCheck, StremthruLink, StremthruNewz, StremthruNewzAdd, StremthruResponse,
    StremthruTorz, StremthruTorznabResponse, StremthruUser, parse_torrent_status,
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
        // Only cache stable results. Ephemeral statuses (downloading, queued) get re-checked
        // next scrape pass. Unknown is cached to avoid hammering the API but isn't treated as
        // a positive hit for dispatch.
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
    if hash.is_empty() {
        tracing::warn!(store, "skipping add_torrent: empty info_hash");
        return Ok(None);
    }
    let magnet = build_magnet_uri(&hash);
    let url = format!("{base_url}v0/store/torz");
    tracing::debug!(store, url = %url, "adding torrent via stremthru torz endpoint");

    let response = http
        .send(PROFILE, |client| {
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

    // "downloaded" = files present and ready (all stores).
    // "cached"     = in store's instant-download pool; TorBox items whose
    //                DownloadFinished/DownloadPresent flags aren't set on the
    //                initial ADD response come back with this status even
    //                though the files are accessible.
    if !matches!(data.status.as_str(), "downloaded" | "cached") {
        let torrent_id = data.id;
        tracing::debug!(
            store,
            hash,
            torrent_id,
            status = %data.status,
            "torrent not in ready state; deleting torz item"
        );
        if let Err(error) = delete_torrent(http, base_url, store, api_key, &torrent_id).await {
            tracing::warn!(store, hash, torrent_id, error = %error, "failed to delete torz item");
        }
        return Ok(None);
    }

    Ok(Some(data))
}

/// Submits an NZB URL to a Newz-capable store via StremThru, polls until the
/// item is ready, and returns the parsed file list. Returns `Ok(None)` when
/// the store accepted the NZB but never reached a ready state within the
/// poll window — caller treats this as "unavailable" the same way the torz
/// path treats `add_torrent` failures.
pub async fn add_newz(
    http: &HttpClient,
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

    let response = http
        .send(PROFILE, |client| {
            client
                .post(&url)
                .header("x-stremthru-store-name", store)
                .header(
                    "x-stremthru-store-authorization",
                    format!("Bearer {api_key}"),
                )
                .json(&serde_json::json!({ "link": nzb_url }))
        })
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("store newz add rejected: HTTP {} - {}", status, body);
    }

    let text = response.text().await?;
    let resp: StremthruResponse<StremthruNewzAdd> = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid newz add response: {e}; body={text}"))?;
    let Some(added) = resp.data else {
        return Ok(None);
    };

    poll_newz(http, base_url, store, api_key, &added.id, poll_timeout).await
}

async fn poll_newz(
    http: &HttpClient,
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
        let response = http
            .send(PROFILE, |client| {
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
            anyhow::bail!("store newz get rejected: HTTP {} - {}", status, body);
        }

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
    let files = newz
        .files
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
            PROFILE,
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
    base_url: &str,
    store: &str,
    api_key: &str,
    torrent_id: &str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}v0/store/torz/{torrent_id}");
    let response = http
        .send(PROFILE, |client| {
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
        .send_data(
            PROFILE,
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
    let files = torz
        .files
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
        })
        .collect();

    DownloadResult {
        info_hash: info_hash.to_string(),
        files,
        provider: Some(store.to_string()),
        plugin_name: "stremthru".to_string(),
    }
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
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> anyhow::Result<GeneratedLink> {
    // The same /link/generate shape exists for both torz (torrents) and newz
    // (usenet). The link itself is the only signal we have to decide which
    // namespace it belongs to once we're past the initial download.
    let kind = if magnet.contains("/store/newz/") { "newz" } else { "torz" };
    let url = format!("{base_url}v0/store/{kind}/link/generate");
    tracing::debug!(store, kind, url = %url, "generating stremthru link");
    let response = http
        .send(PROFILE, |client| {
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
        if riven_core::stream_link::is_fatal_status_code(status.as_u16()) {
            tracing::warn!(store, %status, "store reports torrent is dead");
            return Ok(GeneratedLink::Dead);
        }
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("store rejected link generation: HTTP {} - {}", status, body);
    }

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
    base_url: &str,
    store: &str,
    api_key: &str,
) -> anyhow::Result<riven_core::types::DebridUserInfo> {
    let url = format!("{base_url}v0/store/user");
    let resp: StremthruResponse<StremthruUser> = http
        .get_json(PROFILE, format!("{store}:{url}"), |client| {
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
        .get_json(
            debrid_service(store),
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
