use async_trait::async_trait;
use redis::AsyncCommands;
use serde::Deserialize;
use std::collections::HashMap;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

const DEFAULT_URL: &str = "https://stremthru.13377001.xyz/";
const CACHE_CHECK_TTL_SECS: u64 = 60 * 60 * 24;

const STORE_NAMES: &[&str] = &[
    "realdebrid",
    "alldebrid",
    "debrider",
    "debridlink",
    "easydebrid",
    "offcloud",
    "pikpak",
    "premiumize",
    "torbox",
];

#[derive(Default)]
pub struct StremthruPlugin;

register_plugin!(StremthruPlugin);

fn get_configured_stores(settings: &PluginSettings) -> Vec<(&str, String)> {
    STORE_NAMES
        .iter()
        .filter_map(|name| {
            let key = format!("{name}apikey");
            settings.get(&key).map(|k| (*name, k.to_string()))
        })
        .collect()
}

#[async_trait]
impl Plugin for StremthruPlugin {
    fn name(&self) -> &'static str {
        "stremthru"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::MediaItemDownloadRequested,
            EventType::MediaItemDownloadCacheCheckRequested,
            EventType::MediaItemDownloadProviderListRequested,
            EventType::MediaItemScrapeRequested,
            EventType::MediaItemStreamLinkRequested,
            EventType::DebridUserInfoRequested,
        ]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        Ok(!get_configured_stores(settings).is_empty())
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("stremthruurl", "StremThru URL", "url")
                .with_default("https://stremthru.13377001.xyz/")
                .with_placeholder("https://stremthru.13377001.xyz/"),
            SettingField::new("realdebridapikey", "Real-Debrid API Key", "password"),
            SettingField::new("alldebridapikey", "AllDebrid API Key", "password"),
            SettingField::new("debriderapikey", "Debrider API Key", "password"),
            SettingField::new("debridlinkapikey", "DebridLink API Key", "password"),
            SettingField::new("easydebridapikey", "EasyDebrid API Key", "password"),
            SettingField::new("offcloudapikey", "OffCloud API Key", "password"),
            SettingField::new("pikpakapikey", "PikPak API Key", "password"),
            SettingField::new("premiumizeapikey", "Premiumize API Key", "password"),
            SettingField::new("torboxapikey", "TorBox API Key", "password"),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let base_url = ctx.settings.get_or("stremthruurl", DEFAULT_URL);
        let stores = get_configured_stores(&ctx.settings);

        if let Some(request) = event.scrape_request() {
            let Some(imdb_id) = request.imdb_id else {
                return Ok(HookResponse::Empty);
            };

            let cat = match request.item_type {
                MediaItemType::Movie => "2000",
                _ => "5000",
            };

            let t = match request.item_type {
                MediaItemType::Movie => "movie",
                _ => "tvsearch",
            };

            let mut url =
                format!("{base_url}v0/torznab/api?o=json&imdbid={imdb_id}&t={t}&cat={cat}");
            if let Some(s) = request.season {
                url.push_str(&format!("&season={s}"));
            }
            if let Some(e) = request.episode {
                url.push_str(&format!("&ep={e}"));
            }

            let mut streams = HashMap::new();
            match ctx.http_client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        match resp.text().await {
                            Ok(body) => {
                                tracing::debug!(
                                    url,
                                    body_len = body.len(),
                                    "torznab raw response received"
                                );
                                match serde_json::from_str::<StremthruTorznabResponse>(&body) {
                                    Ok(data) => {
                                        let count = data.channel.items.len();
                                        tracing::debug!(url, count, "torznab items parsed");
                                        for item in data.channel.items {
                                            let info_hash = item
                                                .attr
                                                .iter()
                                                .find(|a| a.attributes.name == "infohash")
                                                .map(|a| &a.attributes.value);
                                            if let (Some(hash), title) = (info_hash, item.title) {
                                                if !hash.is_empty() && !title.is_empty() {
                                                    streams.insert(hash.to_lowercase(), title);
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, body = %body, "failed to parse torznab json")
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to read torznab response body")
                            }
                        }
                    } else {
                        tracing::warn!(status = %status, "torznab request failed");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "torznab scrape failed");
                }
            }
            tracing::debug!(url, found = streams.len(), "scrape complete");

            return Ok(HookResponse::Scrape(streams));
        }

        match event {
            RivenEvent::MediaItemDownloadRequested {
                id: _,
                info_hash,
                magnet,
            } => {
                let mut any_network_error = false;
                for (store, api_key) in &stores {
                    let is_cached = match check_cache(
                        &ctx.http_client,
                        &ctx.redis,
                        &base_url,
                        store,
                        api_key,
                        &[info_hash.clone()],
                    )
                    .await
                    {
                        Ok(results) => results.iter().any(|r| {
                            r.hash.eq_ignore_ascii_case(info_hash)
                                && matches!(
                                    r.status,
                                    TorrentStatus::Cached | TorrentStatus::Downloaded
                                )
                        }),
                        Err(e) => {
                            let is_network = e
                                .downcast_ref::<reqwest::Error>()
                                .map(|re| re.is_connect() || re.is_timeout() || re.is_request())
                                .unwrap_or(false);
                            if is_network {
                                any_network_error = true;
                            }
                            tracing::warn!(store, error = %e, "stremthru cache check failed");
                            continue;
                        }
                    };

                    if !is_cached {
                        tracing::debug!(store, info_hash, "torrent not cached in store; skipping");
                        continue;
                    }

                    match add_torrent(&ctx.http_client, &base_url, store, api_key, magnet).await {
                        Ok(result) => {
                            let files = result
                                .files
                                .into_iter()
                                .map(|f| DownloadFile {
                                    filename: f.name,
                                    file_size: f.size,
                                    download_url: Some(f.link),
                                    stream_url: None,
                                })
                                .collect();

                            return Ok(HookResponse::Download(Box::new(DownloadResult {
                                info_hash: info_hash.clone(),
                                files,
                                provider: Some(store.to_string()),
                                plugin_name: "stremthru".to_string(),
                            })));
                        }
                        Err(e) => {
                            let is_network = e
                                .downcast_ref::<reqwest::Error>()
                                .map(|re| re.is_connect() || re.is_timeout() || re.is_request())
                                .unwrap_or(false);
                            if is_network {
                                any_network_error = true;
                            }
                            tracing::warn!(store, error = %e, "stremthru add torrent failed");
                        }
                    }
                }
                if any_network_error {
                    anyhow::bail!("network error contacting store")
                }
                Ok(HookResponse::DownloadStreamUnavailable)
            }

            RivenEvent::MediaItemDownloadCacheCheckRequested { hashes } => {
                let mut futures = Vec::new();
                for (store, api_key) in &stores {
                    futures.push(check_cache(
                        &ctx.http_client,
                        &ctx.redis,
                        &base_url,
                        store,
                        api_key,
                        hashes,
                    ));
                }

                let results = futures::future::join_all(futures).await;
                let mut all_results = Vec::new();
                for res in results {
                    match res {
                        Ok(items) => all_results.extend(items),
                        Err(e) => tracing::warn!(error = %e, "cache check failed for a store"),
                    }
                }
                Ok(HookResponse::CacheCheck(all_results))
            }

            RivenEvent::MediaItemDownloadProviderListRequested => {
                let providers: Vec<ProviderInfo> = stores
                    .iter()
                    .map(|(store, _)| ProviderInfo {
                        name: store.to_string(),
                        store: store.to_string(),
                    })
                    .collect();
                Ok(HookResponse::ProviderList(providers))
            }

            RivenEvent::MediaItemStreamLinkRequested { magnet, .. } => {
                for (store, api_key) in &stores {
                    match generate_link(&ctx.http_client, &base_url, store, api_key, magnet).await {
                        Ok(link) => {
                            return Ok(HookResponse::StreamLink(StreamLinkResponse { link }));
                        }
                        Err(e) => {
                            tracing::warn!(store, error = %e, "generate link failed");
                        }
                    }
                }
                anyhow::bail!("no store could generate a stream link")
            }

            RivenEvent::DebridUserInfoRequested => {
                let mut infos = Vec::new();
                for (store, api_key) in &stores {
                    match fetch_user_info(&ctx.http_client, &base_url, store, api_key).await {
                        Ok(info) => infos.push(info),
                        Err(e) => {
                            tracing::warn!(store, error = %e, "failed to fetch debrid user info")
                        }
                    }
                }
                Ok(HookResponse::UserInfo(infos))
            }

            _ => Ok(HookResponse::Empty),
        }
    }
}

async fn add_torrent(
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
        anyhow::bail!("store returned no add-torrent data");
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

async fn remove_torrent(
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

async fn check_cache(
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

    // Chunk to avoid 414 Request-URI Too Large errors on some servers.
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

async fn generate_link(
    client: &reqwest::Client,
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> anyhow::Result<String> {
    let url = format!("{base_url}v0/store/torz/link/generate");
    let resp: StremthruResponse<StremthruLink> = client
        .post(&url)
        .header("x-stremthru-store-name", store)
        .header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
        .json(&serde_json::json!({ "link": magnet }))
        .send()
        .await?
        .json()
        .await?;
    Ok(resp
        .data
        .ok_or_else(|| anyhow::anyhow!("store returned no link data"))?
        .link)
}

async fn fetch_user_info(
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

fn parse_torrent_status(s: &str) -> TorrentStatus {
    match s {
        "cached" => TorrentStatus::Cached,
        "queued" => TorrentStatus::Queued,
        "downloading" => TorrentStatus::Downloading,
        "processing" => TorrentStatus::Processing,
        "downloaded" => TorrentStatus::Downloaded,
        "uploading" => TorrentStatus::Uploading,
        "failed" => TorrentStatus::Failed,
        "invalid" => TorrentStatus::Invalid,
        _ => TorrentStatus::Unknown,
    }
}

#[derive(Deserialize)]
struct StremthruTorznabResponse {
    channel: StremthruTorznabChannel,
}

#[derive(Deserialize)]
struct StremthruTorznabChannel {
    #[serde(default)]
    items: Vec<StremthruTorznabItem>,
}

#[derive(Deserialize)]
struct StremthruTorznabItem {
    title: String,
    #[serde(default)]
    attr: Vec<StremthruTorznabAttr>,
}

#[derive(Deserialize)]
struct StremthruTorznabAttr {
    #[serde(rename = "@attributes")]
    attributes: StremthruTorznabAttrContent,
}

#[derive(Deserialize)]
struct StremthruTorznabAttrContent {
    name: String,
    value: String,
}

#[derive(Deserialize)]
struct StremthruResponse<T> {
    data: Option<T>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct StremthruTorrent {
    id: Option<String>,
    status: String,
    files: Vec<StremthruFile>,
}

#[derive(Deserialize)]
struct StremthruFile {
    name: String,
    size: u64,
    #[serde(default)]
    link: String,
}

#[derive(Deserialize)]
struct StremthruCacheCheck {
    items: Vec<StremthruCacheItem>,
}

#[derive(Deserialize)]
struct StremthruCacheItem {
    hash: String,
    status: String,
    #[serde(default)]
    files: Vec<StremthruCacheFile>,
}

#[derive(Deserialize)]
struct StremthruCacheFile {
    name: String,
    size: u64,
}

#[derive(Deserialize)]
struct StremthruLink {
    link: String,
}

#[derive(Deserialize)]
struct StremthruUser {
    email: Option<String>,
    subscription_status: Option<String>,
}
