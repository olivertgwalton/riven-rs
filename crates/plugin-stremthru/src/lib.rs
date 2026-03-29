use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

const DEFAULT_URL: &str = "https://stremthru.13377001.xyz/";

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

    fn version(&self) -> &'static str {
        "0.1.0"
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

        match event {
            RivenEvent::MediaItemDownloadRequested {
                id: _,
                info_hash,
                magnet,
            } => {
                //   hasCacheCheckHook → getCachedTorrentFiles → if not cached, skip.
                //   If cached → getPluginDownloadResult → return files instantly.
                let mut any_network_error = false;
                for (store, api_key) in &stores {
                    // Step 1: Cache check — only proceed if the torrent is cached.
                    let is_cached = match check_cache(
                        &ctx.http_client,
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

                    // Step 2: Torrent is cached — add it to get download URLs instantly.
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

            RivenEvent::MediaItemScrapeRequested {
                imdb_id,
                item_type,
                season,
                episode,
                ..
            } => {
                let imdb_id = match imdb_id {
                    Some(id) => id,
                    None => return Ok(HookResponse::Empty),
                };

                let cat = match item_type {
                    MediaItemType::Movie => "2000",
                    _ => "5000",
                };

                let t = match item_type {
                    MediaItemType::Movie => "movie",
                    _ => "tvsearch",
                };

                let mut url =
                    format!("{base_url}v0/torznab/api?o=json&imdbid={imdb_id}&t={t}&cat={cat}");
                if let Some(s) = season {
                    url.push_str(&format!("&season={s}"));
                }
                if let Some(e) = episode {
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
                                                if let (Some(hash), title) = (info_hash, item.title)
                                                {
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

                Ok(HookResponse::Scrape(streams))
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

// ── API calls ──

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
        anyhow::bail!("store rejected torrent: HTTP {}", response.status());
    }

    let resp: StremthruResponse<StremthruTorrent> = response.json().await?;
    let data = resp.data;

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
    base_url: &str,
    store: &str,
    api_key: &str,
    hashes: &[String],
) -> anyhow::Result<Vec<CacheCheckResult>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }

    // StremThru has a hard limit of 500 hashes per request.
    // StremThru has a hard limit of 500 hashes per request, but we use a smaller
    // chunk size to avoid 414 Request-URI Too Large errors on some servers/proxies.
    const CHUNK_SIZE: usize = 50;

    let mut futures = Vec::new();
    for chunk in hashes.chunks(CHUNK_SIZE) {
        futures.push(check_cache_chunk(client, base_url, store, api_key, chunk));
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
    base_url: &str,
    store: &str,
    api_key: &str,
    hashes: &[String],
) -> anyhow::Result<Vec<CacheCheckResult>> {
    let hash_str = hashes.join(",");
    let url = format!("{base_url}v0/store/torz/check?hash={hash_str}");
    tracing::debug!(
        store,
        hashes = hashes.len(),
        url_len = url.len(),
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
        // Fallback to reading text just in case it's a readable error
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

    let results = resp
        .data
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

    Ok(results)
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
    Ok(resp.data.link)
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

    let premium_until = fetch_premium_until(client, store, api_key)
        .await
        .inspect_err(|e| tracing::debug!(store, error = %e, "could not fetch premium_until"))
        .ok()
        .flatten();

    Ok(riven_core::types::DebridUserInfo {
        store: store.to_string(),
        email: resp.data.email,
        subscription_status: resp.data.subscription_status,
        premium_until,
    })
}

/// Fetch an expiry date (ISO 8601) from each store's native API where supported.
///
/// Each entry is `(url, bearer_token, json_pointer, is_unix_timestamp)`.
/// `json_pointer` uses RFC 6901 syntax (`/field/nested`) to locate the expiry
/// value inside the parsed response, avoiding per-store struct definitions.
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

// ── API response types ──

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
    data: T,
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
