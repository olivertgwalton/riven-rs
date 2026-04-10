mod client;
mod models;

use async_trait::async_trait;
use redis::AsyncCommands;
use std::collections::HashMap;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

use crate::client::{
    AddTorrentError, add_torrent, check_cache, download_result_from_torrent, fetch_user_info,
    generate_link, has_cached_hash,
};
use crate::models::StremthruTorznabResponse;

const DEFAULT_URL: &str = "https://stremthru.13377001.xyz/";
const ADD_TORRENT_INFLIGHT_TTL_SECS: u64 = 20;

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
            settings
                .get(&key)
                .map(|api_key| (*name, api_key.to_string()))
        })
        .collect()
}

fn add_torrent_cooldown_key(store: &str) -> String {
    format!("plugin:stremthru:add-torrent:cooldown:{store}")
}

fn add_torrent_inflight_key(store: &str, info_hash: &str) -> String {
    format!(
        "plugin:stremthru:add-torrent:inflight:{store}:{}",
        info_hash.to_lowercase()
    )
}

async fn store_add_torrent_cooled_down(redis: &redis::aio::ConnectionManager, store: &str) -> bool {
    let mut conn = redis.clone();
    AsyncCommands::exists(&mut conn, add_torrent_cooldown_key(store))
        .await
        .unwrap_or(false)
}

async fn set_store_add_torrent_cooldown(
    redis: &redis::aio::ConnectionManager,
    store: &str,
    ttl_secs: u64,
) {
    let mut conn = redis.clone();
    let _: Result<(), _> =
        AsyncCommands::set_ex(&mut conn, add_torrent_cooldown_key(store), "1", ttl_secs).await;
}

async fn try_acquire_add_torrent_inflight(
    redis: &redis::aio::ConnectionManager,
    store: &str,
    info_hash: &str,
    ttl_secs: u64,
) -> bool {
    let key = add_torrent_inflight_key(store, info_hash);
    let mut conn = redis.clone();
    let acquired: Result<Option<String>, _> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("EX")
        .arg(ttl_secs)
        .arg("NX")
        .query_async(&mut conn)
        .await;
    acquired.ok().flatten().is_some()
}

async fn release_add_torrent_inflight(
    redis: &redis::aio::ConnectionManager,
    store: &str,
    info_hash: &str,
) {
    let mut conn = redis.clone();
    let _: Result<(), _> =
        AsyncCommands::del(&mut conn, add_torrent_inflight_key(store, info_hash)).await;
}

async fn scrape_streams(
    client: &reqwest::Client,
    base_url: &str,
    request: &riven_core::events::ScrapeRequest<'_>,
) -> ScrapeResponse {
    let Some(imdb_id) = request.imdb_id else {
        return HashMap::new();
    };

    let cat = match request.item_type {
        MediaItemType::Movie => "2000",
        _ => "5000",
    };
    let kind = match request.item_type {
        MediaItemType::Movie => "movie",
        _ => "tvsearch",
    };

    let mut url = format!("{base_url}v0/torznab/api?o=json&imdbid={imdb_id}&t={kind}&cat={cat}");
    if let Some(season) = request.season {
        url.push_str(&format!("&season={season}"));
    }
    if let Some(episode) = request.episode {
        url.push_str(&format!("&ep={episode}"));
    }

    let mut streams = HashMap::new();
    match riven_core::http::send(|| client.get(&url)).await {
        Ok(response) if response.status().is_success() => match response.text().await {
            Ok(body) => match serde_json::from_str::<StremthruTorznabResponse>(&body) {
                Ok(data) => {
                    for item in data.channel.items {
                        let info_hash = item
                            .attr
                            .iter()
                            .find(|attr| attr.attributes.name == "infohash")
                            .map(|attr| attr.attributes.value.as_str());
                        if let (Some(hash), title) = (info_hash, item.title)
                            && !hash.is_empty()
                            && !title.is_empty()
                        {
                            let hash = hash.to_lowercase();
                            streams.insert(
                                hash.clone(),
                                ScrapeStream {
                                    title,
                                    magnet: build_magnet_uri(&hash),
                                },
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, body = %body, "failed to parse torznab json");
                }
            },
            Err(error) => {
                tracing::warn!(error = %error, "failed to read torznab response body");
            }
        },
        Ok(response) => {
            tracing::warn!(status = %response.status(), "torznab request failed");
        }
        Err(error) => {
            tracing::warn!(error = %error, "torznab scrape failed");
        }
    }

    tracing::debug!(url, found = streams.len(), "scrape complete");
    streams
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
                .with_default(DEFAULT_URL)
                .with_placeholder(DEFAULT_URL),
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
            let streams = scrape_streams(&ctx.http_client, &base_url, &request).await;
            return Ok(HookResponse::Scrape(streams));
        }

        match event {
            RivenEvent::MediaItemDownloadRequested {
                id: _,
                info_hash,
                magnet,
            } => {
                let query = CacheCheckQuery {
                    hash: info_hash.clone(),
                    magnet: magnet.clone(),
                };
                let cache_checks =
                    futures::future::join_all(stores.iter().map(|(store, api_key)| async {
                        let result = check_cache(
                            &ctx.http_client,
                            &ctx.redis,
                            &base_url,
                            store,
                            api_key,
                            std::slice::from_ref(&query),
                        )
                        .await;
                        (*store, api_key.as_str(), result)
                    }))
                    .await;

                let mut any_network_error = false;
                let mut cached_stores = Vec::new();
                for (store, api_key, result) in cache_checks {
                    match result {
                        Ok(results) if has_cached_hash(&results, info_hash) => {
                            cached_stores.push((store, api_key));
                        }
                        Ok(_) => {
                            tracing::debug!(
                                store,
                                info_hash,
                                "torrent not cached in store; skipping"
                            );
                        }
                        Err(error) => {
                            let is_network = error
                                .downcast_ref::<reqwest::Error>()
                                .map(|reqwest_error| {
                                    reqwest_error.is_connect()
                                        || reqwest_error.is_timeout()
                                        || reqwest_error.is_request()
                                })
                                .unwrap_or(false);
                            if is_network {
                                any_network_error = true;
                            }
                            tracing::warn!(store, error = %error, "stremthru cache check failed");
                        }
                    }
                }

                for (store, api_key) in cached_stores {
                    if store_add_torrent_cooled_down(&ctx.redis, store).await {
                        tracing::debug!(
                            store,
                            info_hash,
                            "store add_torrent cooling down after rate limit"
                        );
                        continue;
                    }
                    if !try_acquire_add_torrent_inflight(
                        &ctx.redis,
                        store,
                        info_hash,
                        ADD_TORRENT_INFLIGHT_TTL_SECS,
                    )
                    .await
                    {
                        tracing::debug!(
                            store,
                            info_hash,
                            "duplicate add_torrent attempt suppressed"
                        );
                        continue;
                    }

                    match add_torrent(&ctx.http_client, &base_url, store, api_key, magnet).await {
                        Ok(torrent) => {
                            release_add_torrent_inflight(&ctx.redis, store, info_hash).await;
                            let result = download_result_from_torrent(store, info_hash, torrent);
                            return Ok(HookResponse::Download(Box::new(result)));
                        }
                        Err(AddTorrentError::RateLimited { retry_after_secs }) => {
                            release_add_torrent_inflight(&ctx.redis, store, info_hash).await;
                            set_store_add_torrent_cooldown(&ctx.redis, store, retry_after_secs)
                                .await;
                            tracing::warn!(
                                store,
                                retry_after_secs,
                                "stremthru add_torrent rate limited"
                            );
                        }
                        Err(AddTorrentError::Other(error)) => {
                            release_add_torrent_inflight(&ctx.redis, store, info_hash).await;
                            let is_network = error
                                .downcast_ref::<reqwest::Error>()
                                .map(|e| e.is_connect() || e.is_timeout() || e.is_request())
                                .unwrap_or(false);
                            if is_network {
                                any_network_error = true;
                            }
                            tracing::warn!(store, error = %error, "stremthru add torrent failed");
                        }
                    }
                }
                if any_network_error {
                    anyhow::bail!("network error contacting store");
                }
                Ok(HookResponse::DownloadStreamUnavailable)
            }
            RivenEvent::MediaItemDownloadCacheCheckRequested { queries } => {
                let mut futures = Vec::new();
                for (store, api_key) in &stores {
                    futures.push(check_cache(
                        &ctx.http_client,
                        &ctx.redis,
                        &base_url,
                        store,
                        api_key,
                        queries,
                    ));
                }

                let results = futures::future::join_all(futures).await;
                let mut all_results = Vec::new();
                for result in results {
                    match result {
                        Ok(items) => all_results.extend(items),
                        Err(error) => {
                            tracing::warn!(error = %error, "cache check failed for a store")
                        }
                    }
                }
                Ok(HookResponse::CacheCheck(all_results))
            }
            RivenEvent::MediaItemDownloadProviderListRequested => {
                let providers = stores
                    .iter()
                    .map(|(store, _)| ProviderInfo {
                        name: store.to_string(),
                        store: store.to_string(),
                    })
                    .collect();
                Ok(HookResponse::ProviderList(providers))
            }
            RivenEvent::MediaItemStreamLinkRequested { magnet, .. } => {
                let results =
                    futures::future::join_all(stores.iter().map(|(store, api_key)| async {
                        let result =
                            generate_link(&ctx.http_client, &base_url, store, api_key, magnet)
                                .await;
                        (*store, result)
                    }))
                    .await;

                for (store, result) in results {
                    match result {
                        Ok(link) => {
                            return Ok(HookResponse::StreamLink(StreamLinkResponse { link }));
                        }
                        Err(error) => {
                            tracing::warn!(store, error = %error, "generate link failed");
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
                        Err(error) => {
                            tracing::warn!(store, error = %error, "failed to fetch debrid user info");
                        }
                    }
                }
                Ok(HookResponse::UserInfo(infos))
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}
