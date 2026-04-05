mod client;
mod models;

use async_trait::async_trait;
use std::collections::HashMap;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

use crate::client::{
    add_torrent, check_cache, download_result_from_torrent, fetch_user_info, generate_link,
    has_cached_hash,
};
use crate::models::StremthruTorznabResponse;

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
            settings
                .get(&key)
                .map(|api_key| (*name, api_key.to_string()))
        })
        .collect()
}

async fn scrape_streams(
    client: &reqwest::Client,
    base_url: &str,
    request: &riven_core::events::ScrapeRequest<'_>,
) -> HashMap<String, String> {
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
    match client.get(&url).send().await {
        Ok(response) if response.status().is_success() => match response.text().await {
            Ok(body) => match serde_json::from_str::<StremthruTorznabResponse>(&body) {
                Ok(data) => {
                    for item in data.channel.items {
                        let info_hash = item
                            .attr
                            .iter()
                            .find(|attr| attr.attributes.name == "infohash")
                            .map(|attr| attr.attributes.value.as_str());
                        if let (Some(hash), title) = (info_hash, item.title) {
                            if !hash.is_empty() && !title.is_empty() {
                                streams.insert(hash.to_lowercase(), title);
                            }
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
                let mut any_network_error = false;
                for (store, api_key) in &stores {
                    let is_cached = match check_cache(
                        &ctx.http_client,
                        &ctx.redis,
                        &base_url,
                        store,
                        api_key,
                        std::slice::from_ref(info_hash),
                    )
                    .await
                    {
                        Ok(results) => has_cached_hash(&results, info_hash),
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
                            continue;
                        }
                    };

                    if !is_cached {
                        tracing::debug!(store, info_hash, "torrent not cached in store; skipping");
                        continue;
                    }

                    match add_torrent(&ctx.http_client, &base_url, store, api_key, magnet).await {
                        Ok(torrent) => {
                            let result = download_result_from_torrent(store, info_hash, torrent);
                            return Ok(HookResponse::Download(Box::new(result)));
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
                            tracing::warn!(store, error = %error, "stremthru add torrent failed");
                        }
                    }
                }
                if any_network_error {
                    anyhow::bail!("network error contacting store");
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
                for (store, api_key) in &stores {
                    match generate_link(&ctx.http_client, &base_url, store, api_key, magnet).await {
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
