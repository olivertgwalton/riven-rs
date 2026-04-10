mod client;
mod models;

use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

use crate::client::{
    check_cache, check_cache_live, download_result_from_cache_check, fetch_user_info, generate_link,
};
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

        match event {
            RivenEvent::MediaItemDownloadRequested { info_hash, .. } => {
                let mut any_network_error = false;

                for (store, api_key) in &stores {
                    match check_cache_live(&ctx.http_client, &base_url, store, api_key, info_hash)
                        .await
                    {
                        Ok(Some(result)) => {
                            tracing::debug!(
                                store,
                                info_hash,
                                files = result.files.len(),
                                "torrent cached; building download result from check"
                            );
                            let download =
                                download_result_from_cache_check(store, info_hash, result);
                            return Ok(HookResponse::Download(Box::new(download)));
                        }
                        Ok(None) => {
                            tracing::debug!(
                                store,
                                info_hash,
                                "torrent not cached in store; skipping"
                            );
                        }
                        Err(error) => {
                            let is_network = error
                                .downcast_ref::<reqwest::Error>()
                                .map(|e| e.is_connect() || e.is_timeout() || e.is_request())
                                .unwrap_or(false);
                            if is_network {
                                any_network_error = true;
                            }
                            tracing::warn!(store, error = %error, "stremthru cache check failed");
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
