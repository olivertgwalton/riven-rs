mod client;
mod models;

use async_trait::async_trait;
use redis::AsyncCommands;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use std::collections::HashMap;

use crate::client::{
    add_torrent, check_cache, download_result_from_torz, fetch_user_info, generate_link,
    scrape_torznab,
};
const DEFAULT_URL: &str = "https://stremthru.13377001.xyz/";
const STORE_SCORE_TTL_SECS: u64 = 60 * 60 * 24 * 7;

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

fn store_score_key(store: &str) -> String {
    format!("plugin:stremthru:store-score:{store}")
}

async fn get_store_scores(
    redis: &redis::aio::ConnectionManager,
    stores: &[(&str, String)],
) -> HashMap<String, i64> {
    let mut conn = redis.clone();
    let mut scores = HashMap::with_capacity(stores.len());

    for (store, _) in stores {
        let score = AsyncCommands::get::<_, Option<i64>>(&mut conn, store_score_key(store))
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        scores.insert((*store).to_string(), score);
    }

    scores
}

async fn adjust_store_score(redis: &redis::aio::ConnectionManager, store: &str, delta: i64) {
    let key = store_score_key(store);
    let mut conn = redis.clone();
    let next = redis::cmd("INCRBY")
        .arg(&key)
        .arg(delta)
        .query_async::<i64>(&mut conn)
        .await;
    if next.is_ok() {
        let _: Result<(), _> =
            AsyncCommands::expire(&mut conn, key, STORE_SCORE_TTL_SECS as i64).await;
    }
}

#[async_trait]
impl Plugin for StremthruPlugin {
    fn name(&self) -> &'static str {
        "stremthru"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::MediaItemScrapeRequested,
            EventType::MediaItemDownloadRequested,
            EventType::MediaItemDownloadCacheCheckRequested,
            EventType::MediaItemDownloadProviderListRequested,
            EventType::MediaItemStreamLinkRequested,
            EventType::DebridUserInfoRequested,
        ]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
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
            RivenEvent::MediaItemScrapeRequested { .. } => {
                let Some(req) = event.scrape_request() else {
                    return Ok(HookResponse::Empty);
                };
                let results = scrape_torznab(&ctx.http, &base_url, &req).await?;
                Ok(HookResponse::Scrape(results))
            }
            RivenEvent::MediaItemDownloadRequested { info_hash, .. } => {
                let mut any_network_error = false;
                let hashes = vec![info_hash.to_lowercase()];
                let score_map = get_store_scores(&ctx.redis, &stores).await;
                let cache_checks =
                    futures::future::join_all(stores.iter().map(|(store, api_key)| async {
                        let result =
                            check_cache(&ctx.http, &ctx.redis, &base_url, store, api_key, &hashes)
                                .await;
                        (*store, api_key.as_str(), result)
                    }))
                    .await;

                let mut cached_stores = Vec::new();
                for (store, api_key, result) in cache_checks {
                    match result {
                        Ok(results) => {
                            if let Some(cache_result) = results.into_iter().find(|result| {
                                result.hash.eq_ignore_ascii_case(info_hash)
                                    && matches!(
                                        result.status,
                                        TorrentStatus::Cached | TorrentStatus::Downloaded
                                    )
                            }) {
                                cached_stores.push((store, api_key, cache_result));
                            } else {
                                tracing::debug!(
                                    store,
                                    info_hash,
                                    "torrent not cached in store; skipping"
                                );
                            }
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

                cached_stores.sort_by(|(store_a, _, result_a), (store_b, _, result_b)| {
                    let score_a = score_map.get(*store_a).copied().unwrap_or_default();
                    let score_b = score_map.get(*store_b).copied().unwrap_or_default();
                    score_b
                        .cmp(&score_a)
                        .then_with(|| result_b.files.len().cmp(&result_a.files.len()))
                        .then_with(|| store_a.cmp(store_b))
                });

                for (store, api_key, cache_result) in cached_stores {
                    match add_torrent(&ctx.http, &base_url, store, api_key, info_hash).await {
                        Ok(Some(torz)) => {
                            adjust_store_score(&ctx.redis, store, 5).await;
                            tracing::debug!(
                                store,
                                info_hash,
                                files = torz.files.len(),
                                "torrent cached; building download result from stremthru add"
                            );
                            let download = download_result_from_torz(store, info_hash, torz);
                            return Ok(HookResponse::Download(Box::new(download)));
                        }
                        Ok(None) => {
                            adjust_store_score(&ctx.redis, store, -2).await;
                            tracing::debug!(
                                store,
                                info_hash,
                                files = cache_result.files.len(),
                                "store passed cache check but add returned unavailable"
                            );
                        }
                        Err(error) => {
                            adjust_store_score(&ctx.redis, store, -1).await;
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
            RivenEvent::MediaItemDownloadCacheCheckRequested { hashes } => {
                let mut futures = Vec::new();
                for (store, api_key) in &stores {
                    futures.push(check_cache(
                        &ctx.http, &ctx.redis, &base_url, store, api_key, hashes,
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
            RivenEvent::MediaItemStreamLinkRequested {
                magnet, provider, ..
            } => {
                let score_map = get_store_scores(&ctx.redis, &stores).await;
                let mut ordered_stores: Vec<(&str, &str)> = stores
                    .iter()
                    .map(|(store, api_key)| (*store, api_key.as_str()))
                    .collect();
                ordered_stores.sort_by(|(store_a, _), (store_b, _)| {
                    let score_a = score_map.get(*store_a).copied().unwrap_or_default();
                    let score_b = score_map.get(*store_b).copied().unwrap_or_default();
                    score_b.cmp(&score_a).then_with(|| store_a.cmp(store_b))
                });

                for (store, api_key) in ordered_stores {
                    if let Some(p) = provider.as_deref()
                        && store != p
                    {
                        continue;
                    }
                    let result = generate_link(&ctx.http, &base_url, store, api_key, magnet).await;
                    match result {
                        Ok(link) => {
                            adjust_store_score(&ctx.redis, store, 1).await;
                            return Ok(HookResponse::StreamLink(StreamLinkResponse { link }));
                        }
                        Err(error) => {
                            adjust_store_score(&ctx.redis, store, -1).await;
                            tracing::warn!(store, error = %error, "generate link failed");
                        }
                    }
                }
                anyhow::bail!("no store could generate a stream link")
            }
            RivenEvent::DebridUserInfoRequested => {
                let mut infos = Vec::new();
                for (store, api_key) in &stores {
                    match fetch_user_info(&ctx.http, &base_url, store, api_key).await {
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
