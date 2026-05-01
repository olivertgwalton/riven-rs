mod client;
mod models;

use async_trait::async_trait;
use redis::AsyncCommands;
use riven_core::events::{EventType, HookResponse, ScrapeRequest};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use std::collections::HashMap;
use std::time::Duration;

use crate::client::{
    add_torrent, check_cache, download_result_from_torz, fetch_user_info, generate_link,
    scrape_torznab,
};
const DEFAULT_URL: &str = "https://stremthru.13377001.xyz/";
const STORE_SCORE_TTL_SECS: u64 = 60 * 60 * 24 * 7;

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("stremthru").with_rate_limit(1, Duration::from_secs(1));

pub(crate) const REALDEBRID_PROFILE: HttpServiceProfile = HttpServiceProfile::new("realdebrid");
pub(crate) const TORBOX_PROFILE: HttpServiceProfile = HttpServiceProfile::new("torbox");
pub(crate) const ALLDEBRID_PROFILE: HttpServiceProfile = HttpServiceProfile::new("alldebrid");
pub(crate) const DEBRIDLINK_PROFILE: HttpServiceProfile = HttpServiceProfile::new("debridlink");
pub(crate) const PREMIUMIZE_PROFILE: HttpServiceProfile = HttpServiceProfile::new("premiumize");

pub(crate) fn debrid_service(store: &str) -> HttpServiceProfile {
    match store {
        "realdebrid" => REALDEBRID_PROFILE,
        "torbox" => TORBOX_PROFILE,
        "alldebrid" => ALLDEBRID_PROFILE,
        "debridlink" => DEBRIDLINK_PROFILE,
        "premiumize" => PREMIUMIZE_PROFILE,
        _ => HttpServiceProfile::new_owned(store.to_owned()),
    }
}

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

fn get_configured_stores(settings: &PluginSettings) -> Vec<(&'static str, String)> {
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
        let _result: Result<(), _> =
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
            SettingField::new("scrapenabled", "Enable Torznab Scraper", "boolean")
                .with_default("true")
                .with_description("Scrape torrent results via the StremThru Torznab endpoint. Disable to use StremThru only for downloading and cache checks."),
            SettingField::new("checkdebridcache", "Check Debrid Cache", "boolean")
                .with_default("true")
                .with_description("When enabled, queries /store/torz/check first and only attempts add_torrent on confirmed cached/downloaded hashes. When disabled, skips the cache check and calls add_torrent directly on each ranked stream."),
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

    async fn on_scrape_requested(
        &self,
        req: &ScrapeRequest<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        if ctx.settings.get_or("scrapenabled", "true") == "false" {
            return Ok(HookResponse::Empty);
        }
        let base_url = ctx.settings.get_or("stremthruurl", DEFAULT_URL);
        let results = scrape_torznab(&ctx.http, &base_url, req).await?;
        Ok(HookResponse::Scrape(results))
    }

    async fn on_download_requested(
        &self,
        _id: i64,
        info_hash: &str,
        _magnet: &str,
        cached_stores: &[riven_core::types::CachedStoreEntry],
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let base_url = ctx.settings.get_or("stremthruurl", DEFAULT_URL);
        let stores = get_configured_stores(&ctx.settings);
        let score_map = get_store_scores(&ctx.redis, &stores).await;
        let mut any_network_error = false;

                // Build the ordered list of (store, api_key, file_count) to try.
                // Priority: pre-checked stores from the bulk cache check > on-demand
                // cache check > direct add (when checkdebridcache is disabled).
                #[derive(Clone)]
                struct StoreAttempt<'s> {
                    store: &'s str,
                    api_key: &'s str,
                    file_count: usize,
                }

                let attempts: Vec<StoreAttempt<'_>> = if !cached_stores.is_empty() {
                    let mut v: Vec<StoreAttempt<'_>> = cached_stores
                        .iter()
                        .filter_map(|entry| {
                            stores
                                .iter()
                                .find(|(s, _)| s.eq_ignore_ascii_case(&entry.store))
                                .map(|(s, k)| StoreAttempt {
                                    store: s,
                                    api_key: k.as_str(),
                                    file_count: entry.files.len(),
                                })
                        })
                        .collect();
                    v.sort_by(|a, b| {
                        let sa = score_map.get(a.store).copied().unwrap_or_default();
                        let sb = score_map.get(b.store).copied().unwrap_or_default();
                        sb.cmp(&sa)
                            .then_with(|| b.file_count.cmp(&a.file_count))
                            .then_with(|| a.store.cmp(b.store))
                    });
                    v
                } else if ctx.settings.get_or("checkdebridcache", "true") != "false" {
                    // Fallback on-demand check (e.g. manual download trigger with no pre-check data).
                    let hashes = vec![info_hash.to_lowercase()];
                    let checks = futures::future::join_all(stores.iter().map(|(s, k)| async {
                        let r = check_cache(&ctx.http, &ctx.redis, &base_url, s, k, &hashes).await;
                        (*s, k.as_str(), r)
                    }))
                    .await;

                    let mut v = Vec::new();
                    for (store, api_key, result) in checks {
                        match result {
                            Ok(results) => {
                                if let Some(r) = results.into_iter().find(|r| {
                                    r.hash.eq_ignore_ascii_case(info_hash)
                                        && matches!(r.status, TorrentStatus::Cached | TorrentStatus::Downloaded | TorrentStatus::Unknown)
                                }) {
                                    v.push(StoreAttempt { store, api_key, file_count: r.files.len() });
                                } else {
                                    tracing::debug!(store, info_hash, "torrent not cached in store; skipping");
                                }
                            }
                            Err(error) => {
                                if error.downcast_ref::<reqwest::Error>()
                                    .map(|e| e.is_connect() || e.is_timeout() || e.is_request())
                                    .unwrap_or(false)
                                {
                                    any_network_error = true;
                                }
                                tracing::warn!(store, error = %error, "stremthru cache check failed");
                            }
                        }
                    }
                    v.sort_by(|a, b| {
                        let sa = score_map.get(a.store).copied().unwrap_or_default();
                        let sb = score_map.get(b.store).copied().unwrap_or_default();
                        sb.cmp(&sa)
                            .then_with(|| b.file_count.cmp(&a.file_count))
                            .then_with(|| a.store.cmp(b.store))
                    });
                    v
                } else {
                    // Direct mode: skip cache check entirely.
                    let mut v: Vec<StoreAttempt<'_>> = stores
                        .iter()
                        .map(|(s, k)| StoreAttempt { store: s, api_key: k.as_str(), file_count: 0 })
                        .collect();
                    v.sort_by(|a, b| {
                        let sa = score_map.get(a.store).copied().unwrap_or_default();
                        let sb = score_map.get(b.store).copied().unwrap_or_default();
                        sb.cmp(&sa).then_with(|| a.store.cmp(b.store))
                    });
                    v
                };

                for attempt in attempts {
                    match add_torrent(&ctx.http, &base_url, attempt.store, attempt.api_key, info_hash).await {
                        Ok(Some(torz)) => {
                            adjust_store_score(&ctx.redis, attempt.store, 5).await;
                            tracing::debug!(
                                store = attempt.store,
                                info_hash,
                                files = torz.files.len(),
                                "torrent added"
                            );
                            let download = download_result_from_torz(attempt.store, info_hash, torz);
                            return Ok(HookResponse::Download(Box::new(download)));
                        }
                        Ok(None) => {
                            adjust_store_score(&ctx.redis, attempt.store, -2).await;
                            tracing::debug!(store = attempt.store, info_hash, "add_torrent returned unavailable");
                        }
                        Err(error) => {
                            adjust_store_score(&ctx.redis, attempt.store, -1).await;
                            if error
                                .downcast_ref::<reqwest::Error>()
                                .map(|e| e.is_connect() || e.is_timeout() || e.is_request())
                                .unwrap_or(false)
                            {
                                any_network_error = true;
                            }
                            tracing::warn!(store = attempt.store, error = %error, "stremthru add_torrent failed");
                        }
                    }
                }

        if any_network_error {
            anyhow::bail!("network error contacting store");
        }
        Ok(HookResponse::DownloadStreamUnavailable)
    }

    async fn on_download_cache_check_requested(
        &self,
        hashes: &[String],
        provider: Option<&str>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let check_cache_enabled = ctx.settings.get_or("checkdebridcache", "true") != "false";
        if !check_cache_enabled {
            return Ok(HookResponse::Empty);
        }
        let base_url = ctx.settings.get_or("stremthruurl", DEFAULT_URL);
        let mut stores = get_configured_stores(&ctx.settings);

        // Caller-scoped to a single provider — drop the others so we don't
        // do work the caller already decided is unnecessary. The download
        // flow uses this so an early hit on the first provider skips slower
        // ones.
        if let Some(filter) = provider {
            stores.retain(|(store, _)| *store == filter);
            if stores.is_empty() {
                tracing::debug!(
                    requested_provider = filter,
                    "stremthru: requested provider not configured"
                );
                return Ok(HookResponse::CacheCheck(Vec::new()));
            }
        }

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

    async fn on_download_provider_list_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let stores = get_configured_stores(&ctx.settings);
        let providers = stores
            .iter()
            .map(|(store, _)| ProviderInfo {
                name: store.to_string(),
                store: store.to_string(),
            })
            .collect();
        Ok(HookResponse::ProviderList(providers))
    }

    async fn on_stream_link_requested(
        &self,
        magnet: &str,
        _info_hash: &str,
        provider: Option<&str>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let base_url = ctx.settings.get_or("stremthruurl", DEFAULT_URL);
        let stores = get_configured_stores(&ctx.settings);
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
            if let Some(p) = provider
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

    async fn on_debrid_user_info_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let base_url = ctx.settings.get_or("stremthruurl", DEFAULT_URL);
        let stores = get_configured_stores(&ctx.settings);
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
}

#[cfg(test)]
mod tests;
