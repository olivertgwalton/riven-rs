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
    add_torrent, check_cache, download_result_from_cache, download_result_from_torz, fetch_user_info,
    generate_link, scrape_torznab,
};
use riven_core::types::TorrentStatus;
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
            SettingField::new("scrapenabled", "Enable Torznab Scraper", "boolean")
                .with_default("true")
                .with_description("Scrape torrent results via the StremThru Torznab endpoint. Disable to use StremThru only for downloading and cache checks."),
            SettingField::new("checkdebridcache", "Check Debrid Cache", "boolean")
                .with_default("true")
                .with_description("When enabled, attempts to add the torrent directly to each debrid service — if successful, it is treated as cached. When disabled, queries /store/torz/check in batches of up to 500 hashes per store and uses the result as the source of truth before attempting any add."),
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
                let scrape_enabled = ctx.settings.get_or("scrapenabled", "true") != "false";
                if !scrape_enabled {
                    return Ok(HookResponse::Empty);
                }
                let Some(req) = event.scrape_request() else {
                    return Ok(HookResponse::Empty);
                };
                let results = scrape_torznab(&ctx.http, &base_url, &req).await?;
                Ok(HookResponse::Scrape(results))
            }
            RivenEvent::MediaItemDownloadRequested { info_hash, .. } => {
                // "Check Debrid Cache" enabled = direct add_torrent (implicit availability check).
                // disabled = /store/torz/check batch first, only add confirmed cached hashes.
                let direct_mode =
                    ctx.settings.get_or("checkdebridcache", "true") == "true";

                let score_map = get_store_scores(&ctx.redis, &stores).await;

                if !direct_mode {
                    // ── Check-first mode ──────────────────────────────────────────────
                    // Query each store's cache in parallel. Bypass Redis so we get live
                    // file links in the response. For `Downloaded` status (torrent
                    // already in account), use the links directly — no add_torrent call.
                    // For `Cached` status (instant availability), call add_torrent to
                    // obtain usable links.
                    let hash_lc = info_hash.to_lowercase();
                    let hash_list = vec![hash_lc.clone()];
                    let bypass = vec![hash_lc];
                    let cache_futures: Vec<_> = stores
                        .iter()
                        .map(|(store, api_key)| {
                            let hash_list = hash_list.clone();
                            let bypass = bypass.clone();
                            let base_url = base_url.clone();
                            async move {
                                let result = check_cache(
                                    &ctx.http,
                                    &ctx.redis,
                                    &base_url,
                                    store,
                                    api_key,
                                    &hash_list,
                                    &bypass,
                                )
                                .await;
                                (*store, api_key.as_str(), result)
                            }
                        })
                        .collect();
                    let cache_results = futures::future::join_all(cache_futures).await;

                    let mut downloaded_stores: Vec<(&str, &str, riven_core::types::CacheCheckResult)> =
                        Vec::new();
                    let mut cached_stores: Vec<(&str, &str, usize)> = Vec::new();

                    for (store, api_key, result) in &cache_results {
                        match result {
                            Ok(results) => {
                                if let Some(r) =
                                    results.iter().find(|r| r.hash.eq_ignore_ascii_case(info_hash))
                                {
                                    match r.status {
                                        TorrentStatus::Downloaded => {
                                            downloaded_stores.push((*store, *api_key, r.clone()));
                                        }
                                        TorrentStatus::Cached => {
                                            cached_stores.push((*store, *api_key, r.files.len()));
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::warn!(store, error = %error, "cache check failed");
                            }
                        }
                    }

                    let score_for = |store: &str| score_map.get(store).copied().unwrap_or_default();
                    downloaded_stores.sort_by(|(a, _, ra), (b, _, rb)| {
                        score_for(b).cmp(&score_for(a))
                            .then_with(|| rb.files.len().cmp(&ra.files.len()))
                            .then_with(|| a.cmp(b))
                    });
                    cached_stores.sort_by(|(a, _, fa), (b, _, fb)| {
                        score_for(b).cmp(&score_for(a))
                            .then_with(|| fb.cmp(fa))
                            .then_with(|| a.cmp(b))
                    });

                    tracing::debug!(
                        info_hash,
                        downloaded = downloaded_stores.len(),
                        cached = cached_stores.len(),
                        "check_first mode: stores with torrent available"
                    );

                    let mut any_network_error = false;

                    // Use links from the cache check response directly for Downloaded torrents.
                    for (store, api_key, cache_result) in downloaded_stores {
                        if cache_result.files.iter().any(|f| f.link.is_some()) {
                            adjust_store_score(&ctx.redis, store, 5).await;
                            tracing::debug!(
                                store,
                                info_hash,
                                files = cache_result.files.len(),
                                "torrent already in account; using cache-check links directly"
                            );
                            let download = download_result_from_cache(store, info_hash, cache_result);
                            return Ok(HookResponse::Download(Box::new(download)));
                        }
                        // Links absent — fall back to add_torrent for this store.
                        tracing::debug!(store, info_hash, "Downloaded but no links; falling back to add_torrent");
                        match add_torrent(&ctx.http, &base_url, store, api_key, info_hash).await {
                            Ok(Some(torz)) => {
                                adjust_store_score(&ctx.redis, store, 5).await;
                                let download = download_result_from_torz(store, info_hash, torz);
                                return Ok(HookResponse::Download(Box::new(download)));
                            }
                            Ok(None) => { adjust_store_score(&ctx.redis, store, -2).await; }
                            Err(error) => {
                                adjust_store_score(&ctx.redis, store, -1).await;
                                if error.downcast_ref::<reqwest::Error>()
                                    .map(|e| e.is_connect() || e.is_timeout() || e.is_request())
                                    .unwrap_or(false)
                                {
                                    any_network_error = true;
                                }
                                tracing::warn!(store, error = %error, "add_torrent fallback failed (check_first)");
                            }
                        }
                    }

                    // For Cached torrents, add_torrent is needed to get usable links.
                    for (store, api_key, _) in cached_stores {
                        match add_torrent(&ctx.http, &base_url, store, api_key, info_hash).await {
                            Ok(Some(torz)) => {
                                adjust_store_score(&ctx.redis, store, 5).await;
                                tracing::debug!(
                                    store,
                                    info_hash,
                                    files = torz.files.len(),
                                    "cached torrent added (check_first)"
                                );
                                let download = download_result_from_torz(store, info_hash, torz);
                                return Ok(HookResponse::Download(Box::new(download)));
                            }
                            Ok(None) => {
                                adjust_store_score(&ctx.redis, store, -2).await;
                                tracing::debug!(store, info_hash, "add_torrent returned unavailable (check_first)");
                            }
                            Err(error) => {
                                adjust_store_score(&ctx.redis, store, -1).await;
                                if error
                                    .downcast_ref::<reqwest::Error>()
                                    .map(|e| e.is_connect() || e.is_timeout() || e.is_request())
                                    .unwrap_or(false)
                                {
                                    any_network_error = true;
                                }
                                tracing::warn!(store, error = %error, "stremthru add_torrent failed (check_first)");
                            }
                        }
                    }

                    if any_network_error {
                        anyhow::bail!("network error contacting store");
                    }
                    Ok(HookResponse::DownloadStreamUnavailable)
                } else {
                    // ── Direct mode (Check Debrid Cache enabled) ──────────────────────
                    // Attempt add_torrent on each store sequentially, ordered by score.
                    // StremThru only returns success when status is "downloaded", so
                    // this implicitly checks instant availability without a separate call.
                    let mut ordered_stores: Vec<(&str, &str)> = stores
                        .iter()
                        .map(|(store, api_key)| (*store, api_key.as_str()))
                        .collect();
                    ordered_stores.sort_by(|(store_a, _), (store_b, _)| {
                        let score_a = score_map.get(*store_a).copied().unwrap_or_default();
                        let score_b = score_map.get(*store_b).copied().unwrap_or_default();
                        score_b.cmp(&score_a).then_with(|| store_a.cmp(store_b))
                    });

                    let mut any_network_error = false;
                    for (store, api_key) in ordered_stores {
                        match add_torrent(&ctx.http, &base_url, store, api_key, info_hash).await {
                            Ok(Some(torz)) => {
                                adjust_store_score(&ctx.redis, store, 5).await;
                                tracing::debug!(
                                    store,
                                    info_hash,
                                    files = torz.files.len(),
                                    "torrent added; building download result from stremthru add"
                                );
                                let download = download_result_from_torz(store, info_hash, torz);
                                return Ok(HookResponse::Download(Box::new(download)));
                            }
                            Ok(None) => {
                                adjust_store_score(&ctx.redis, store, -2).await;
                                tracing::debug!(
                                    store,
                                    info_hash,
                                    "add torrent returned unavailable"
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
                                tracing::warn!(
                                    store,
                                    error = %error,
                                    "stremthru add torrent failed"
                                );
                            }
                        }
                    }

                    if any_network_error {
                        anyhow::bail!("network error contacting store");
                    }
                    Ok(HookResponse::DownloadStreamUnavailable)
                }
            }
            RivenEvent::MediaItemDownloadCacheCheckRequested {
                hashes,
                bypass_cache,
                purpose,
            } => {
                // Direct mode mirrors python-riven: no pre-flight cache API
                // calls — add_torrent is the source of truth. We still answer
                // `UiDisplay` requests so the "cached" badge keeps working.
                let direct_mode =
                    ctx.settings.get_or("checkdebridcache", "true") == "true";
                if direct_mode
                    && *purpose == riven_core::events::CacheCheckPurpose::DownloadFlow
                {
                    return Ok(HookResponse::Empty);
                }

                let mut futures = Vec::new();
                for (store, api_key) in &stores {
                    futures.push(check_cache(
                        &ctx.http,
                        &ctx.redis,
                        &base_url,
                        store,
                        api_key,
                        hashes,
                        bypass_cache,
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

#[cfg(test)]
mod tests;
