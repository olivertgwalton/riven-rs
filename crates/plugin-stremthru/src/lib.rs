mod client;
mod models;
mod newznab;

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
    add_newz, add_torrent, check_cache, download_result_from_newz, download_result_from_torz,
    GeneratedLink, fetch_user_info, generate_link, scrape_torznab,
};
use crate::newznab::{is_nzb_info_hash, nzb_url_redis_key, scrape_newznab};

const DEFAULT_URL: &str = "https://stremthru.13377001.xyz/";
const STORE_SCORE_TTL_SECS: u64 = 60 * 60 * 24 * 7;

const NEWZ_POLL_TIMEOUT_SECS: u64 = 1800;

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

/// Newz-capable stores: the configured debrid stores plus `stremthru` itself
/// when `stremthruauth` is set (a self-hosted StremThru with NNTP +
/// Newznab indexers configured in its dashboard).
fn get_newz_stores(settings: &PluginSettings) -> Vec<(&'static str, String)> {
    let mut stores = get_configured_stores(settings);
    if let Some(auth) = settings.get("stremthruauth") {
        stores.push(("stremthru", auth.to_string()));
    }
    stores
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
            SettingField::new("newznabenabled", "Enable Newznab (NZB) Scraper", "boolean")
                .with_default("false")
                .with_description("Scrape NZB results via the StremThru Newznab aggregator endpoint. Requires `stremthruauth`."),
            SettingField::new("stremthruauth", "StremThru Auth", "password")
                .with_placeholder("username:apikey")
                .with_description("STREMTHRU_AUTH credentials. Used as the `apikey` query parameter for /v0/newznab/api AND as the store authorization when dispatching NZBs to a self-hosted StremThru (NNTP + indexers configured in its dashboard)."),
            SettingField::new("newznabcategories", "Newznab Categories", "text")
                .with_default("2000,5000")
                .with_description("Comma-separated Newznab category IDs (2000 = Movies, 5000 = TV)."),
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
        let base_url = ctx.settings.get_or("stremthruurl", DEFAULT_URL);
        let torz_enabled = ctx.settings.get_or("scrapenabled", "true") != "false";
        let newz_enabled = ctx.settings.get_or("newznabenabled", "false") != "false"
            && ctx.settings.get("stremthruauth").is_some();

        if !torz_enabled && !newz_enabled {
            return Ok(HookResponse::Empty);
        }

        let mut combined = riven_core::types::ScrapeResponse::new();

        if torz_enabled {
            match scrape_torznab(&ctx.http, &base_url, req).await {
                Ok(results) => combined.extend(results),
                Err(error) => {
                    tracing::warn!(error = %error, "stremthru torznab scrape failed");
                }
            }
        }

        if newz_enabled {
            let apikey = ctx.settings.get_or("stremthruauth", "").to_string();
            let categories = ctx
                .settings
                .get_or("newznabcategories", "2000,5000")
                .to_string();
            match scrape_newznab(
                &ctx.http,
                &ctx.redis,
                &base_url,
                &apikey,
                &categories,
                req,
            )
            .await
            {
                Ok(results) => combined.extend(results),
                Err(error) => {
                    tracing::warn!(error = %error, "stremthru newznab scrape failed");
                }
            }
        }

        Ok(HookResponse::Scrape(combined))
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

        if is_nzb_info_hash(info_hash) {
            let newz_stores = get_newz_stores(&ctx.settings);
            let newz_scores = get_store_scores(&ctx.redis, &newz_stores).await;
            return handle_newz_download(
                info_hash,
                &base_url,
                &newz_stores,
                &newz_scores,
                ctx,
            )
            .await;
        }

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
        // NZB hashes are synthetic (sha1 of the NZB URL) and don't map to
        // StremThru's content-hash-keyed cache.
        let hashes: Vec<String> = hashes
            .iter()
            .filter(|h| !is_nzb_info_hash(h))
            .cloned()
            .collect();
        if hashes.is_empty() {
            return Ok(HookResponse::CacheCheck(Vec::new()));
        }
        let hashes = hashes.as_slice();
        let base_url = ctx.settings.get_or("stremthruurl", DEFAULT_URL);
        let mut stores = get_configured_stores(&ctx.settings);

        // Caller-scoped to a single provider — drop the others so an early
        // hit on the first provider skips slower ones.
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
        // Use the newz-inclusive store list: an entry originally served by
        // the StremThru aggregator store must be reachable again for the
        // link refresh.
        let stores = get_newz_stores(&ctx.settings);
        let score_map = get_store_scores(&ctx.redis, &stores).await;
        let mut ordered_stores: Vec<(&str, &str)> = stores
            .iter()
            .map(|(store, api_key)| (*store, api_key.as_str()))
            .collect();
        // Prefer the originally-pinned store first; fall through to other
        // configured stores if it returns Dead/Err. Beyond the pinned store,
        // order by health score so historically-reliable stores are tried first.
        ordered_stores.sort_by(|(store_a, _), (store_b, _)| {
            let pinned_a = provider.is_some_and(|p| *store_a == p);
            let pinned_b = provider.is_some_and(|p| *store_b == p);
            if pinned_a != pinned_b {
                return pinned_b.cmp(&pinned_a);
            }
            let score_a = score_map.get(*store_a).copied().unwrap_or_default();
            let score_b = score_map.get(*store_b).copied().unwrap_or_default();
            score_b.cmp(&score_a).then_with(|| store_a.cmp(store_b))
        });

        let mut saw_dead = false;
        for (store, api_key) in ordered_stores {
            let result = generate_link(&ctx.http, &base_url, store, api_key, magnet).await;
            match result {
                Ok(GeneratedLink::Link(link)) => {
                    adjust_store_score(&ctx.redis, store, 1).await;
                    return Ok(HookResponse::StreamLink(StreamLinkResponse {
                        link,
                        provider: Some(store.to_string()),
                    }));
                }
                Ok(GeneratedLink::Dead) => {
                    adjust_store_score(&ctx.redis, store, -1).await;
                    saw_dead = true;
                }
                Err(error) => {
                    adjust_store_score(&ctx.redis, store, -1).await;
                    tracing::warn!(store, error = %error, "generate link failed");
                }
            }
        }
        // Every configured store either reported the torrent permanently
        // gone or errored. If at least one reported `Dead`, surface that so
        // the link-request consumer can blacklist the stream and re-download.
        if saw_dead {
            return Ok(HookResponse::StreamLinkDead);
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

async fn handle_newz_download(
    info_hash: &str,
    base_url: &str,
    stores: &[(&str, String)],
    score_map: &HashMap<String, i64>,
    ctx: &PluginContext,
) -> anyhow::Result<HookResponse> {
    let mut redis = ctx.redis.clone();
    let nzb_url: Option<String> =
        AsyncCommands::get::<_, Option<String>>(&mut redis, nzb_url_redis_key(info_hash))
            .await
            .ok()
            .flatten();
    let Some(nzb_url) = nzb_url else {
        tracing::warn!(info_hash, "no NZB URL in Redis; cannot dispatch to stremthru newz");
        return Ok(HookResponse::DownloadStreamUnavailable);
    };

    let mut ordered: Vec<(&str, &str)> = stores
        .iter()
        .map(|(store, api_key)| (*store, api_key.as_str()))
        .collect();
    ordered.sort_by(|(a, _), (b, _)| {
        let sa = score_map.get(*a).copied().unwrap_or_default();
        let sb = score_map.get(*b).copied().unwrap_or_default();
        sb.cmp(&sa).then_with(|| a.cmp(b))
    });

    let poll_timeout = Duration::from_secs(NEWZ_POLL_TIMEOUT_SECS);
    let mut any_network_error = false;
    for (store, api_key) in ordered {
        match add_newz(&ctx.http, base_url, store, api_key, &nzb_url, poll_timeout).await {
            Ok(Some(newz)) => {
                adjust_store_score(&ctx.redis, store, 5).await;
                tracing::debug!(store, info_hash, files = newz.files.len(), "newz added");
                let download = download_result_from_newz(store, info_hash, newz);
                return Ok(HookResponse::Download(Box::new(download)));
            }
            Ok(None) => {
                adjust_store_score(&ctx.redis, store, -2).await;
                tracing::debug!(store, info_hash, "add_newz returned unavailable");
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
                tracing::warn!(store, error = %error, "stremthru add_newz failed");
            }
        }
    }

    if any_network_error {
        anyhow::bail!("network error contacting newz store");
    }
    Ok(HookResponse::DownloadStreamUnavailable)
}

#[cfg(test)]
mod tests;
