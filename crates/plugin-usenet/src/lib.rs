//! Direct-streaming Usenet downloader plugin.
//!
//! Parses an NZB, persists segment metadata in Redis, and returns a
//! `stream_url` pointing at riven-api's `/usenet/...` route. Bytes are pulled
//! from NNTP on demand as the player requests them.
//!
//! This plugin owns the NNTP credentials; the streamer in `riven-api` reads
//! them from this plugin's settings at startup and from then on the two
//! communicate only through Redis-stored NZB metadata.

use async_trait::async_trait;
use lru::LruCache;
use redis::AsyncCommands;
use riven_core::events::{EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::{
    CacheCheckResult, CachedStoreEntry, DownloadFile, DownloadResult, ProviderInfo,
    StreamLinkResponse, TorrentStatus,
};
use riven_usenet::nntp::{NntpPool, NntpProvider, NntpServerConfig};
use riven_usenet::{NntpConfig, UsenetStreamer};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

mod availnzb;
mod health_check;

pub(crate) const PROVIDER: &str = "usenet";
const NZB_INFO_HASH_PREFIX: &str = "nzb-";

fn nzb_body_cache() -> &'static Mutex<LruCache<String, Arc<String>>> {
    static C: OnceLock<Mutex<LruCache<String, Arc<String>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap())))
}

// Indexer download endpoints rate-limit separately from search and start
// returning 429s well before the per-day quota; 30/min stays under the
// common indexer limits.
pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("usenet-nzb-fetch").with_rate_limit(30, Duration::from_secs(60));

pub(crate) fn nzb_url_redis_key(info_hash: &str) -> String {
    format!("riven:nzb:url:{info_hash}")
}

fn is_nzb_info_hash(info_hash: &str) -> bool {
    info_hash.starts_with(NZB_INFO_HASH_PREFIX)
}

#[derive(Default)]
pub struct UsenetPlugin;

register_plugin!(UsenetPlugin);

#[derive(Debug, serde::Deserialize)]
struct ProviderJson {
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    pass: Option<String>,
    #[serde(default = "default_tls")]
    tls: bool,
    #[serde(default = "default_max_conns")]
    max_connections: u32,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    backup: bool,
}

fn default_port() -> u16 {
    563
}
fn default_tls() -> bool {
    true
}
fn default_max_conns() -> u32 {
    8
}

impl ProviderJson {
    fn into_provider(self) -> NntpProvider {
        NntpProvider {
            config: NntpServerConfig {
                host: self.host,
                port: self.port,
                user: self.user,
                pass: self.pass,
                use_tls: self.tls,
                max_connections: self.max_connections,
                timeout: Duration::from_secs(30),
            },
            priority: self.priority,
            is_backup: self.backup,
        }
    }
}

pub fn nntp_config_from_settings(settings: &PluginSettings) -> Option<NntpConfig> {
    let raw = settings.get("nntpproviders")?;
    parse_providers_str(raw)
}

// `nntpproviders` is stored as a JSON object when loaded from DB JSONB, or
// as a JSON-encoded string when loaded via the flattened settings store.
pub fn nntp_config_from_json_value(value: &serde_json::Value) -> Option<NntpConfig> {
    let raw_field = value.as_object()?.get("nntpproviders")?;
    match raw_field {
        serde_json::Value::Object(_) => parse_providers_value(raw_field),
        serde_json::Value::String(s) => parse_providers_str(s),
        _ => None,
    }
}

fn parse_providers_str(raw: &str) -> Option<NntpConfig> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(trimmed).ok()?;
    parse_providers_value(&v)
}

fn parse_providers_value(v: &serde_json::Value) -> Option<NntpConfig> {
    let map = v.as_object()?;
    let mut providers: Vec<NntpProvider> = Vec::with_capacity(map.len());
    for (_name, entry) in map.iter() {
        let parsed: ProviderJson = serde_json::from_value(entry.clone()).ok()?;
        providers.push(parsed.into_provider());
    }
    if providers.is_empty() {
        return None;
    }
    Some(NntpConfig { providers })
}

#[async_trait]
impl Plugin for UsenetPlugin {
    fn name(&self) -> &'static str {
        "usenet"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::CoreStarted,
            EventType::MediaItemDownloadRequested,
            EventType::MediaItemDownloadCacheCheckRequested,
            EventType::MediaItemDownloadProviderListRequested,
            EventType::MediaItemStreamLinkRequested,
        ]
    }

    async fn on_core_started(&self, ctx: &PluginContext) -> anyhow::Result<HookResponse> {
        if let Some(cfg) = nntp_config_from_settings(&ctx.settings) {
            let pool = NntpPool::new_multi(cfg.providers);
            health_check::spawn(
                ctx.db_pool.clone(),
                ctx.redis.clone(),
                pool,
                ctx.http.clone(),
                ctx.settings.clone(),
            );
        }
        Ok(HookResponse::Empty)
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(nntp_config_from_settings(settings).is_some())
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("nntpproviders", "NNTP Providers", "dictionary")
                .required()
                .with_key_placeholder("provider_name")
                .with_add_label("Add provider")
                .with_description(
                    "One or more NNTP providers. Each entry is named (any short label) \
                     and configures one server. With multiple providers, primaries are \
                     tried first by priority; backups are only consulted after every \
                     primary returned article-not-found.",
                )
                .with_item_fields(vec![
                    SettingField::new("host", "Host", "text")
                        .required()
                        .with_placeholder("news.newshosting.com"),
                    SettingField::new("port", "Port", "number").with_default("563"),
                    SettingField::new("user", "Username", "text"),
                    SettingField::new("pass", "Password", "password"),
                    SettingField::new("tls", "Use TLS", "boolean").with_default("true"),
                    SettingField::new("max_connections", "Max Connections", "number")
                        .with_default("8")
                        .with_description(
                            "Concurrent NNTP connections. Should not exceed the \
                             provider's per-account limit.",
                        ),
                    SettingField::new("priority", "Priority", "number")
                        .with_default("0")
                        .with_description("Lower numbers are tried first."),
                    SettingField::new("backup", "Backup", "boolean")
                        .with_default("false")
                        .with_description(
                            "Consult only after every primary returned article-not-found. \
                             Typical block-account or fill-provider setup.",
                        ),
                ]),
            SettingField::new("archivepassword", "Archive Password", "password")
                .with_description(
                    "Password for encrypted RAR archives. Applied to every encrypted \
                     archive encountered. Leave blank if your releases are not encrypted.",
                ),
            SettingField::new("publicbaseurl", "Public Base URL", "url")
                .required()
                .with_placeholder("http://riven.local:8080")
                .with_description(
                    "Base URL of this riven-api instance, used to construct stream URLs \
                     that point back at the /usenet/ streaming route. Use a loopback \
                     address (http://127.0.0.1:<port>) so the VFS reaches /usenet/ from \
                     localhost and the route's loopback auth exemption applies.",
                ),
            SettingField::new("availnzbenabled", "Enable AvailNZB Pre-Filter", "boolean")
                .with_default("false")
                .with_description(
                    "Consult AvailNZB (snzb.stream) during cache check to filter out \
                     NZB releases the crowdsourced dataset has marked unavailable. \
                     Releases AvailNZB has no data on are passed through unchanged. \
                     Disable to keep the default behaviour of trusting every NZB \
                     candidate as cached until the streamer's STAT sample says otherwise.",
                ),
            SettingField::new("availnzburl", "AvailNZB URL", "url")
                .with_default(availnzb::DEFAULT_BASE_URL)
                .with_placeholder(availnzb::DEFAULT_BASE_URL)
                .with_description(
                    "Base URL of the AvailNZB instance. Defaults to the public snzb.stream service.",
                ),
            SettingField::new("availnzbapikey", "AvailNZB API Key", "password")
                .with_description(
                    "API key used to report playback outcomes back to AvailNZB \
                     (X-API-Key header). Optional. Required only to contribute \
                     reports — the URL availability check works without one. \
                     Obtain via POST /api/v1/keys/roll_key on the AvailNZB host.",
                ),
        ]
    }

    async fn on_download_cache_check_requested(
        &self,
        hashes: &[String],
        provider: Option<&str>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        if let Some(p) = provider
            && p != PROVIDER
        {
            return Ok(HookResponse::Empty);
        }

        let nzb_hashes: Vec<&String> = hashes.iter().filter(|h| is_nzb_info_hash(h)).collect();
        if nzb_hashes.is_empty() {
            return Ok(HookResponse::Empty);
        }

        // Default policy: treat every NZB candidate as Cached without
        // probing. Each probe would cost a full NZB-XML download plus an
        // NNTP STAT, and with dozens of candidates per item the indexer
        // rate limit is exhausted before any actual download runs. The
        // streamer's ingest step still STATs a sample of segments before
        // exposing the file, so dead NZBs are caught there instead.
        //
        // When AvailNZB is enabled, query the crowdsourced dataset by NZB
        // URL and drop hashes it has *explicitly* marked unavailable from
        // the result list. Hashes AvailNZB has no opinion on (or where
        // the lookup fails) keep the default Cached status.
        let availnzb_enabled = ctx.settings.get_or("availnzbenabled", "false") != "false";
        let unavailable: std::collections::HashSet<String> = if availnzb_enabled {
            let base_url = ctx
                .settings
                .get_or("availnzburl", availnzb::DEFAULT_BASE_URL)
                .to_string();
            let lookups = nzb_hashes.iter().map(|hash| {
                let hash = (*hash).clone();
                let base_url = base_url.clone();
                async move {
                    let nzb_url = nzb_url_for_hash(&hash, ctx).await?;
                    let outcome =
                        availnzb::check_url(&ctx.http, &ctx.redis, &base_url, &nzb_url).await;
                    Some((hash, outcome))
                }
            });
            futures::future::join_all(lookups)
                .await
                .into_iter()
                .flatten()
                .filter_map(|(h, o)| matches!(o, availnzb::Availability::Unavailable).then_some(h))
                .collect()
        } else {
            std::collections::HashSet::new()
        };

        let results: Vec<CacheCheckResult> = nzb_hashes
            .into_iter()
            .filter(|h| !unavailable.contains(*h))
            .map(|h| CacheCheckResult {
                hash: h.clone(),
                store: PROVIDER.to_string(),
                status: TorrentStatus::Cached,
                files: Vec::new(),
            })
            .collect();
        Ok(HookResponse::CacheCheck(results))
    }

    async fn on_download_requested(
        &self,
        _id: i64,
        info_hash: &str,
        _magnet: &str,
        _cached_stores: &[CachedStoreEntry],
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        if !is_nzb_info_hash(info_hash) {
            return Ok(HookResponse::Empty);
        }
        let Some(nntp_cfg) = nntp_config_from_settings(&ctx.settings) else {
            return Ok(HookResponse::Empty);
        };
        let Some(public_base) = ctx.settings.get("publicbaseurl") else {
            tracing::warn!("usenet plugin: publicbaseurl not configured");
            return Ok(HookResponse::Empty);
        };
        let public_base = public_base.trim_end_matches('/').to_string();

        let Some(xml_arc) = fetch_nzb_xml(info_hash, ctx).await else {
            tracing::warn!(info_hash, "no NZB body available; cannot ingest");
            return Ok(HookResponse::DownloadStreamUnavailable);
        };

        let streamer = UsenetStreamer::new(nntp_cfg, ctx.redis.clone());
        let password = ctx.settings.get("archivepassword");
        let nzb_url_for_report = nzb_url_for_hash(info_hash, ctx).await;
        let meta = match streamer.ingest(info_hash, &xml_arc, password).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(info_hash, error = %e, "usenet ingest failed");
                // Ingest's STAT pass found the articles missing — feed that
                // back to AvailNZB so other clients can skip this NZB.
                if let Some(url) = nzb_url_for_report {
                    availnzb::spawn_report_if_configured(
                        ctx.http.clone(),
                        &ctx.settings,
                        url,
                        false,
                        None,
                    );
                }
                return Ok(HookResponse::DownloadStreamUnavailable);
            }
        };

        if let Some(url) = nzb_url_for_report {
            let release_name = meta.files.first().map(|f| f.filename.clone());
            availnzb::spawn_report_if_configured(
                ctx.http.clone(),
                &ctx.settings,
                url,
                true,
                release_name,
            );
        }

        let files: Vec<DownloadFile> = meta
            .files
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                let url = format!("{public_base}/usenet/{info_hash}/{idx}");
                DownloadFile {
                    filename: f.filename.clone(),
                    file_size: f.total_size,
                    download_url: Some(url.clone()),
                    stream_url: Some(url),
                }
            })
            .collect();

        tracing::info!(
            info_hash,
            file_count = files.len(),
            primary = files.first().map(|f| f.filename.as_str()),
            "usenet stream registered"
        );

        Ok(HookResponse::Download(Box::new(DownloadResult {
            info_hash: info_hash.to_string(),
            files,
            provider: Some(PROVIDER.to_string()),
            plugin_name: self.name().to_string(),
        })))
    }

    async fn on_download_provider_list_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        if nntp_config_from_settings(&ctx.settings).is_none() {
            return Ok(HookResponse::ProviderList(Vec::new()));
        }
        Ok(HookResponse::ProviderList(vec![ProviderInfo {
            name: PROVIDER.to_string(),
            store: PROVIDER.to_string(),
        }]))
    }

    async fn on_stream_link_requested(
        &self,
        _magnet: &str,
        info_hash: &str,
        provider: Option<&str>,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        if !is_nzb_info_hash(info_hash) {
            return Ok(HookResponse::Empty);
        }
        if let Some(p) = provider
            && p != PROVIDER
        {
            return Ok(HookResponse::Empty);
        }
        // The persisted stream_url already points at /usenet/, so there's
        // no live link to refresh.
        Ok(HookResponse::StreamLink(StreamLinkResponse {
            link: String::new(),
            provider: Some(PROVIDER.to_string()),
        }))
    }
}

async fn nzb_url_for_hash(info_hash: &str, ctx: &PluginContext) -> Option<String> {
    let mut redis = ctx.redis.clone();
    AsyncCommands::get::<_, Option<String>>(&mut redis, nzb_url_redis_key(info_hash))
        .await
        .ok()
        .flatten()
}

async fn fetch_nzb_xml(info_hash: &str, ctx: &PluginContext) -> Option<Arc<String>> {
    if let Some(hit) = nzb_body_cache().lock().unwrap().get(info_hash).cloned() {
        return Some(hit);
    }
    let nzb_url = nzb_url_for_hash(info_hash, ctx).await?;
    let resp = ctx
        .http
        .send_data(PROFILE, Some(nzb_url.clone()), |client| client.get(&nzb_url))
        .await
        .ok()?;
    if !resp.status().is_success() {
        tracing::debug!(info_hash, status = %resp.status(), "nzb fetch returned non-success");
        return None;
    }
    let xml = resp.text().ok()?;
    let arc = Arc::new(xml);
    nzb_body_cache()
        .lock()
        .unwrap()
        .put(info_hash.to_string(), arc.clone());
    Some(arc)
}
