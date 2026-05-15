//! Direct-streaming Usenet downloader plugin.
//!
//! Where `plugin-sabnzbd` hands an NZB off to SABnzbd to download to disk,
//! this plugin treats the NZB as a streamable resource: it parses the NZB,
//! persists segment metadata for the streamer in Redis, and returns a
//! `stream_url` that points at riven-api's `/usenet/...` route. Bytes are
//! pulled from NNTP on demand as the player requests them.
//!
//! This plugin owns the NNTP credentials. The streamer in `riven-api` reads
//! them from this plugin's settings (via `PluginRegistry::get_plugin_settings_json`)
//! at startup; from then on the plugin and the streamer communicate only
//! through Redis-stored NZB metadata.

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
use riven_usenet::nntp::{NntpProvider, NntpServerConfig};
use riven_usenet::{NntpConfig, UsenetStreamer};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const PROVIDER: &str = "usenet";
const NZB_INFO_HASH_PREFIX: &str = "nzb-";

/// Process-wide cache of recently-fetched NZB XML bodies, keyed by info_hash.
/// Consulted by `on_download_requested` so a re-attempt within one download
/// flow doesn't re-fetch the XML. Bounded so a long-running session doesn't
/// pin every NZB it has ever seen.
fn nzb_body_cache() -> &'static Mutex<LruCache<String, Arc<String>>> {
    static C: OnceLock<Mutex<LruCache<String, Arc<String>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap())))
}

/// Rate-limit NZB body fetches. Indexers (NZBgeek, DrunkenSlug, etc.) cap
/// the download endpoint separately from the search API and start returning
/// 429s well before the per-day quota — the season→episode cascade can fan
/// out dozens of probes in a single tick, which trips the limit instantly.
/// 30/min is conservative across the common indexers and aligns with the
/// newznab plugin's API limit.
pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("usenet-nzb-fetch").with_rate_limit(30, Duration::from_secs(60));

fn nzb_url_redis_key(info_hash: &str) -> String {
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

/// Build the multi-provider NNTP config from this plugin's settings.
///
/// The settings store one `dictionary` field, `nntpproviders`, persisted
/// as a JSON-encoded object `{ "<provider_name>": { host, port, ... }, ... }`.
/// Returns `None` if the field is absent, blank, malformed, or empty.
pub fn nntp_config_from_settings(settings: &PluginSettings) -> Option<NntpConfig> {
    let raw = settings.get("nntpproviders")?;
    parse_providers_str(raw)
}

/// Build NNTP config from a JSON object whose `nntpproviders` field is
/// either a JSON object (canonical, when loaded from DB JSONB) or a
/// JSON-encoded string holding the object (when loaded via the
/// flattened settings store).
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
            EventType::MediaItemDownloadRequested,
            EventType::MediaItemDownloadCacheCheckRequested,
            EventType::MediaItemDownloadProviderListRequested,
            EventType::MediaItemStreamLinkRequested,
        ]
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
        ]
    }

    async fn on_download_cache_check_requested(
        &self,
        hashes: &[String],
        provider: Option<&str>,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        // Only respond when the caller is asking about our provider (or did
        // not narrow). Other providers (realdebrid, alldebrid, …) get the
        // empty answer so we don't pollute their cache map with NZB hashes.
        if let Some(p) = provider
            && p != PROVIDER
        {
            return Ok(HookResponse::Empty);
        }

        let nzb_hashes: Vec<&String> = hashes.iter().filter(|h| is_nzb_info_hash(h)).collect();
        if nzb_hashes.is_empty() {
            return Ok(HookResponse::Empty);
        }

        // Treat every NZB candidate as "Cached" without probing. Probing was
        // structurally wrong for usenet: each probe cost one full NZB-XML
        // download from the indexer plus one NNTP STAT, and with dozens of
        // candidates per item the per-indexer rate limit (nzbgeek/drunkenslug
        // ~25-30/min) is exhausted before any actual download is attempted —
        // which turns *every* candidate into `Unknown` and starves the
        // download loop.
        //
        // Mirrors decypharr's `Process`/`Manager.AddNZB` and nzbdav's queue
        // pipeline: both projects skip per-candidate verification and let
        // the real download path discover dead releases and fall through to
        // the next ranked stream. The streamer's ingest step still STATs a
        // sample of segments before exposing the file, so dead NZBs are
        // still caught — just at ingest time rather than cache-check time.
        let results: Vec<CacheCheckResult> = nzb_hashes
            .into_iter()
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

        // `archivepassword` is consulted only when the archive's RAR file
        // headers report encryption; passing it always is harmless.
        let streamer = UsenetStreamer::new(nntp_cfg, ctx.redis.clone());
        let password = ctx.settings.get("archivepassword");
        let meta = match streamer.ingest(info_hash, &xml_arc, password).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(info_hash, error = %e, "usenet ingest failed");
                return Ok(HookResponse::DownloadStreamUnavailable);
            }
        };

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
        // The persisted stream_url already points at our /usenet/ route, so
        // there is no separate "live link" to refresh.
        Ok(HookResponse::StreamLink(StreamLinkResponse {
            link: String::new(),
            provider: Some(PROVIDER.to_string()),
        }))
    }
}

/// Fetch the NZB XML for `info_hash`, consulting (and populating) the
/// process-wide body cache. Returns `None` if the NZB URL is no longer in
/// Redis or the HTTP fetch failed.
async fn fetch_nzb_xml(info_hash: &str, ctx: &PluginContext) -> Option<Arc<String>> {
    if let Some(hit) = nzb_body_cache().lock().unwrap().get(info_hash).cloned() {
        return Some(hit);
    }
    let mut redis = ctx.redis.clone();
    let nzb_url: Option<String> = AsyncCommands::get::<_, Option<String>>(
        &mut redis,
        nzb_url_redis_key(info_hash),
    )
    .await
    .ok()
    .flatten();
    let nzb_url = nzb_url?;
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

