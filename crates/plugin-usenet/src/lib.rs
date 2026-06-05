//! Direct-streaming Usenet downloader plugin.
//!
//! Parses an NZB, persists segment metadata, and returns a `usenet://`
//! `stream_url` that the VFS resolves to the in-process usenet streamer.
//! Bytes are pulled from NNTP on demand as the player requests them.
//!
//! This plugin owns the NNTP credentials; the streamer in `riven-api` reads
//! them from this plugin's settings at startup and from then on the two
//! communicate only through Redis-stored NZB metadata.

use async_trait::async_trait;
use lru::LruCache;
use redis::AsyncCommands;
use riven_core::events::{EventType, HookResponse};
use riven_core::types::StreamLinkResponse;
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::{
    CacheCheckResult, CachedStoreEntry, DownloadFile, DownloadResult, ProviderInfo,
    TorrentStatus,
};
use riven_usenet::nntp::{NntpProvider, NntpServerConfig};
use riven_usenet::{NntpConfig, UsenetStreamer};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

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
    for (_name, entry) in map {
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
            // Build (or retrieve) the shared streamer so the playback path,
            // ingest path, and this health-check task all share one
            // `NntpPool` — and one `max_connections` budget — against the
            // provider.
            let streamer = UsenetStreamer::shared(cfg, ctx.db_pool.clone());
            health_check::spawn(
                ctx.db_pool.clone(),
                ctx.redis.clone(),
                streamer,
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
            SettingField::new(
                "healthcheckmaxfailures",
                "Consecutive Failures Before Delete",
                "number",
            )
            .with_default("2")
            .with_description(
                "Number of back-to-back failed STAT samples required before the \
                 library entry is deleted and re-scraped.",
            ),
            SettingField::new("maxdownloadworkers", "Max Download Workers", "number")
                .with_default("4")
                .with_description(
                    "How many NZBs ingest concurrently. Keep this low (default 4): on \
                     usenet, total throughput is bounded by your connection, so more \
                     concurrent downloads don't drain a backlog faster — they split \
                     your bandwidth into slow trickles and starve playback/scanning \
                     (segment fetches go from ~100ms to many seconds). Raise it only \
                     if you have spare bandwidth and want faster backlog drain at the \
                     cost of streaming responsiveness.",
                ),
            SettingField::new(
                "availabilitysamplepercent",
                "Availability Sample %",
                "number",
            )
            .with_default("5")
            .with_description(
                "Percentage of a release's segments to STAT-check at ingest before \
                 accepting it (1-100, default 5, matching altmount). The sample is \
                 strategic — always the first/last few segments (DMCA takedowns and \
                 truncated uploads) plus a spread middle. Higher = more thorough \
                 dead-release detection but slower ingest; lower = faster but more \
                 chance an incomplete release slips through. Bounded to a sane \
                 absolute range internally. NOTE: sampling at any percent can miss a \
                 lone dead segment — enable \"Full Segment Verification\" to catch those.",
            ),
            SettingField::new("checkallsegments", "Full Segment Verification", "boolean")
                .with_default("false")
                .with_description(
                    "STAT-check 100% of the selected release's segments before \
                     committing to it — the only check that reliably catches a single \
                     dead article (sampling almost always misses one). Runs once on the \
                     winning candidate (not every candidate), so it costs one full STAT \
                     sweep per download. Also makes the background health scanner verify \
                     every segment. Recommended after provider changes or if titles keep \
                     stalling mid-playback; leave off to rely on the faster sample.",
                ),
            SettingField::new(
                "acceptablemissingpercent",
                "Acceptable Missing Segments %",
                "number",
            )
            .with_default("0")
            .with_description(
                "Maximum fraction of segments allowed missing before full \
                 verification rejects a release (0-50, default 0 = altmount's \
                 zero-tolerance). Keep at 0: the read path has no par2 repair, so any \
                 missing segment in the played range stalls playback. Raise only if \
                 you knowingly accept gaps.",
            ),
            SettingField::new("autorepair", "Auto-Repair Unhealthy Titles", "boolean")
                .with_default("false")
                .with_description(
                    "Automatically re-grab titles the health scanner finds to have \
                     missing data or no segment map: the broken release is dropped \
                     and re-scraped for a complete one. Uses exponential backoff and \
                     gives up after the retry cap below. Titles that merely couldn't \
                     be verified (provider unreachable) are never auto-repaired.",
                ),
            SettingField::new("repairmaxretries", "Auto-Repair Max Retries", "number")
                .with_default("3")
                .with_description(
                    "How many automatic re-grab attempts a broken title gets before \
                     it's left alone. Backoff doubles between attempts (1h, 2h, 4h …) \
                     up to a 24h cap.",
                ),
        ]
    }

    async fn on_download_cache_check_requested(
        &self,
        hashes: &[String],
        provider: Option<&str>,
        _ctx: &PluginContext,
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

        // Treat every NZB candidate as Cached without probing. Probing each
        // would cost a full NZB-XML download plus an NNTP STAT, and with dozens
        // of candidates per item the indexer rate limit is exhausted before any
        // actual download runs. The streamer's ingest step STATs a sample of
        // segments before exposing the file, so dead NZBs are caught there
        // instead — at NNTP speed, with no extra throttled HTTP round-trip.
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

        let Some(xml_arc) = fetch_nzb_xml(info_hash, ctx).await else {
            tracing::warn!(info_hash, "no NZB body available; cannot ingest");
            return Ok(HookResponse::DownloadStreamUnavailable);
        };

        let streamer = UsenetStreamer::shared(nntp_cfg, ctx.db_pool.clone());
        let password = ctx.settings.get("archivepassword");
        let sample_percent = ctx.settings.get_parsed_or::<usize>(
            "availabilitysamplepercent",
            riven_usenet::DEFAULT_AVAILABILITY_SAMPLE_PERCENT,
        );
        let meta = match streamer
            .ingest(info_hash, &xml_arc, password, sample_percent)
            .await
        {
            Ok(m) => m,
            Err(riven_usenet::StreamerError::IngestQueueFull) => {
                tracing::debug!(info_hash, "ingest queue full; will retry next cycle");
                return Ok(HookResponse::DownloadStreamUnavailable);
            }
            Err(e) => {
                tracing::warn!(info_hash, error = %e, "usenet ingest failed");
                return Ok(HookResponse::DownloadStreamUnavailable);
            }
        };

        // Full-verify the *winner* before committing. The ingest probe above is
        // a cheap strategic sample (it runs per candidate); it catches grossly
        // incomplete releases but can miss a lone dead article. When enabled,
        // STAT every segment of this selected release once so a single-segment
        // gap (which stalls playback at a fixed runtime point) is caught here
        // and the download loop falls through to the next ranked candidate.
        let check_all = ctx.settings.get_bool("checkallsegments");
        if check_all {
            let acceptable_missing = ctx
                .settings
                .get_parsed_or::<f64>("acceptablemissingpercent", 0.0)
                .clamp(0.0, 50.0);
            if let Err(e) = streamer
                .verify_release_complete(info_hash, acceptable_missing)
                .await
            {
                tracing::warn!(
                    info_hash,
                    error = %e,
                    "usenet full segment verification failed; rejecting candidate"
                );
                return Ok(HookResponse::DownloadStreamUnavailable);
            }
            tracing::debug!(info_hash, "usenet full segment verification passed");
        }

        let files: Vec<DownloadFile> = meta
            .files
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                // The VFS reads usenet files in-process and identifies them by
                // the explicit (info_hash, file_index) below. The `usenet://`
                // URI is a self-contained marker (no public base URL needed,
                // nothing fetches it) that keeps the queue's "has a playable
                // URL" check happy and is recognised by the stream-link
                // refresh short-circuit.
                let url = format!("usenet://{info_hash}/{idx}");
                DownloadFile {
                    filename: f.filename.clone(),
                    file_size: f.total_size,
                    download_url: Some(url.clone()),
                    stream_url: Some(url),
                    usenet_info_hash: Some(info_hash.to_string()),
                    usenet_file_index: i32::try_from(idx).ok(),
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

    async fn on_stream_link_requested(
        &self,
        magnet: &str,
        _info_hash: &str,
        _provider: Option<&str>,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        // The usenet stream marker is a permanent `usenet://{hash}/{index}`
        // URI. It never expires, so when the VFS refreshes after a transient
        // NNTP error we return it unchanged instead of letting StremThru try
        // (and fail) to debrid-resolve a non-magnet URL.
        if magnet.starts_with("usenet://") {
            return Ok(HookResponse::StreamLink(StreamLinkResponse {
                link: magnet.to_string(),
                provider: Some(PROVIDER.to_string()),
            }));
        }
        Ok(HookResponse::Empty)
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
