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
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{FieldType, Plugin, PluginContext, SettingField};
use riven_core::settings::PluginSettings;
use riven_core::types::StreamLinkResponse;
use riven_core::types::{
    CacheCheckResult, CachedStoreEntry, DownloadFile, DownloadResult, ProviderInfo, TorrentStatus,
};
use riven_usenet::nntp::{NntpProvider, NntpServerConfig};
use riven_usenet::{NntpConfig, UsenetStreamer};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

mod health_check;

pub(crate) const PROVIDER: &str = "usenet";

fn nzb_body_cache() -> &'static Mutex<LruCache<String, Arc<String>>> {
    static C: OnceLock<Mutex<LruCache<String, Arc<String>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap())))
}

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("usenet-nzb-fetch").with_rate_limit(30, Duration::from_secs(60));

pub(crate) use riven_core::nzb::{is_nzb_info_hash, nzb_url_redis_key};

/// A candidate that fails ingest/verification at download time is permanently
/// blacklisted immediately — it's a dead release, and retrying it every
/// download cycle (with no way to ever stop) is what starves the queue.
async fn blacklist_failed_download_candidate(media_item_id: i64, info_hash: &str, release: &str) {
    tracing::warn!(
        info_hash,
        release,
        media_item_id,
        "usenet download verification failed; blacklisting release"
    );
    if let Err(error) =
        riven_db::repo::blacklist_stream_permanent_by_hash(media_item_id, info_hash).await
    {
        tracing::warn!(
            info_hash,
            release,
            %error,
            "failed to blacklist failed release"
        );
    }
}

/// A candidate that fails only because the provider(s) reported their own
/// connection limit ("502 too many connections") after the pool's bounded
/// retry gave up is *not* known to be dead — that's the account being at
/// capacity, not a bad release. Skip it for the rest of this download
/// attempt via the non-permanent blacklist (cleared on the next scrape by
/// `clear_blacklisted_streams`) instead of ruling it out for good.
async fn defer_transient_download_candidate(media_item_id: i64, info_hash: &str, release: &str) {
    tracing::warn!(
        info_hash,
        release,
        media_item_id,
        "usenet ingest/verification hit a transient provider-capacity error; deferring candidate this cycle"
    );
    if let Err(error) = riven_db::repo::blacklist_stream_by_hash(media_item_id, info_hash).await {
        tracing::warn!(
            info_hash,
            release,
            %error,
            "failed to defer transient-failure release"
        );
    }
}

#[derive(Default)]
pub struct UsenetPlugin;

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

    fn category(&self) -> &'static str {
        "sources"
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
            let streamer = UsenetStreamer::shared(cfg, riven_db::orm().clone());
            health_check::spawn(ctx.redis.clone(), streamer, ctx.settings.clone());
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
            SettingField::new("nntpproviders", "NNTP Providers", FieldType::Dictionary)
                .required()
                .with_key_placeholder("provider_name")
                .with_add_label("Add provider")
                .with_description(
                    "Your Usenet server accounts. Add one per provider. \
                     Primaries are tried first; backup servers are only used when every primary fails.",
                )
                .with_item_fields(vec![
                    SettingField::new("host", "Host", FieldType::Text)
                        .required()
                        .with_placeholder("news.newshosting.com"),
                    SettingField::new("port", "Port", FieldType::Number).with_default("563"),
                    SettingField::new("user", "Username", FieldType::Text),
                    SettingField::new("pass", "Password", FieldType::Password),
                    SettingField::new("tls", "Use TLS", FieldType::Boolean).with_default("true"),
                    SettingField::new("max_connections", "Max Connections", FieldType::Number)
                        .with_default("8")
                        .with_description(
                            "How many simultaneous connections to open. Don't exceed your provider's account limit.",
                        ),
                    SettingField::new("priority", "Priority", FieldType::Number)
                        .with_default("0")
                        .with_description("Lower numbers are tried first."),
                    SettingField::new("backup", "Backup", FieldType::Boolean)
                        .with_default("false")
                        .with_description(
                            "Only use this server when all primary servers fail. Good for block accounts or fill providers.",
                        ),
                ]),
            SettingField::new("archivepassword", "Archive Password", FieldType::Password).with_description(
                "Password for password-protected archives. Leave blank if your downloads aren't encrypted.",
            ),
            SettingField::new(
                "healthcheckmaxfailures",
                "Consecutive Failures Before Delete",
                FieldType::Number,
            )
            .with_default("2")
            .with_description(
                "How many health check failures in a row before a title is dropped and re-scraped.",
            ),
            SettingField::new("maxdownloadworkers", "Max Download Workers", FieldType::Number)
                .with_default("4")
                .with_description(
                    "How many downloads run at the same time. Keep this low — more parallel downloads \
                     split your bandwidth and can slow down playback. Raise only if you have spare bandwidth.",
                ),
            SettingField::new(
                "availabilitysamplepercent",
                "Availability Sample %",
                FieldType::Number,
            )
            .with_default("5")
            .with_description(
                "What percentage of a release's files to spot-check before accepting it. \
                 Higher = more thorough but slower. Even at 100% a single bad file can slip through — \
                 enable Full Segment Verification to catch those.",
            ),
            SettingField::new("checkallsegments", "Full Segment Verification", FieldType::Boolean)
                .with_default("false")
                .with_description(
                    "Check every file in the release before committing to it. The only reliable way \
                     to catch a single missing file. Slower, but recommended if titles keep stalling mid-playback.",
                ),
            SettingField::new("verifypar2blocks", "PAR2 Block Verification", FieldType::Boolean)
                .with_default("false")
                .with_description(
                    "Check RAR volumes against the release's PAR2 checksums before committing to it. \
                     Catches a volume with the wrong content entirely, not just a missing one. Downloads \
                     real data to check (unlike the other options here), adding a few percent to every \
                     grab's bandwidth — off by default for that reason.",
                ),
            SettingField::new(
                "acceptablemissingpercent",
                "Acceptable Missing Segments %",
                FieldType::Number,
            )
            .with_default("0")
            .with_description(
                "How many missing files (%) to tolerate before rejecting a release. \
                 Leave at 0 — any missing file can cause playback to stall.",
            ),
            SettingField::new("autorepair", "Auto-Repair Unhealthy Titles", FieldType::Boolean)
                .with_default("false")
                .with_description(
                    "Automatically re-download titles the health scanner finds broken. \
                     Drops the bad release and looks for a working one. Gives up after the retry limit below.",
                ),
            SettingField::new("repairmaxretries", "Auto-Repair Max Retries", FieldType::Number)
                .with_default("3")
                .with_description(
                    "How many times to retry a broken title before giving up. Waits longer between each attempt (1h, 2h, 4h…).",
                ),
            SettingField::new(
                "blacklistonreadfailure",
                "Blacklist On Read Failure",
                FieldType::Boolean,
            )
            .with_default("false")
            .with_description(
                "When playback hits a missing file, immediately swap to a different release \
                 instead of waiting for the background health check. The bad release is permanently blacklisted.",
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
        id: i64,
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
            // No NZB body means no release name exists anywhere yet — the
            // media item id is the only handle a reader has here.
            tracing::warn!(
                info_hash,
                media_item_id = id,
                "no NZB body available; cannot ingest"
            );
            return Ok(HookResponse::DownloadStreamUnavailable);
        };
        // Cheap head-only peek (never a full parse — see `peek_release_title`),
        // so every log below this point names the release rather than only its
        // synthetic hash. Ingest failures happen before any meta exists, so
        // this is the only name available on those paths.
        let release = riven_usenet::peek_release_title(&xml_arc)
            .unwrap_or_else(|| riven_usenet::UNKNOWN_FILE_LABEL.to_string());

        let streamer = UsenetStreamer::shared(nntp_cfg, riven_db::orm().clone());
        let password = ctx.settings.get("archivepassword");
        let sample_percent = ctx.settings.get_parsed_or::<usize>(
            "availabilitysamplepercent",
            riven_usenet::DEFAULT_AVAILABILITY_SAMPLE_PERCENT,
        );
        let verify_par2 = ctx.settings.get_bool("verifypar2blocks");
        let check_all = ctx.settings.get_bool("checkallsegments");
        let acceptable_missing = ctx
            .settings
            .get_parsed_or::<f64>("acceptablemissingpercent", 0.0)
            .clamp(0.0, 50.0);
        // Ingest (NNTP fetch + optional PAR2 content verify) and the optional
        // full segment sweep are real network work against a candidate that
        // might be a dead release. No wall-clock timeout here: a healthy
        // release under heavy pool contention should be allowed to take as
        // long as it takes rather than being falsely treated as dead. The
        // pool itself already bounds provider-capacity retries (see
        // `NntpPool::acquire`'s handling of "too many connections"), so a
        // hang here means either a genuinely dead release (surfaces as a
        // verification error below) or every provider's own read/connect
        // timeout, neither of which needs a second timer wrapped around them.
        let verify = async {
            let meta = streamer
                .ingest(info_hash, &xml_arc, password, sample_percent, verify_par2)
                .await?;
            if check_all {
                streamer
                    .verify_release_complete(info_hash, acceptable_missing)
                    .await?;
            }
            Ok::<_, riven_usenet::StreamerError>(meta)
        }
        .await;

        let meta = match verify {
            Ok(m) => m,
            Err(riven_usenet::StreamerError::IngestQueueFull) => {
                tracing::debug!(
                    info_hash,
                    release,
                    "ingest queue full; will retry next cycle"
                );
                return Ok(HookResponse::DownloadStreamUnavailable);
            }
            Err(riven_usenet::StreamerError::Nntp(
                riven_usenet::nntp::NntpError::TooManyConnections(status),
            )) => {
                tracing::warn!(
                    info_hash,
                    release,
                    status,
                    "usenet ingest/verification failed because provider(s) are at their own \
                     connection limit"
                );
                defer_transient_download_candidate(id, info_hash, &release).await;
                return Ok(HookResponse::DownloadStreamUnavailable);
            }
            Err(e) => {
                tracing::warn!(info_hash, release, error = %e, "usenet ingest/verification failed");
                blacklist_failed_download_candidate(id, info_hash, &release).await;
                return Ok(HookResponse::DownloadStreamUnavailable);
            }
        };

        let files: Vec<DownloadFile> = meta
            .files
            .iter()
            .enumerate()
            .map(|(idx, f)| {
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

        tracing::debug!(
            info_hash,
            release,
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
        .send_data(PROFILE, Some(nzb_url.clone()), |client| {
            client.get(&nzb_url)
        })
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
