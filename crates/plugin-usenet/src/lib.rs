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
use redis::AsyncCommands;
use riven_core::events::{EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::{
    CachedStoreEntry, DownloadFile, DownloadResult, ProviderInfo, StreamLinkResponse,
};
use riven_usenet::{NntpConfig, UsenetStreamer};
use riven_usenet::nntp::NntpServerConfig;
use std::time::Duration;

const PROVIDER: &str = "usenet";
const NZB_INFO_HASH_PREFIX: &str = "nzb-";

pub(crate) const PROFILE: HttpServiceProfile = HttpServiceProfile::new("usenet-nzb-fetch");

fn nzb_url_redis_key(info_hash: &str) -> String {
    format!("riven:nzb:url:{info_hash}")
}

fn is_nzb_info_hash(info_hash: &str) -> bool {
    info_hash.starts_with(NZB_INFO_HASH_PREFIX)
}

#[derive(Default)]
pub struct UsenetPlugin;

register_plugin!(UsenetPlugin);

/// Build an NNTP server config from this plugin's settings. Returns `None`
/// if any required field is missing. Called both by the plugin itself
/// (during ingest) and by riven-app at startup (to construct the streamer).
pub fn nntp_config_from_settings(settings: &PluginSettings) -> Option<NntpConfig> {
    let host = settings.get("nntphost")?.to_string();
    let port: u16 = settings.get_parsed_or("nntpport", 563);
    let user = settings.get("nntpuser").map(|s| s.to_string());
    let pass = settings.get("nntppass").map(|s| s.to_string());
    let use_tls = settings
        .get("nntptls")
        .map(|s| !matches!(s.to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let max_connections: u32 = settings.get_parsed_or("maxconnections", 8);
    Some(NntpConfig {
        server: NntpServerConfig {
            host,
            port,
            user,
            pass,
            use_tls,
            max_connections,
            timeout: Duration::from_secs(30),
        },
    })
}

/// Build NNTP config directly from a JSON object whose keys mirror the
/// plugin's settings schema (lowercase, dash-stripped — e.g. `nntphost`).
pub fn nntp_config_from_json_value(value: &serde_json::Value) -> Option<NntpConfig> {
    let obj = value.as_object()?;
    let get = |k: &str| -> Option<String> {
        obj.get(k)
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };
    let host = get("nntphost")?;
    let port: u16 = get("nntpport").and_then(|s| s.parse().ok()).unwrap_or(563);
    let user = get("nntpuser");
    let pass = get("nntppass");
    let use_tls = get("nntptls")
        .map(|s| !matches!(s.to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let max_connections: u32 = get("maxconnections")
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    Some(NntpConfig {
        server: NntpServerConfig {
            host,
            port,
            user,
            pass,
            use_tls,
            max_connections,
            timeout: Duration::from_secs(30),
        },
    })
}

#[async_trait]
impl Plugin for UsenetPlugin {
    fn name(&self) -> &'static str {
        "usenet"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::MediaItemDownloadRequested,
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
            SettingField::new("nntphost", "NNTP Host", "text")
                .required()
                .with_placeholder("news.example.com"),
            SettingField::new("nntpport", "NNTP Port", "text").with_default("563"),
            SettingField::new("nntpuser", "NNTP Username", "text"),
            SettingField::new("nntppass", "NNTP Password", "password"),
            SettingField::new("nntptls", "Use TLS", "boolean").with_default("true"),
            SettingField::new("maxconnections", "Max Connections", "text")
                .with_default("8")
                .with_description(
                    "Concurrent NNTP connections. Should not exceed your provider's per-account limit.",
                ),
            SettingField::new("publicbaseurl", "Public Base URL", "url")
                .required()
                .with_placeholder("http://riven.local:8080")
                .with_description(
                    "Base URL of this riven-api instance, used to construct stream URLs that \
                     point back at the /usenet/ streaming route.",
                ),
        ]
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

        // Look up the NZB URL stored by the indexer plugin.
        let mut redis = ctx.redis.clone();
        let nzb_url: Option<String> =
            AsyncCommands::get::<_, Option<String>>(&mut redis, nzb_url_redis_key(info_hash))
                .await
                .ok()
                .flatten();
        let Some(nzb_url) = nzb_url else {
            tracing::warn!(info_hash, "no NZB URL in Redis; cannot ingest");
            return Ok(HookResponse::DownloadStreamUnavailable);
        };

        // Download the NZB itself (XML, small).
        let resp = ctx
            .http
            .send_data(PROFILE, Some(nzb_url.clone()), |client| {
                client.get(&nzb_url)
            })
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("nzb download HTTP {}", resp.status());
        }
        let xml = resp.text().unwrap_or_default();

        // Parse + persist segment metadata. Engine fetches NNTP bytes lazily later.
        let streamer = UsenetStreamer::new(nntp_cfg, ctx.redis.clone());
        let meta = match streamer.ingest(info_hash, &xml).await {
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
        if let Some(p) = provider {
            if p != PROVIDER {
                return Ok(HookResponse::Empty);
            }
        }
        // The persisted stream_url already points at our /usenet/ route, so
        // there is no separate "live link" to refresh. Return empty so the
        // caller falls through to the entry's stored stream_url.
        Ok(HookResponse::StreamLink(StreamLinkResponse {
            link: String::new(),
        }))
    }
}
