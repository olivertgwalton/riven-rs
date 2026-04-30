use std::time::{Duration, Instant};

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
use serde::Deserialize;

pub(crate) const PROFILE: HttpServiceProfile = HttpServiceProfile::new("sabnzbd");

const NZB_INFO_HASH_PREFIX: &str = "nzb-";
const PROVIDER: &str = "sabnzbd";

fn nzb_url_redis_key(info_hash: &str) -> String {
    format!("riven:nzb:url:{info_hash}")
}

fn is_nzb_info_hash(info_hash: &str) -> bool {
    info_hash.starts_with(NZB_INFO_HASH_PREFIX)
}

#[derive(Default)]
pub struct SabnzbdPlugin;

register_plugin!(SabnzbdPlugin);

#[async_trait]
impl Plugin for SabnzbdPlugin {
    fn name(&self) -> &'static str {
        "sabnzbd"
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
        Ok(settings.get("url").is_some() && settings.get("apikey").is_some())
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("url", "SABnzbd URL", "url")
                .required()
                .with_placeholder("http://sabnzbd.local:8080"),
            SettingField::new("apikey", "API Key", "password").required(),
            SettingField::new("category", "Category", "text")
                .with_default("riven")
                .with_description("SABnzbd category for submitted NZBs."),
            SettingField::new("streambaseurl", "Stream Base URL", "url")
                .with_placeholder("http://sabnzbd.local:8080/complete")
                .with_description(
                    "HTTP base URL serving SABnzbd's completed download directory. \
                     Stream URLs are constructed as `<base>/<storage>/<file>`.",
                ),
            SettingField::new("polltimeoutsecs", "Poll Timeout (s)", "text")
                .with_default("1800")
                .with_description(
                    "Maximum seconds to wait for a submitted NZB to finish before giving up.",
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
        let Some(base_url) = ctx.settings.get("url") else {
            return Ok(HookResponse::Empty);
        };
        let Some(api_key) = ctx.settings.get("apikey") else {
            return Ok(HookResponse::Empty);
        };
        let category = ctx.settings.get_or("category", "riven").to_string();
        let poll_timeout = ctx
            .settings
            .get_or("polltimeoutsecs", "1800")
            .parse::<u64>()
            .unwrap_or(1800);
        let base_url = base_url.trim_end_matches('/').to_string();

        let mut redis_conn = ctx.redis.clone();
        let nzb_url: Option<String> =
            AsyncCommands::get::<_, Option<String>>(&mut redis_conn, nzb_url_redis_key(info_hash))
                .await
                .ok()
                .flatten();
        let Some(nzb_url) = nzb_url else {
            tracing::warn!(info_hash, "no NZB URL in Redis; cannot submit to SABnzbd");
            return Ok(HookResponse::DownloadStreamUnavailable);
        };

        let nzo_id = sab_add_url(&ctx.http, &base_url, api_key, &nzb_url, &category).await?;
        tracing::info!(info_hash, nzo_id, "submitted NZB to SABnzbd");

        let history = poll_until_complete(
            &ctx.http,
            &base_url,
            api_key,
            &nzo_id,
            Duration::from_secs(poll_timeout),
        )
        .await?;

        let stream_base = ctx
            .settings
            .get("streambaseurl")
            .map(|s| s.trim_end_matches('/').to_string());

        let files: Vec<DownloadFile> = history
            .files
            .into_iter()
            .map(|f| {
                let stream_url = stream_base.as_ref().map(|base| {
                    format!(
                        "{base}/{storage}/{name}",
                        storage = history.storage,
                        name = f.name
                    )
                });
                DownloadFile {
                    filename: f.name,
                    file_size: f.size,
                    download_url: stream_url.clone(),
                    stream_url,
                }
            })
            .collect();

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
        if ctx.settings.get("url").is_none() || ctx.settings.get("apikey").is_none() {
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
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        if !is_nzb_info_hash(info_hash) {
            return Ok(HookResponse::Empty);
        }
        if let Some(p) = provider
            && p != PROVIDER
        {
            return Ok(HookResponse::Empty);
        }
        // SABnzbd doesn't host live streams; the file is on local disk after
        // download. The DB-stored `stream_url` from `on_download_requested` is
        // what callers should use. We return Empty here so the client falls
        // back to the entry's stored stream_url.
        let _ = ctx;
        Ok(HookResponse::StreamLink(StreamLinkResponse {
            link: String::new(),
        }))
    }
}

async fn sab_add_url(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    api_key: &str,
    nzb_url: &str,
    category: &str,
) -> anyhow::Result<String> {
    let url = format!("{base_url}/api");
    let params = [
        ("mode", "addurl"),
        ("name", nzb_url),
        ("apikey", api_key),
        ("cat", category),
        ("output", "json"),
    ];
    let resp = http
        .send_data(PROFILE, Some(url.clone()), |client| {
            client.get(&url).query(&params)
        })
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("sabnzbd addurl returned HTTP {}", resp.status());
    }
    let body: AddUrlResponse = resp
        .json()
        .map_err(|e| anyhow::anyhow!("sabnzbd addurl parse error: {e}"))?;
    if !body.status {
        anyhow::bail!("sabnzbd addurl status=false");
    }
    body.nzo_ids
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("sabnzbd addurl returned no nzo_id"))
}

async fn poll_until_complete(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    api_key: &str,
    nzo_id: &str,
    timeout: Duration,
) -> anyhow::Result<HistorySlot> {
    let start = Instant::now();
    let mut interval = Duration::from_secs(5);
    loop {
        if start.elapsed() > timeout {
            anyhow::bail!("sabnzbd download timed out after {:?}", timeout);
        }
        let history = sab_history(http, base_url, api_key, nzo_id).await?;
        if let Some(slot) = history {
            match slot.status.as_str() {
                "Completed" => return Ok(slot),
                "Failed" => anyhow::bail!("sabnzbd reported Failed: {}", slot.fail_message),
                _ => {}
            }
        }
        tokio::time::sleep(interval).await;
        if interval < Duration::from_secs(60) {
            interval = (interval * 2).min(Duration::from_secs(60));
        }
    }
}

async fn sab_history(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    api_key: &str,
    nzo_id: &str,
) -> anyhow::Result<Option<HistorySlot>> {
    let url = format!("{base_url}/api");
    let params = [
        ("mode", "history"),
        ("apikey", api_key),
        ("nzo_ids", nzo_id),
        ("output", "json"),
    ];
    let resp = http
        .send_data(PROFILE, Some(url.clone()), |client| {
            client.get(&url).query(&params)
        })
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("sabnzbd history returned HTTP {}", resp.status());
    }
    let body: HistoryResponse = resp
        .json()
        .map_err(|e| anyhow::anyhow!("sabnzbd history parse error: {e}"))?;
    Ok(body
        .history
        .slots
        .into_iter()
        .find(|s| s.nzo_id == nzo_id))
}

#[derive(Deserialize)]
struct AddUrlResponse {
    status: bool,
    #[serde(default)]
    nzo_ids: Vec<String>,
}

#[derive(Deserialize)]
struct HistoryResponse {
    history: HistoryBlock,
}

#[derive(Deserialize)]
struct HistoryBlock {
    #[serde(default)]
    slots: Vec<HistorySlot>,
}

#[derive(Deserialize)]
struct HistorySlot {
    nzo_id: String,
    status: String,
    #[serde(default)]
    fail_message: String,
    #[serde(default)]
    storage: String,
    #[serde(default)]
    files: Vec<HistoryFile>,
}

#[derive(Deserialize)]
struct HistoryFile {
    #[serde(rename = "filename", alias = "name")]
    name: String,
    #[serde(default, rename = "bytes")]
    size: u64,
}
