mod discovery;
mod nntp;
mod preflight;
mod storage;
mod streaming;
mod types;

use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::{
    CacheCheckFile, CacheCheckResult, DownloadFile, DownloadResult, ScrapeResponse,
    StreamLinkResponse, TorrentStatus,
};

use crate::discovery::discover_streams;
use crate::preflight::preflight_payload;
use crate::storage::{load_payload, store_payload};
use crate::streaming::build_stream_url;
use crate::types::{PreflightResult, UsenetPayload};

pub(crate) const CACHE_TTL_SECS: u64 = 60 * 60 * 24 * 30;
pub(crate) const PREFLIGHT_TTL_SECS: u64 = 60 * 60 * 24;

#[derive(Default)]
pub struct UsenetPlugin;

register_plugin!(UsenetPlugin);

#[async_trait]
impl Plugin for UsenetPlugin {
    fn name(&self) -> &'static str {
        "usenet"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::MediaItemScrapeRequested,
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
        Ok(!configured_addons(settings).is_empty())
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("addonurls", "Discovery Source URLs", "textarea")
                .with_placeholder("https://example.com/manifest.json")
                .with_description(
                    "Configured addon manifest URLs that return streams with nzbUrl or nzbUrls.",
                ),
            SettingField::new("servers", "NNTP Server URLs", "textarea")
                .with_placeholder("nntps://user:pass@news.example.com:563")
                .with_description(
                    "Fallback NNTP URLs when a stream does not include servers.",
                ),
            SettingField::new("preflightonscrape", "Preflight During Scrape", "checkbox")
                .with_default("true")
                .with_description(
                    "Fetch, parse, and NNTP-check NZBs during scrape so ranking uses discovered media filenames.",
                ),
            SettingField::new("preflightrequired", "Require Preflight", "checkbox")
                .with_default("true")
                .with_description(
                    "Only mark Usenet candidates cached after the NZB parses and NNTP checks pass.",
                ),
            SettingField::new("nntpcheckenabled", "NNTP Availability Check", "checkbox")
                .with_default("true")
                .with_description("Use native NNTP STAT checks against sampled NZB articles."),
            SettingField::new("nntpsamplepercent", "NNTP Sample Percent", "number")
                .with_default("10")
                .with_placeholder("10")
                .with_description(
                    "Percentage of NZB article IDs to STAT, always including first and last.",
                ),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::MediaItemScrapeRequested { .. } => handle_scrape(event, ctx).await,
            RivenEvent::MediaItemDownloadCacheCheckRequested { hashes } => {
                handle_cache_check(ctx, hashes).await
            }
            RivenEvent::MediaItemDownloadRequested {
                info_hash, magnet, ..
            } => handle_download(ctx, info_hash, magnet).await,
            RivenEvent::MediaItemDownloadProviderListRequested => {
                Ok(HookResponse::ProviderList(vec![
                    riven_core::types::ProviderInfo {
                        name: "Usenet".to_string(),
                        store: "usenet".to_string(),
                    },
                ]))
            }
            RivenEvent::MediaItemStreamLinkRequested {
                info_hash,
                magnet,
                provider,
                stream_base_url,
            } => {
                if provider
                    .as_deref()
                    .is_some_and(|provider| provider != "usenet")
                {
                    return Ok(HookResponse::Empty);
                }
                let Some(payload) = payload_from_magnet_or_store(ctx, info_hash, magnet).await
                else {
                    return Ok(HookResponse::Empty);
                };
                Ok(HookResponse::StreamLink(StreamLinkResponse {
                    link: build_stream_url(ctx, info_hash, &payload, stream_base_url.as_deref())
                        .await?,
                }))
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}

async fn handle_scrape(event: &RivenEvent, ctx: &PluginContext) -> anyhow::Result<HookResponse> {
    let Some(req) = event.scrape_request() else {
        return Ok(HookResponse::Empty);
    };

    let fallback_servers = configured_servers(&ctx.settings);
    let preflight_on_scrape = setting_bool_default(&ctx.settings, "preflightonscrape", true);
    let mut results = ScrapeResponse::new();

    for source_url in configured_addons(&ctx.settings) {
        match discover_streams(
            ctx,
            &source_url,
            &fallback_servers,
            &req,
            preflight_on_scrape,
        )
        .await
        {
            Ok(streams) => results.extend(streams),
            Err(error) => tracing::warn!(source_url, error = %error, "usenet scrape failed"),
        }
    }

    Ok(HookResponse::Scrape(results))
}

async fn handle_cache_check(
    ctx: &PluginContext,
    hashes: &[String],
) -> anyhow::Result<HookResponse> {
    let mut checks = Vec::new();
    let preflight_required = setting_bool_default(&ctx.settings, "preflightrequired", true);

    for hash in hashes {
        if let Some(payload) = load_payload(ctx, hash).await {
            match preflight_payload(ctx, hash, &payload).await {
                Ok(preflight) if !preflight.files.is_empty() => {
                    checks.push(cache_check_from_preflight(hash, &preflight));
                }
                Ok(_) if !preflight_required => {
                    checks.push(cache_check_from_payload(hash, &payload))
                }
                Err(error) if !preflight_required => {
                    tracing::warn!(hash, error = %error, "usenet preflight failed; falling back to addon metadata");
                    checks.push(cache_check_from_payload(hash, &payload));
                }
                Err(error) => {
                    tracing::debug!(hash, error = %error, "usenet preflight rejected candidate");
                }
                _ => {}
            }
        }
    }

    Ok(HookResponse::CacheCheck(checks))
}

async fn handle_download(
    ctx: &PluginContext,
    info_hash: &str,
    magnet: &str,
) -> anyhow::Result<HookResponse> {
    let Some(payload) = payload_from_magnet_or_store(ctx, info_hash, magnet).await else {
        return Ok(HookResponse::Empty);
    };

    let stream_url = build_stream_url(ctx, info_hash, &payload, None).await?;
    let files = match preflight_payload(ctx, info_hash, &payload).await {
        Ok(preflight) if !preflight.files.is_empty() => preflight
            .files
            .into_iter()
            .map(|file| DownloadFile {
                filename: file.name,
                file_size: file.size.unwrap_or(1),
                download_url: Some(payload.to_magnet().unwrap_or_default()),
                stream_url: Some(stream_url.clone()),
            })
            .collect(),
        Ok(_) | Err(_) => vec![DownloadFile {
            filename: payload.filename.clone(),
            file_size: payload.video_size.unwrap_or(1),
            download_url: Some(payload.to_magnet()?),
            stream_url: Some(stream_url),
        }],
    };

    Ok(HookResponse::Download(Box::new(DownloadResult {
        info_hash: info_hash.to_string(),
        files,
        provider: Some("usenet".to_string()),
        plugin_name: "usenet".to_string(),
    })))
}

async fn payload_from_magnet_or_store(
    ctx: &PluginContext,
    hash: &str,
    magnet: &str,
) -> Option<UsenetPayload> {
    if let Some(payload) = UsenetPayload::from_magnet(magnet) {
        store_payload(ctx, hash, &payload).await;
        return Some(payload);
    }
    load_payload(ctx, hash).await
}

fn cache_check_from_payload(hash: &str, payload: &UsenetPayload) -> CacheCheckResult {
    CacheCheckResult {
        hash: hash.to_string(),
        status: TorrentStatus::Cached,
        files: vec![CacheCheckFile {
            index: 0,
            name: payload.filename.clone(),
            path: payload.filename.clone(),
            size: payload.video_size,
            link: None,
        }],
    }
}

fn cache_check_from_preflight(hash: &str, preflight: &PreflightResult) -> CacheCheckResult {
    CacheCheckResult {
        hash: hash.to_string(),
        status: TorrentStatus::Cached,
        files: preflight
            .files
            .iter()
            .enumerate()
            .map(|(index, file)| CacheCheckFile {
                index: index as u32,
                name: file.name.clone(),
                path: file.name.clone(),
                size: file.size,
                link: None,
            })
            .collect(),
    }
}

pub(crate) fn configured_addons(settings: &PluginSettings) -> Vec<String> {
    split_setting(&settings.get_or("addonurls", ""))
}

fn configured_servers(settings: &PluginSettings) -> Vec<String> {
    split_setting(&settings.get_or("servers", ""))
}

pub(crate) fn split_setting(value: &str) -> Vec<String> {
    value
        .split([',', '\n'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub(crate) fn first_line(value: &str) -> Option<&str> {
    value.lines().map(str::trim).find(|line| !line.is_empty())
}

pub(crate) fn setting_bool_default(settings: &PluginSettings, key: &str, default: bool) -> bool {
    settings
        .get(key)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}
