use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::types::MediaItemType;

const TORRENTIO_BASE_URL: &str = "http://torrentio.strem.fun/";
const DEFAULT_FILTER: &str = "sort=qualitysize%7Cqualityfilter=threed,480p,scr,cam";

#[derive(Default)]
pub struct TorrentioPlugin;

register_plugin!(TorrentioPlugin);

#[async_trait]
impl Plugin for TorrentioPlugin {
    fn name(&self) -> &'static str {
        "torrentio"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemScrapeRequested]
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("filter", "Filter", "text")
                .with_default("sort=qualitysize|qualityfilter=threed,480p,scr,cam")
                .with_placeholder("sort=qualitysize|qualityfilter=...")
                .with_description("Torrentio filter/sort query string."),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let Some(request) = event.scrape_request() else {
            return Ok(HookResponse::Empty);
        };
        let Some(imdb_id) = request.imdb_id else {
            return Ok(HookResponse::Empty);
        };

        let filter = ctx.settings.get_or("filter", DEFAULT_FILTER);

        let (scrape_type, identifier) = match request.item_type {
            MediaItemType::Movie => ("movie", String::new()),
            _ => {
                let s = request.season_or_1();
                let e = request.episode_or_1();
                ("series", format!(":{s}:{e}"))
            }
        };

        let url =
            format!("{TORRENTIO_BASE_URL}{filter}/stream/{scrape_type}/{imdb_id}{identifier}.json");

        tracing::debug!(
            url = %url,
            imdb_id,
            scrape_type,
            season = request.season,
            episode = request.episode,
            "requesting torrentio streams"
        );
        let http_resp = riven_core::http::send(|| ctx.http_client.get(&url)).await?;
        let status = http_resp.status();
        if !status.is_success() {
            let body = http_resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "torrentio returned HTTP {status}: {}",
                body.chars().take(200).collect::<String>()
            );
        }
        let resp: TorrentioResponse = http_resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("torrentio response parse error for {url}: {e}"))?;

        let mut results = HashMap::new();
        for stream in resp.streams {
            if let Some(info_hash) = stream.info_hash {
                let title = stream
                    .title
                    .as_deref()
                    .unwrap_or("")
                    .lines()
                    .next()
                    .unwrap_or("")
                    .split("👤")
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string();

                if !title.is_empty() {
                    let info_hash = info_hash.to_lowercase();
                    results.insert(info_hash, title);
                }
            }
        }

        tracing::info!(count = results.len(), imdb_id, "torrentio scrape complete");
        Ok(HookResponse::Scrape(results))
    }
}

#[derive(Deserialize)]
struct TorrentioResponse {
    #[serde(default)]
    streams: Vec<TorrentioStream>,
}

#[derive(Deserialize)]
struct TorrentioStream {
    title: Option<String>,
    #[serde(rename = "infoHash")]
    info_hash: Option<String>,
}
