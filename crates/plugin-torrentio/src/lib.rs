use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use riven_core::register_plugin;

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

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemScrapeRequested]
    }

    async fn validate(&self, _settings: &PluginSettings) -> anyhow::Result<bool> {
        Ok(true)
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
        match event {
            RivenEvent::MediaItemScrapeRequested {
                id: _,
                item_type,
                imdb_id,
                season,
                episode,
                ..
            } => {
                let imdb_id = match imdb_id {
                    Some(id) => id,
                    None => return Ok(HookResponse::Empty),
                };

                let filter = ctx.settings.get_or("filter", DEFAULT_FILTER);

                let (scrape_type, identifier) = match item_type {
                    MediaItemType::Movie => ("movie", String::new()),
                    _ => {
                        let s = season.unwrap_or(1);
                        let e = episode.unwrap_or(1);
                        ("series", format!(":{s}:{e}"))
                    }
                };

                let url = format!(
                    "{TORRENTIO_BASE_URL}{filter}/stream/{scrape_type}/{imdb_id}{identifier}.json"
                );

                let resp: TorrentioResponse = ctx
                    .http_client
                    .get(&url)
                    .send()
                    .await?
                    .json()
                    .await?;

                let mut results = HashMap::new();
                for stream in resp.streams {
                    if let Some(info_hash) = stream.info_hash {
                        // Extract title: first line before "👤" marker
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
                            results.insert(info_hash.to_lowercase(), title);
                        }
                    }
                }

                tracing::info!(count = results.len(), imdb_id, "torrentio scrape complete");
                Ok(HookResponse::Scrape(results))
            }
            _ => Ok(HookResponse::Empty),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_url_parsing() {
        let base = "http://torrentio.strem.fun/";
        let filter = "sort=qualitysize|qualityfilter=threed,480p,scr,cam";
        let scrape_type = "movie";
        let imdb_id = "tt0114709"; // Toy Story

        let url = format!("{}{}/stream/{}/{}.json", base, filter, scrape_type, imdb_id);
        println!("URL: {}", url);
        match reqwest::Url::parse(&url) {
            Ok(_) => println!("Parsed OK!"),
            Err(e) => panic!("Parse failed: {}", e),
        }
    }
}
