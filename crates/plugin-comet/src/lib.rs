use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

const DEFAULT_URL: &str = "https://comet.feels.legal";

#[derive(Default)]
pub struct CometPlugin;

register_plugin!(CometPlugin);

#[async_trait]
impl Plugin for CometPlugin {
    fn name(&self) -> &'static str {
        "comet"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemScrapeRequested]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        let base_url = settings.get_or("url", DEFAULT_URL);
        let base_url = base_url.trim_end_matches('/');
        let url = format!("{base_url}/manifest.json");
        let client = reqwest::Client::new();
        match client.get(&url).send().await {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(_) => Ok(false),
        }
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("url", "URL", "url")
                .required()
                .with_default(DEFAULT_URL)
                .with_placeholder(DEFAULT_URL)
                .with_description("Base URL of your Comet instance."),
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

        let base_url = ctx.settings.get_or("url", DEFAULT_URL);
        let base_url = base_url.trim_end_matches('/');

        // Build the identifier suffix and scrape type.
        // Movies:  /stream/movie/{imdbId}.json
        // Shows:   /stream/series/{imdbId}.json
        // Seasons: /stream/series/{imdbId}:{season}.json
        // Episodes:/stream/series/{imdbId}:{season}:{episode}.json
        let (scrape_type, identifier) = match request.item_type {
            MediaItemType::Movie => ("movie", String::new()),
            MediaItemType::Show => ("series", String::new()),
            MediaItemType::Season => ("series", format!(":{}", request.season_or_1())),
            MediaItemType::Episode => {
                let s = request.season_or_1();
                let e = request.episode_or_1();
                ("series", format!(":{s}:{e}"))
            }
        };

        let url = format!("{base_url}/stream/{scrape_type}/{imdb_id}{identifier}.json");

        let resp: CometResponse = match riven_core::http::send(|| ctx.http_client.get(&url)).await?.json().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, imdb_id, title = request.title, "comet response parse failed");
                return Ok(HookResponse::Scrape(HashMap::new()));
            }
        };

        let mut results = HashMap::new();
        for stream in resp.streams {
            let Some(info_hash) = stream.info_hash else {
                continue;
            };

            // Title priority:
            // 1. behaviorHints.filename (exact original filename)
            // 2. First line of description, strip leading emoji char
            let title = if let Some(filename) = stream.behavior_hints.and_then(|h| h.filename) {
                filename
            } else if let Some(desc) = stream.description {
                desc.lines()
                    .next()
                    .unwrap_or("")
                    .chars()
                    .skip(1) // strip leading emoji
                    .collect::<String>()
                    .trim()
                    .to_string()
            } else {
                continue;
            };

            if !title.is_empty() {
                let info_hash = info_hash.to_lowercase();
                results.insert(
                    info_hash.clone(),
                    ScrapeStream {
                        title,
                        magnet: build_magnet_uri(&info_hash),
                    },
                );
            }
        }

        tracing::info!(
            count = results.len(),
            imdb_id,
            title = request.title,
            "comet scrape complete"
        );
        Ok(HookResponse::Scrape(results))
    }
}

#[derive(Deserialize)]
struct CometResponse {
    #[serde(default)]
    streams: Vec<CometStream>,
}

#[derive(Deserialize)]
struct CometStream {
    description: Option<String>,
    #[serde(rename = "infoHash")]
    info_hash: Option<String>,
    #[serde(rename = "behaviorHints")]
    behavior_hints: Option<CometBehaviorHints>,
}

#[derive(Deserialize)]
struct CometBehaviorHints {
    filename: Option<String>,
}
