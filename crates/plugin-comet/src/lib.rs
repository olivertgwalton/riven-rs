use async_trait::async_trait;
use serde::Deserialize;
use std::time::Duration;

use riven_core::events::{EventType, HookResponse, ScrapeRequest};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

const DEFAULT_URL: &str = "https://comet.feels.legal";

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("comet").with_rate_limit(150, Duration::from_secs(60));

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

    async fn validate(
        &self,
        settings: &PluginSettings,
        http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        let base_url = settings.get_or("url", DEFAULT_URL);
        let base_url = base_url.trim_end_matches('/');
        let url = format!("{base_url}/manifest.json");
        match http.send(PROFILE, |client| client.get(&url)).await {
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

    async fn on_scrape_requested(
        &self,
        request: &ScrapeRequest<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
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

        let resp_data = match ctx
            .http
            .send_data(PROFILE, Some(url.clone()), |client| {
                client.get(&url)
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!(error = %e, imdb_id, title = request.title, "comet request failed");
                return Ok(HookResponse::Scrape(ScrapeResponse::new()));
            }
        };
        let resp: CometResponse = match resp_data.json() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, imdb_id, title = request.title, "comet response parse failed");
                return Ok(HookResponse::Scrape(ScrapeResponse::new()));
            }
        };

        let results = scrape_results_from_response(resp);

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

fn scrape_results_from_response(resp: CometResponse) -> ScrapeResponse {
    let mut results = ScrapeResponse::new();
    for stream in resp.streams {
        let Some(info_hash) = stream.info_hash.clone() else {
            continue;
        };
        let Some(title) = title_from_stream(stream) else {
            continue;
        };

        if !title.is_empty() {
            results.insert(info_hash.to_lowercase(), ScrapeEntry::new(title));
        }
    }
    results
}

fn title_from_stream(stream: CometStream) -> Option<String> {
    // Title priority:
    // 1. behaviorHints.filename (exact original filename)
    // 2. First line of description, strip leading emoji char
    stream.behavior_hints.and_then(|h| h.filename).or_else(|| {
        stream.description.map(|desc| {
            desc.lines()
                .next()
                .unwrap_or("")
                .chars()
                .skip(1)
                .collect::<String>()
                .trim()
                .to_string()
        })
    })
}

#[cfg(test)]
mod tests;
