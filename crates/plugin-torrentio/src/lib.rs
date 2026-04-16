use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::http::profiles;
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::types::MediaItemType;

const TORRENTIO_BASE_URL: &str = "http://torrentio.strem.fun/";
const DEFAULT_FILTER: &str = "sort=qualitysize%7Cqualityfilter=threed,480p,scr,cam";
const PEER_COUNT_MARKER: &str = "\u{1F464}";

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

        let scrape_type = scrape_type(request.item_type);
        let url = scrape_url(
            &filter,
            request.item_type,
            imdb_id,
            request.season,
            request.episode,
        );

        tracing::debug!(
            url = %url,
            imdb_id,
            scrape_type,
            season = request.season,
            episode = request.episode,
            "requesting torrentio streams"
        );
        let http_resp = ctx
            .http
            .send_data(profiles::TORRENTIO, Some(url.clone()), |client| {
                client.get(&url)
            })
            .await?;
        let status = http_resp.status();
        if !status.is_success() {
            let body = http_resp.text().unwrap_or_default();
            anyhow::bail!(
                "torrentio returned HTTP {status}: {}",
                body.chars().take(200).collect::<String>()
            );
        }
        let resp: TorrentioResponse = http_resp
            .json()
            .map_err(|e| anyhow::anyhow!("torrentio response parse error for {url}: {e}"))?;

        let results = scrape_results_from_response(resp);

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

fn scrape_url(
    filter: &str,
    item_type: MediaItemType,
    imdb_id: &str,
    season: Option<i32>,
    episode: Option<i32>,
) -> String {
    let (scrape_type, identifier) = match item_type {
        MediaItemType::Movie => (scrape_type(item_type), String::new()),
        _ => {
            let s = season.unwrap_or(1);
            let e = episode.unwrap_or(1);
            (scrape_type(item_type), format!(":{s}:{e}"))
        }
    };

    format!("{TORRENTIO_BASE_URL}{filter}/stream/{scrape_type}/{imdb_id}{identifier}.json")
}

fn scrape_type(item_type: MediaItemType) -> &'static str {
    match item_type {
        MediaItemType::Movie => "movie",
        MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode => "series",
    }
}

fn scrape_results_from_response(resp: TorrentioResponse) -> riven_core::types::ScrapeResponse {
    let mut results = HashMap::new();
    for stream in resp.streams {
        if let Some(info_hash) = stream.info_hash {
            let title = stream_title(stream.title.as_deref());

            if !title.is_empty() {
                results.insert(info_hash.to_lowercase(), title.into());
            }
        }
    }
    results
}

fn stream_title(title: Option<&str>) -> String {
    title
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("")
        .split(PEER_COUNT_MARKER)
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests;
