use async_trait::async_trait;
use reqwest::StatusCode;
use riven_core::events::{EventType, HookResponse, ScrapeRequest};
use riven_core::http::RetryLaterError;
use riven_core::http::profiles;
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::stremio::StremioScrapeConfig;
use riven_core::types::{ScrapeEntry, ScrapeResponse};
use serde::Deserialize;

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

    async fn on_scrape_requested(
        &self,
        request: &ScrapeRequest<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let Some(stremio) = StremioScrapeConfig::from_request(request) else {
            return Ok(HookResponse::Empty);
        };

        let filter = ctx.settings.get_or("filter", DEFAULT_FILTER);
        let url = format!(
            "{TORRENTIO_BASE_URL}{filter}/stream/{kind}/{imdb_id}{suffix}.json",
            kind = stremio.kind.as_str(),
            imdb_id = stremio.imdb_id,
            suffix = stremio.id_suffix(),
        );

        tracing::debug!(
            url = %url,
            imdb_id = stremio.imdb_id,
            scrape_type = stremio.kind.as_str(),
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
            if is_deferred_status(status) {
                tracing::warn!(
                    status = %status,
                    imdb_id = stremio.imdb_id,
                    title = request.title,
                    "torrentio temporarily unavailable; deferring scrape"
                );
                return Err(RetryLaterError.into());
            }
            anyhow::bail!(
                "torrentio returned HTTP {status}: {}",
                body.chars().take(200).collect::<String>()
            );
        }
        let resp: TorrentioResponse = http_resp
            .json()
            .map_err(|e| anyhow::anyhow!("torrentio response parse error for {url}: {e}"))?;

        let results = scrape_results_from_response(resp);

        tracing::info!(
            count = results.len(),
            imdb_id = stremio.imdb_id,
            "torrentio scrape complete"
        );
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


fn is_deferred_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn scrape_results_from_response(resp: TorrentioResponse) -> ScrapeResponse {
    let mut results = ScrapeResponse::new();
    for stream in resp.streams {
        if let Some(info_hash) = stream.info_hash {
            let title = stream_title(stream.title.as_deref());
            if !title.is_empty() {
                results.insert(info_hash.to_lowercase(), ScrapeEntry::new(title));
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
