use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashSet;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{validate_api_key, Plugin, PluginContext};
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use riven_core::register_plugin;

const DEFAULT_URL: &str = "http://localhost:5055";
const DEFAULT_FILTER: &str = "approved";
const PAGE_SIZE: u32 = 20;

#[derive(Default)]
pub struct SeerrPlugin;

register_plugin!(SeerrPlugin);

#[async_trait]
impl Plugin for SeerrPlugin {
    fn name(&self) -> &'static str {
        "seerr"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::ContentServiceRequested,
            EventType::MediaItemDownloadSuccess,
            EventType::MediaItemsDeleted,
        ]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        let url = settings.get_or("url", DEFAULT_URL);
        let base_url = url.trim_end_matches('/');
        validate_api_key(settings, "apikey", &format!("{base_url}/api/v1/auth/me"), "x-api-key").await
    }


    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("apikey", "API Key", "password").required(),
            SettingField::new("url", "Server URL", "url")
                .with_default("http://localhost:5055")
                .with_placeholder("http://localhost:5055"),
            SettingField::new("filter", "Request Filter", "text")
                .with_default("approved")
                .with_placeholder("approved")
                .with_description("Which request status to import (approved, all, pending)."),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let url = ctx.settings.get_or("url", DEFAULT_URL);
        let base_url = url.trim_end_matches('/');

        match event {
            RivenEvent::ContentServiceRequested => {
                fetch_content(ctx, api_key, base_url).await
            }

            RivenEvent::MediaItemDownloadSuccess { id, item_type, .. } => {
                // Look up the external_request_id for this item (or its parent show).
                let request_id = get_seerr_request_id(&ctx.db_pool, *id, *item_type).await;
                if let Some(rid) = request_id {
                    let mark_url = format!("{base_url}/api/v1/request/{rid}/available");
                    if let Err(e) = ctx
                        .http_client
                        .post(&mark_url)
                        .header("x-api-key", api_key)
                        .send()
                        .await
                        .and_then(|r| r.error_for_status())
                    {
                        tracing::warn!(error = %e, request_id = rid, "failed to mark seerr request as available");
                    } else {
                        tracing::info!(request_id = rid, "marked seerr request as available");
                    }
                }
                Ok(HookResponse::Empty)
            }

            RivenEvent::MediaItemsDeleted { external_request_ids } => {
                for rid in external_request_ids {
                    let del_url = format!("{base_url}/api/v1/request/{rid}");
                    if let Err(e) = ctx
                        .http_client
                        .delete(&del_url)
                        .header("x-api-key", api_key)
                        .send()
                        .await
                        .and_then(|r| r.error_for_status())
                    {
                        tracing::warn!(error = %e, request_id = rid, "failed to delete seerr request");
                    } else {
                        tracing::info!(request_id = rid, "deleted seerr request");
                    }
                }
                Ok(HookResponse::Empty)
            }

            _ => Ok(HookResponse::Empty),
        }
    }
}

// ── Helpers ──

/// Fetch the Seerr request ID linked to a media item.
/// For seasons/episodes walks up to the show so we use the show's request.
async fn get_seerr_request_id(
    pool: &sqlx::PgPool,
    id: i64,
    _item_type: MediaItemType,
) -> Option<String> {
    use riven_db::repo;

    let item = repo::get_media_item(pool, id).await.ok()??;

    // For seasons/episodes, the item_request_id is on the item itself (propagated
    // from the show at index time), so we can use it directly.
    let request_id = item.item_request_id?;
    let request = repo::get_item_request_by_id(pool, request_id).await.ok()??;
    request.external_request_id
}

async fn fetch_content(
    ctx: &PluginContext,
    api_key: &str,
    base_url: &str,
) -> anyhow::Result<HookResponse> {
    let filter = ctx.settings.get_or("filter", DEFAULT_FILTER);

    let mut movies = Vec::new();
    let mut shows = Vec::new();
    let mut seen_movies = HashSet::new();
    let mut seen_shows = HashSet::new();

    let mut skip = 0u32;
    loop {
        let req_url = format!(
            "{base_url}/api/v1/request?take={PAGE_SIZE}&skip={skip}&filter={filter}&sort=added"
        );
        let resp: SeerrRequestResponse = ctx
            .http_client
            .get(&req_url)
            .header("x-api-key", api_key)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        for request in &resp.results {
            let requested_by = request
                .requested_by
                .as_ref()
                .and_then(|u| u.email.clone());

            match request.media_type.as_deref() {
                Some("movie") => {
                    if let Some(ref media) = request.media {
                        if let Some(tmdb_id) = media.tmdb_id {
                            let key = tmdb_id.to_string();
                            if seen_movies.insert(key.clone()) {
                                movies.push(ExternalIds {
                                    tmdb_id: Some(key),
                                    external_request_id: Some(request.id.to_string()),
                                    requested_by: requested_by.clone(),
                                    ..Default::default()
                                });
                            }
                        }
                    }
                }
                Some("tv") => {
                    if let Some(ref media) = request.media {
                        if let Some(tvdb_id) = media.tvdb_id {
                            let key = tvdb_id.to_string();
                            let seasons: Vec<i32> = request
                                .seasons
                                .iter()
                                .flatten()
                                .filter_map(|s| s.season_number)
                                .collect();

                            if seen_shows.insert(key.clone()) {
                                shows.push(ExternalIds {
                                    tvdb_id: Some(key),
                                    external_request_id: Some(request.id.to_string()),
                                    requested_by: requested_by.clone(),
                                    requested_seasons: if seasons.is_empty() {
                                        None
                                    } else {
                                        Some(seasons)
                                    },
                                    ..Default::default()
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if resp.results.len() < PAGE_SIZE as usize {
            break;
        }
        skip += PAGE_SIZE;
    }

    Ok(HookResponse::ContentService(Box::new(ContentServiceResponse {
        movies,
        shows,
    })))
}

// ── Seerr API types ──

#[derive(Deserialize)]
struct SeerrRequestResponse {
    results: Vec<SeerrRequest>,
}

#[derive(Deserialize)]
struct SeerrRequest {
    id: i64,
    #[serde(rename = "type")]
    media_type: Option<String>,
    media: Option<SeerrMedia>,
    #[serde(rename = "requestedBy")]
    requested_by: Option<SeerrUser>,
    #[serde(default)]
    seasons: Option<Vec<SeerrSeason>>,
}

#[derive(Deserialize)]
struct SeerrMedia {
    #[serde(rename = "tmdbId")]
    tmdb_id: Option<i64>,
    #[serde(rename = "tvdbId")]
    tvdb_id: Option<i64>,
}

#[derive(Deserialize)]
struct SeerrUser {
    email: Option<String>,
}

#[derive(Deserialize)]
struct SeerrSeason {
    #[serde(rename = "seasonNumber")]
    season_number: Option<i32>,
}
