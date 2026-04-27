use async_trait::async_trait;
use riven_core::events::{DownloadSuccessInfo, EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{ContentCollection, Plugin, PluginContext, validate_api_key};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use serde::Deserialize;
use std::time::Duration;

const DEFAULT_URL: &str = "http://localhost:5055";

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("seerr").with_rate_limit(20, Duration::from_secs(1));
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

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::ContentServiceRequested,
            EventType::MediaItemDownloadSuccess,
            EventType::MediaItemsDeleted,
        ]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        let url = settings.get_or("url", DEFAULT_URL);
        let base_url = url.trim_end_matches('/');
        validate_api_key(
            http,
            settings,
            "apikey",
            &format!("{base_url}/api/v1/auth/me"),
            "x-api-key",
        )
        .await
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

    async fn query_content(
        &self,
        query: &str,
        args: &serde_json::Value,
        ctx: &PluginContext,
    ) -> anyhow::Result<riven_core::types::ContentServiceResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let url = ctx.settings.get_or("url", DEFAULT_URL);
        let base_url_owned = url.trim_end_matches('/').to_string();
        let filter = args
            .get("filter")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_FILTER)
            .to_string();
        let full = fetch_seerr_content(&ctx.http, api_key, &base_url_owned, &filter).await?;
        Ok(match query {
            "movies" => riven_core::types::ContentServiceResponse {
                movies: full.movies,
                shows: vec![],
            },
            "shows" => riven_core::types::ContentServiceResponse {
                movies: vec![],
                shows: full.shows,
            },
            _ => full,
        })
    }

    async fn on_content_service_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let url = ctx.settings.get_or("url", DEFAULT_URL);
        let base_url = url.trim_end_matches('/');
        fetch_content(ctx, api_key, base_url).await
    }

    async fn on_download_success(
        &self,
        info: &DownloadSuccessInfo<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let url = ctx.settings.get_or("url", DEFAULT_URL);
        let base_url = url.trim_end_matches('/');

        let request_id = get_seerr_request_id(&ctx.db_pool, info.id).await;
        if let Some(rid) = request_id {
            let mark_url = format!("{base_url}/api/v1/request/{rid}/available");
            tracing::debug!(request_id = rid, target_url = %mark_url, "marking seerr request as available");
            if let Err(e) = ctx
                .http
                .send(PROFILE, |client| {
                    client.post(&mark_url).header("x-api-key", api_key)
                })
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

    async fn on_items_deleted(
        &self,
        _item_ids: &[i64],
        external_request_ids: &[String],
        _deleted_paths: &[String],
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let url = ctx.settings.get_or("url", DEFAULT_URL);
        let base_url = url.trim_end_matches('/');

        for rid in external_request_ids {
            let del_url = format!("{base_url}/api/v1/request/{rid}");
            tracing::debug!(request_id = rid, target_url = %del_url, "deleting seerr request");
            if let Err(e) = ctx
                .http
                .send(PROFILE, |client| {
                    client.delete(&del_url).header("x-api-key", api_key)
                })
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
}

async fn get_seerr_request_id(pool: &sqlx::PgPool, id: i64) -> Option<String> {
    use riven_db::repo;

    let item = repo::get_media_item(pool, id).await.ok()??;
    let request_id = item.item_request_id?;
    let request = repo::get_item_request_by_id(pool, request_id)
        .await
        .ok()??;
    request.external_request_id
}

async fn fetch_seerr_content(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    base_url: &str,
    filter: &str,
) -> anyhow::Result<riven_core::types::ContentServiceResponse> {
    let mut content = ContentCollection::default();
    let mut skip = 0u32;
    loop {
        let req_url = format!(
            "{base_url}/api/v1/request?take={PAGE_SIZE}&skip={skip}&filter={filter}&sort=added"
        );
        tracing::debug!(target_url = %req_url, skip, filter, "fetching seerr requests");
        let resp: SeerrRequestResponse = http
            .get_json(PROFILE, req_url.clone(), |client| {
                client.get(&req_url).header("x-api-key", api_key)
            })
            .await?;

        for request in &resp.results {
            let requested_by = request.requested_by.as_ref().and_then(|u| u.email.clone());
            match request.media_type.as_deref() {
                Some("movie") => {
                    if let Some(ref media) = request.media
                        && let Some(tmdb_id) = media.tmdb_id
                    {
                        content.insert_movie(ExternalIds {
                            tmdb_id: Some(tmdb_id.to_string()),
                            external_request_id: Some(request.id.to_string()),
                            requested_by: requested_by.clone(),
                            ..Default::default()
                        });
                    }
                }
                Some("tv") => {
                    if let Some(ref media) = request.media
                        && let Some(tvdb_id) = media.tvdb_id
                    {
                        let seasons: Vec<i32> = request
                            .seasons
                            .iter()
                            .flatten()
                            .filter_map(|s| s.season_number)
                            .collect();
                        content.insert_show(ExternalIds {
                            tvdb_id: Some(tvdb_id.to_string()),
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
                _ => {}
            }
        }

        if resp.results.len() < PAGE_SIZE as usize {
            break;
        }
        skip += PAGE_SIZE;
    }
    Ok(content.into_response())
}

async fn fetch_content(
    ctx: &PluginContext,
    api_key: &str,
    base_url: &str,
) -> anyhow::Result<HookResponse> {
    let filter = ctx.settings.get_or("filter", DEFAULT_FILTER);
    let content = fetch_seerr_content(&ctx.http, api_key, base_url, &filter).await?;
    let mut collection = ContentCollection::default();
    for m in content.movies {
        collection.insert_movie(m);
    }
    for s in content.shows {
        collection.insert_show(s);
    }
    Ok(collection.into_hook_response())
}

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

#[cfg(test)]
mod tests;
