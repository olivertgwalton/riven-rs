use async_trait::async_trait;
use chrono::Utc;
use riven_db::repo;
use serde::{Deserialize, Serialize};

use riven_core::events::{DownloadSuccessInfo, EventType, HookResponse};
use riven_core::http::{HttpServiceProfile, profiles};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use std::time::Duration;

mod dispatch;
mod metadata;

use dispatch::dispatch_webhooks;
#[cfg(test)]
use dispatch::{NotificationService, build_simple_embed, format_duration, parse_notification_url};
use metadata::{fetch_tmdb_overview, fetch_tvdb_slug};

const TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";
const TVDB_BASE_URL: &str = "https://api4.thetvdb.com/v4";
const TVDB_DEFAULT_API_KEY: &str = "6be85335-5c4f-4d8d-b945-d3ed0eb8cdce";

const TMDB_PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("tmdb").with_rate_limit(40, Duration::from_secs(1));
const TVDB_PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("tvdb").with_rate_limit(25, Duration::from_secs(1));

#[derive(Default)]
pub struct NotificationsPlugin;

#[async_trait]
impl Plugin for NotificationsPlugin {
    fn name(&self) -> &'static str {
        "notifications"
    }

    fn category(&self) -> &'static str {
        "services"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemDownloadSuccess]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        let urls = settings.get_list("urls");
        Ok(!urls.is_empty())
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::{FieldType, SettingField};
        vec![
            SettingField::new("urls", "Webhook URLs", FieldType::Textarea)
                .required()
                .with_placeholder("https://discord.com/api/webhooks/...")
                .with_description(
                    "Comma-separated webhook URLs. Supports Discord and generic JSON endpoints.",
                ),
            SettingField::new("detailed", "Detailed Embeds", FieldType::Boolean).with_description(
                "Show rich Discord embeds with overview, rating, and external links.",
            ),
            SettingField::new(
                "tmdb_api_key",
                "TMDB API Read Access Token",
                FieldType::Password,
            )
            .with_description("Optional. Required for overview text in detailed Discord embeds."),
        ]
    }

    async fn on_download_success(
        &self,
        info: &DownloadSuccessInfo<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let urls = ctx.settings.get_list("urls");
        let detailed = ctx.settings.get_bool("detailed");

        let mut payload = NotificationPayload {
            event: "riven.media-item.download.success".to_string(),
            title: info.title.to_string(),
            full_title: info
                .full_title
                .map_or_else(|| info.title.to_string(), str::to_string),
            item_type: info.item_type,
            year: info.year,
            imdb_id: info.imdb_id.map(str::to_string),
            tmdb_id: info.tmdb_id.map(str::to_string),
            tvdb_id: None,
            poster_path: info.poster_path.map(str::to_string),
            downloader: info.plugin_name.to_string(),
            provider: info.provider.map(str::to_string),
            duration_seconds: info.duration_seconds,
            timestamp: Utc::now().to_rfc3339(),
            is_anime: false,
            rating: None,
            overview: None,
            tvdb_slug: None,
        };

        if !rewrite_for_request_root(ctx, info.id, &mut payload).await? {
            return Ok(HookResponse::Empty);
        }

        if detailed {
            if let Some(api_key) = ctx.settings.get("tmdb_api_key") {
                payload.overview = fetch_tmdb_overview(&ctx.http, api_key, &payload).await;
            }
            if let Some(ref tvdb_id) = payload.tvdb_id.clone() {
                payload.tvdb_slug = fetch_tvdb_slug(&ctx.http, tvdb_id).await;
            }
        }

        dispatch_webhooks(ctx, &urls, &payload, detailed).await;

        Ok(HookResponse::Empty)
    }
}

async fn rewrite_for_request_root(
    ctx: &PluginContext,
    item_id: i64,
    payload: &mut NotificationPayload,
) -> anyhow::Result<bool> {
    let Some(item) = repo::get_media_item(item_id).await? else {
        return Ok(true);
    };

    payload.is_anime = item.is_anime;
    payload.rating = item.rating;
    payload.tvdb_id = item.tvdb_id.clone();

    let Some(request_id) = item.item_request_id else {
        return Ok(true);
    };
    let Some(request) = repo::get_item_request_by_id(request_id).await? else {
        return Ok(false);
    };
    if request.state != ItemRequestState::Completed {
        return Ok(false);
    }
    let Some(root_item) = repo::get_request_root_item(request_id).await? else {
        return Ok(false);
    };
    if !mark_request_notification_sent(ctx, request_id).await? {
        return Ok(false);
    }

    payload.title = root_item.title.clone();
    payload.full_title = root_item
        .full_title
        .clone()
        .unwrap_or_else(|| root_item.title.clone());
    payload.item_type = root_item.item_type;
    payload.year = root_item.year;
    payload.imdb_id = root_item.imdb_id.clone();
    payload.tmdb_id = root_item.tmdb_id.clone();
    payload.tvdb_id = root_item.tvdb_id.clone();
    payload.poster_path = root_item.poster_path.clone();
    payload.is_anime = root_item.is_anime;
    payload.rating = root_item.rating;
    payload.duration_seconds = request
        .completed_at
        .unwrap_or_else(Utc::now)
        .signed_duration_since(request.created_at)
        .to_std()
        .map_or(payload.duration_seconds, |duration| duration.as_secs_f64());
    Ok(true)
}

#[derive(Debug, Serialize)]
struct NotificationPayload {
    event: String,
    title: String,
    full_title: String,
    item_type: MediaItemType,
    year: Option<i32>,
    imdb_id: Option<String>,
    tmdb_id: Option<String>,
    tvdb_id: Option<String>,
    poster_path: Option<String>,
    downloader: String,
    provider: Option<String>,
    duration_seconds: f64,
    timestamp: String,
    is_anime: bool,
    rating: Option<f64>,
    overview: Option<String>,
    #[serde(skip)]
    tvdb_slug: Option<String>,
}

async fn mark_request_notification_sent(
    ctx: &PluginContext,
    request_id: i64,
) -> anyhow::Result<bool> {
    let key = format!("riven:notifications:request-complete:{request_id}");
    let mut conn = ctx.redis.clone();
    let result: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(60 * 60 * 24 * 30)
        .query_async(&mut conn)
        .await?;
    Ok(result.is_some())
}

#[cfg(test)]
mod tests;
