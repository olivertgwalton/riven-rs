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
mod templates;

use dispatch::dispatch_webhooks;
#[cfg(test)]
use dispatch::{
    NotificationService, build_pushbullet_body, build_simple_embed, format_duration,
    parse_notification_url,
};
use metadata::{fetch_tmdb_overview, fetch_tvdb_slug};
use templates::{TemplateCategory, render_template, template_category, template_variables};

const TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";
const TVDB_BASE_URL: &str = "https://api4.thetvdb.com/v4";
const TVDB_DEFAULT_API_KEY: &str = "6be85335-5c4f-4d8d-b945-d3ed0eb8cdce";

/// Shared description for all four template fields. Plain `{{variable}}`
/// substitution, no conditionals — a variable with no value for this
/// download renders as an empty string, so avoid a lone line built entirely
/// from one optional variable (e.g. `Group: {{release_group}}`) if you'd
/// rather it disappear than leave a bare label.
const TEMPLATE_VARIABLES_HELP: &str = "Leave blank to use the default layout. Available in \
     both movie and show templates: {{title}}, {{year}}, {{quality}}, {{resolution}}, \
     {{release_group}}, {{downloader}}, {{provider}}, {{rating}}, {{overview}}, {{poster}}, \
     {{duration}}, {{tmdb_link}}, {{imdb_link}}. Show templates only: {{season}}, \
     {{episode}}, {{episode_title}}, {{tvdb_link}} — empty for a season/show-level \
     completion rather than a single episode.";

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
        &[
            EventType::MediaItemDownloadSuccess,
            EventType::NotificationTestRequested,
        ]
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
                    "Comma-separated webhook URLs, using Apprise-style notation. Supports \
                     Discord (a full webhook URL or discord://id/token), Pushbullet \
                     (pbul://<access_token>), and generic JSON endpoints (json://... or \
                     jsons://...).",
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
            SettingField::new(
                "movie_use_custom_template",
                "Use custom template",
                FieldType::Boolean,
            )
            .with_section("Movie notifications")
            .with_description(
                "Off by default (uses the built-in layout). Switch on to apply the templates \
                 below — leaves them in place, so you can draft a template without it taking \
                 effect yet.",
            ),
            SettingField::new("movie_title_template", "Title", FieldType::Textarea)
                .with_section("Movie notifications")
                .with_placeholder("Downloaded: {{title}} ({{year}})")
                .with_description(TEMPLATE_VARIABLES_HELP),
            SettingField::new("movie_body_template", "Body", FieldType::Textarea)
                .with_section("Movie notifications")
                .with_placeholder(
                    "{{quality}} {{resolution}} by {{release_group}} via {{downloader}}",
                )
                .with_description(TEMPLATE_VARIABLES_HELP),
            SettingField::new(
                "show_use_custom_template",
                "Use custom template",
                FieldType::Boolean,
            )
            .with_section("Show notifications")
            .with_description(
                "Off by default (uses the built-in layout). Switch on to apply the templates \
                 below — leaves them in place, so you can draft a template without it taking \
                 effect yet.",
            ),
            SettingField::new("show_title_template", "Title", FieldType::Textarea)
                .with_section("Show notifications")
                .with_placeholder("Downloaded: {{title}}")
                .with_description(TEMPLATE_VARIABLES_HELP),
            SettingField::new("show_body_template", "Body", FieldType::Textarea)
                .with_section("Show notifications")
                .with_placeholder("{{episode_title}} S{{season}}E{{episode}} — {{quality}}")
                .with_description(TEMPLATE_VARIABLES_HELP),
        ]
    }

    async fn on_download_success(
        &self,
        info: &DownloadSuccessInfo<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
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
            resolution: None,
            quality: None,
            release_group: None,
            season: None,
            episode: None,
            episode_title: None,
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

        // The release behind this download — quality/resolution/group come
        // from the just-created filesystem entry's linked stream, not the
        // item itself, so this always looks up `info.id` (the item that
        // actually triggered this download) rather than any request-root
        // item `rewrite_for_request_root` may have substituted above.
        match repo::get_latest_release_info(info.id).await {
            Ok(Some(release)) => {
                payload.resolution = release.resolution;
                payload.quality = release.quality;
                payload.release_group = release.release_group;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(id = info.id, %error, "failed to look up release info for notification templates");
            }
        }

        render_and_dispatch(ctx, &payload, detailed).await;

        Ok(HookResponse::Empty)
    }

    /// Preview a template with placeholder data — see the trait default's
    /// doc comment. `item_type` selects the dummy payload's category:
    /// `Movie` for the movie templates, anything else for the show
    /// templates (populated as a single test episode so season/episode/
    /// episode_title all render with a value).
    async fn on_notification_test_requested(
        &self,
        item_type: MediaItemType,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let detailed = ctx.settings.get_bool("detailed");
        let payload = dummy_payload(item_type);
        render_and_dispatch(ctx, &payload, detailed).await;
        Ok(HookResponse::Empty)
    }
}

/// Shared tail of `on_download_success` and `on_notification_test_requested`:
/// pick the movie/show template pair, render it if that category's "use
/// custom template" toggle is on, and dispatch to every configured target.
async fn render_and_dispatch(ctx: &PluginContext, payload: &NotificationPayload, detailed: bool) {
    let urls = ctx.settings.get_list("urls");
    let (use_custom_key, title_key, body_key) = match template_category(payload.item_type) {
        TemplateCategory::Movie => (
            "movie_use_custom_template",
            "movie_title_template",
            "movie_body_template",
        ),
        TemplateCategory::Show => (
            "show_use_custom_template",
            "show_title_template",
            "show_body_template",
        ),
    };
    let (custom_title, custom_body) = if ctx.settings.get_bool(use_custom_key) {
        let vars = template_variables(payload);
        (
            ctx.settings
                .get(title_key)
                .map(|t| render_template(t, &vars)),
            ctx.settings
                .get(body_key)
                .map(|t| render_template(t, &vars)),
        )
    } else {
        (None, None)
    };

    dispatch_webhooks(
        ctx,
        &urls,
        payload,
        detailed,
        custom_title.as_deref(),
        custom_body.as_deref(),
    )
    .await;
}

/// Placeholder payload for `on_notification_test_requested`. `item_type` is
/// `Movie` for the movie category; any other value builds a single test
/// episode so every show-only variable (season/episode/episode_title) has a
/// value to preview.
fn dummy_payload(item_type: MediaItemType) -> NotificationPayload {
    let is_movie = item_type == MediaItemType::Movie;
    let name = if is_movie { "Test Movie" } else { "Test Show" };
    NotificationPayload {
        event: "riven.notifications.test-requested".to_string(),
        title: name.to_string(),
        full_title: name.to_string(),
        item_type,
        year: Some(2026),
        imdb_id: Some("tt0000000".to_string()),
        tmdb_id: Some("0".to_string()),
        tvdb_id: (!is_movie).then(|| "0".to_string()),
        poster_path: None,
        downloader: "stremthru".to_string(),
        provider: Some("realdebrid".to_string()),
        duration_seconds: 42.0,
        timestamp: Utc::now().to_rfc3339(),
        is_anime: false,
        rating: Some(7.5),
        overview: Some(
            "This is a test notification, sent from Riven's settings to preview your \
             notification template."
                .to_string(),
        ),
        tvdb_slug: (!is_movie).then(|| "test-show".to_string()),
        resolution: Some("1080p".to_string()),
        quality: Some("WEB-DL".to_string()),
        release_group: Some("GROUP".to_string()),
        season: (!is_movie).then_some(1),
        episode: (!is_movie).then_some(1),
        episode_title: (!is_movie).then(|| "Test Episode".to_string()),
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
    payload.season = item.season_number;
    payload.episode = item.episode_number;
    payload.episode_title = (item.item_type == MediaItemType::Episode).then(|| item.title.clone());

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
    // Overwritten to match the root item rather than kept from the
    // originally-triggering item: a request completing means the whole
    // show/season is done, not specifically whichever episode happened to
    // finish last, so season/episode should reflect that (empty unless the
    // root item is itself a single requested episode).
    payload.season = root_item.season_number;
    payload.episode = root_item.episode_number;
    payload.episode_title =
        (root_item.item_type == MediaItemType::Episode).then(|| root_item.title.clone());
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
    resolution: Option<String>,
    quality: Option<String>,
    release_group: Option<String>,
    season: Option<i32>,
    episode: Option<i32>,
    episode_title: Option<String>,
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
