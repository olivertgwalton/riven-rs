use async_trait::async_trait;
use reqwest::Method;
use riven_core::events::{DownloadSuccessInfo, EventType, HookResponse};
use riven_core::http::profiles;
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::{ActivePlaybackSession, PlaybackMethod, PlaybackState};
use riven_db::repo;
use serde::Deserialize;
use serde::Serialize;

#[derive(Default)]
pub struct EmbyPlugin;

#[derive(Default)]
pub struct JellyfinPlugin;

register_plugin!(EmbyPlugin);
register_plugin!(JellyfinPlugin);

const MEDIA_SERVER_TOKEN_HEADER: &str = "X-Emby-Token";

#[derive(Serialize)]
struct LibraryUpdate<'a> {
    #[serde(rename = "Updates")]
    updates: Vec<PathUpdate<'a>>,
}

#[derive(Serialize)]
struct PathUpdate<'a> {
    #[serde(rename = "Path")]
    path: &'a str,
    #[serde(rename = "UpdateType")]
    update_type: &'a str,
}

fn media_server_request(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    api_key: &str,
) -> reqwest::RequestBuilder {
    client
        .request(method, url)
        .header(MEDIA_SERVER_TOKEN_HEADER, api_key)
}

/// Notify a Jellyfin/Emby server that the given VFS paths were created.
/// All paths are sent in a single request.
pub(crate) async fn notify_paths(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    api_key: &str,
    paths: &[String],
    update_type: &str,
    plugin: &'static str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}/Library/Media/Updated");
    let updates = paths
        .iter()
        .map(|p| PathUpdate {
            path: p,
            update_type,
        })
        .collect();

    tracing::debug!(plugin, target_url = %url, path_count = paths.len(), update_type, "notifying media server about updated library paths");
    let body = LibraryUpdate { updates };
    let resp = http
        .send(profiles::media_server(plugin), |client| {
            media_server_request(client, Method::POST, &url, api_key).json(&body)
        })
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("{plugin} notify failed: {}", resp.status());
    }

    tracing::info!(plugin, paths = paths.len(), "library paths notified");
    Ok(())
}

async fn refresh_library(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    api_key: &str,
    plugin: &'static str,
) -> anyhow::Result<()> {
    let url = format!("{base_url}/Library/Refresh");
    tracing::debug!(plugin, target_url = %url, "requesting media server library refresh");
    let resp = http
        .send(profiles::media_server(plugin), |client| {
            media_server_request(client, Method::POST, &url, api_key)
        })
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("{plugin} refresh failed: {}", resp.status());
    }

    tracing::info!(plugin, "library refresh requested");
    Ok(())
}

fn media_server_settings_schema() -> Vec<SettingField> {
    vec![
        SettingField::new("url", "Server URL", "url")
            .required()
            .with_placeholder("http://localhost:8096"),
        SettingField::new("apikey", "API Key", "password").required(),
        SettingField::new("librarypath", "Library Path", "text")
            .with_default("/mount")
            .with_placeholder("/mount")
            .with_description("Path Jellyfin/Emby uses to reference the Riven VFS mount."),
    ]
}

async fn notify_download_success(
    plugin: &'static str,
    info: &DownloadSuccessInfo<'_>,
    ctx: &PluginContext,
) -> anyhow::Result<HookResponse> {
    let url = ctx
        .require_setting("url")?
        .trim_end_matches('/')
        .to_string();
    let api_key = ctx.require_setting("apikey")?;
    let library_path = ctx.settings.get_or("librarypath", "/mount");

    let raw_paths = repo::get_media_entry_paths_for_items(&ctx.db_pool, &[info.id]).await?;
    if raw_paths.is_empty() {
        tracing::warn!(id = info.id, title = info.title, "{plugin}: no filesystem entries");
        return Ok(HookResponse::Empty);
    }
    let paths: Vec<String> = raw_paths
        .into_iter()
        .map(|path| rewrite_media_path(&library_path, &path))
        .collect();
    if plugin == "jellyfin" {
        refresh_library(&ctx.http, &url, api_key, plugin).await?;
    } else {
        notify_paths(&ctx.http, &url, api_key, &paths, "Created", plugin).await?;
    }
    Ok(HookResponse::Empty)
}

async fn notify_items_deleted(
    plugin: &'static str,
    deleted_paths: &[String],
    ctx: &PluginContext,
) -> anyhow::Result<HookResponse> {
    if deleted_paths.is_empty() {
        return Ok(HookResponse::Empty);
    }
    let url = ctx
        .require_setting("url")?
        .trim_end_matches('/')
        .to_string();
    let api_key = ctx.require_setting("apikey")?;
    let library_path = ctx.settings.get_or("librarypath", "/mount");

    if plugin == "jellyfin" {
        refresh_library(&ctx.http, &url, api_key, plugin).await?;
    } else {
        let paths: Vec<String> = deleted_paths
            .iter()
            .map(|path| rewrite_media_path(&library_path, path))
            .collect();
        notify_paths(&ctx.http, &url, api_key, &paths, "Deleted", plugin).await?;
    }
    Ok(HookResponse::Empty)
}

fn rewrite_media_path(library_path: &str, media_path: &str) -> String {
    let library_path = library_path.trim_end_matches('/');
    let media_path = media_path.trim_start_matches('/');
    format!("{library_path}/{media_path}")
}

macro_rules! impl_media_server_plugin {
    ($plugin_ty:ident, $name:literal) => {
        #[async_trait]
        impl Plugin for $plugin_ty {
            fn name(&self) -> &'static str {
                $name
            }

            fn subscribed_events(&self) -> &[EventType] {
                &[
                    EventType::MediaItemDownloadSuccess,
                    EventType::MediaItemsDeleted,
                    EventType::ActivePlaybackSessionsRequested,
                ]
            }

            async fn validate(
                &self,
                settings: &PluginSettings,
                _http: &riven_core::http::HttpClient,
            ) -> anyhow::Result<bool> {
                Ok(settings.has("url") && settings.has("apikey"))
            }

            fn settings_schema(&self) -> Vec<SettingField> {
                media_server_settings_schema()
            }

            async fn on_active_playback_sessions_requested(
                &self,
                ctx: &PluginContext,
            ) -> anyhow::Result<HookResponse> {
                let url = ctx
                    .require_setting("url")?
                    .trim_end_matches('/')
                    .to_string();
                let api_key = ctx.require_setting("apikey")?;
                let sessions = get_active_sessions(&ctx.http, &url, api_key, $name).await?;
                Ok(HookResponse::ActivePlaybackSessions(sessions))
            }

            async fn on_download_success(
                &self,
                info: &DownloadSuccessInfo<'_>,
                ctx: &PluginContext,
            ) -> anyhow::Result<HookResponse> {
                notify_download_success($name, info, ctx).await
            }

            async fn on_items_deleted(
                &self,
                _item_ids: &[i64],
                _external_request_ids: &[String],
                deleted_paths: &[String],
                ctx: &PluginContext,
            ) -> anyhow::Result<HookResponse> {
                notify_items_deleted($name, deleted_paths, ctx).await
            }
        }
    };
}

impl_media_server_plugin!(EmbyPlugin, "emby");
impl_media_server_plugin!(JellyfinPlugin, "jellyfin");

async fn get_active_sessions(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    api_key: &str,
    server: &'static str,
) -> anyhow::Result<Vec<ActivePlaybackSession>> {
    let url = format!("{base_url}/Sessions");
    tracing::debug!(server, target_url = %url, "fetching active playback sessions from media server");
    let resp: Vec<MediaServerSession> = http
        .get_json(profiles::media_server(server), url.clone(), |client| {
            media_server_request(client, Method::GET, &url, api_key)
        })
        .await?;

    Ok(resp
        .into_iter()
        .filter_map(|session| {
            let item = session.now_playing_item?;
            let item_id = item.id.clone();
            let duration = item.run_time_ticks;
            let position = session
                .play_state
                .as_ref()
                .and_then(|state| state.position_ticks);
            let playback_method = session
                .play_state
                .as_ref()
                .map(map_media_server_playback_method)
                .unwrap_or(PlaybackMethod::Unknown);

            Some(ActivePlaybackSession {
                server: server.to_string(),
                user_name: session.user_name,
                parent_title: item.series_name,
                item_title: item.name.unwrap_or_else(|| "Unknown item".to_string()),
                item_type: item.item_type,
                season_number: item.parent_index_number,
                episode_number: item.index_number,
                playback_state: session
                    .play_state
                    .as_ref()
                    .map(map_media_server_playback_state)
                    .unwrap_or(PlaybackState::Unknown),
                playback_method,
                position_seconds: position.and_then(|v| u64::try_from(v / 10_000_000).ok()),
                duration_seconds: duration.and_then(|v| u64::try_from(v / 10_000_000).ok()),
                device_name: session.device_name,
                client_name: session.client,
                image_url: item_id
                    .map(|id| format!("{base_url}/Items/{id}/Images/Primary?api_key={api_key}")),
            })
        })
        .collect())
}

fn map_media_server_playback_state(play_state: &MediaServerPlayState) -> PlaybackState {
    if play_state.is_paused.unwrap_or(false) {
        PlaybackState::Paused
    } else if play_state.is_paused.is_some() {
        PlaybackState::Playing
    } else {
        PlaybackState::Unknown
    }
}

fn map_media_server_playback_method(play_state: &MediaServerPlayState) -> PlaybackMethod {
    match play_state.play_method.as_deref().unwrap_or_default() {
        "DirectPlay" => PlaybackMethod::DirectPlay,
        "DirectStream" => PlaybackMethod::DirectStream,
        "Transcode" | "Transcoding" => PlaybackMethod::Transcode,
        _ => PlaybackMethod::Unknown,
    }
}

#[derive(Deserialize)]
struct MediaServerSession {
    #[serde(rename = "UserName")]
    user_name: Option<String>,
    #[serde(rename = "DeviceName")]
    device_name: Option<String>,
    #[serde(rename = "Client")]
    client: Option<String>,
    #[serde(rename = "NowPlayingItem")]
    now_playing_item: Option<MediaServerNowPlayingItem>,
    #[serde(rename = "PlayState")]
    play_state: Option<MediaServerPlayState>,
}

#[derive(Deserialize)]
struct MediaServerNowPlayingItem {
    #[serde(rename = "Id")]
    id: Option<String>,
    #[serde(rename = "SeriesName")]
    series_name: Option<String>,
    #[serde(rename = "Name")]
    name: Option<String>,
    #[serde(rename = "Type")]
    item_type: Option<String>,
    #[serde(rename = "ParentIndexNumber")]
    parent_index_number: Option<i32>,
    #[serde(rename = "IndexNumber")]
    index_number: Option<i32>,
    #[serde(rename = "RunTimeTicks")]
    run_time_ticks: Option<i64>,
}

#[derive(Deserialize)]
struct MediaServerPlayState {
    #[serde(rename = "PositionTicks")]
    position_ticks: Option<i64>,
    #[serde(rename = "IsPaused")]
    is_paused: Option<bool>,
    #[serde(rename = "PlayMethod")]
    play_method: Option<String>,
}

#[cfg(test)]
mod tests;
