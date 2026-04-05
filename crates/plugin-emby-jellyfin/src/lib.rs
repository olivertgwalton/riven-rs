use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
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
    update_type: &'static str,
}

/// Notify a Jellyfin/Emby server that the given VFS paths were created.
/// All paths are sent in a single request.
pub(crate) async fn notify_paths(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    paths: &[String],
    plugin: &'static str,
) -> anyhow::Result<()> {
    let updates = paths
        .iter()
        .map(|p| PathUpdate {
            path: p,
            update_type: "Created",
        })
        .collect();

    let resp = client
        .post(format!("{base_url}/Library/Media/Updated"))
        .query(&[("api_key", api_key)])
        .json(&LibraryUpdate { updates })
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("{plugin} notify failed: {}", resp.status());
    }

    tracing::info!(plugin, paths = paths.len(), "library paths notified");
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

async fn notify_media_server(
    plugin: &'static str,
    event: &RivenEvent,
    ctx: &PluginContext,
) -> anyhow::Result<HookResponse> {
    let RivenEvent::MediaItemDownloadSuccess { id, title, .. } = event else {
        return Ok(HookResponse::Empty);
    };

    let url = ctx
        .require_setting("url")?
        .trim_end_matches('/')
        .to_string();
    let api_key = ctx.require_setting("apikey")?;
    let library_path = ctx.settings.get_or("librarypath", "/mount");
    let entries = repo::get_media_entries(&ctx.db_pool, *id).await?;
    if entries.is_empty() {
        tracing::warn!(id, title, "{plugin}: no filesystem entries");
        return Ok(HookResponse::Empty);
    }
    let paths: Vec<String> = entries
        .into_iter()
        .map(|entry| rewrite_media_path(&library_path, &entry.path))
        .collect();
    notify_paths(&ctx.http_client, &url, api_key, &paths, plugin).await?;
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
                    EventType::ActivePlaybackSessionsRequested,
                ]
            }

            async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
                Ok(settings.has("url") && settings.has("apikey"))
            }

            fn settings_schema(&self) -> Vec<SettingField> {
                media_server_settings_schema()
            }

            async fn handle_event(
                &self,
                event: &RivenEvent,
                ctx: &PluginContext,
            ) -> anyhow::Result<HookResponse> {
                match event {
                    RivenEvent::ActivePlaybackSessionsRequested => {
                        let url = ctx
                            .require_setting("url")?
                            .trim_end_matches('/')
                            .to_string();
                        let api_key = ctx.require_setting("apikey")?;
                        let sessions =
                            get_active_sessions(&ctx.http_client, &url, api_key, $name).await?;
                        Ok(HookResponse::ActivePlaybackSessions(sessions))
                    }
                    _ => notify_media_server($name, event, ctx).await,
                }
            }
        }
    };
}

impl_media_server_plugin!(EmbyPlugin, "emby");
impl_media_server_plugin!(JellyfinPlugin, "jellyfin");

async fn get_active_sessions(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    server: &'static str,
) -> anyhow::Result<Vec<ActivePlaybackSession>> {
    let resp: Vec<MediaServerSession> = client
        .get(format!("{base_url}/Sessions"))
        .query(&[("api_key", api_key)])
        .send()
        .await?
        .json()
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
                position_seconds: position.map(|value| value / 10_000_000),
                duration_seconds: duration.map(|value| value / 10_000_000),
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
