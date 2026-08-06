use async_trait::async_trait;
use reqwest::Method;
use riven_core::events::{DownloadSuccessInfo, EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{FieldType, Plugin, PluginContext, SettingField};
use riven_core::settings::PluginSettings;
use riven_core::types::{ActivePlaybackSession, PlaybackMethod, PlaybackState, artwork_path};
use riven_db::repo;
use serde::Deserialize;
use serde::Serialize;

pub(crate) const EMBY_PROFILE: HttpServiceProfile = HttpServiceProfile::new("emby");
pub(crate) const JELLYFIN_PROFILE: HttpServiceProfile = HttpServiceProfile::new("jellyfin");

fn server_profile(plugin: &str) -> HttpServiceProfile {
    match plugin {
        "emby" => EMBY_PROFILE,
        "jellyfin" => JELLYFIN_PROFILE,
        _ => HttpServiceProfile::new_owned(plugin.to_owned()),
    }
}

#[derive(Default)]
pub struct EmbyPlugin;

#[derive(Default)]
pub struct JellyfinPlugin;

/// Legacy header understood by Emby, and by Jellyfin servers older than 12.0
/// (as a fallback, when legacy auth hasn't been disabled).
const EMBY_TOKEN_HEADER: &str = "X-Emby-Token";

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

/// Builds an authenticated request for a media server.
/// Jellyfin 12.0 disabled the legacy headers i.e. X-Emby-Token by default.
/// Jellyfin reimplementation uses `ApiKey` which supports >=10.8
/// Emby has not made this change continues to use `X-Emby-Token` header.
fn media_server_request(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    api_key: &str,
    plugin: &str,
) -> reqwest::RequestBuilder {
    let req = client.request(method, url);
    if plugin == "jellyfin" {
        req.query(&[("ApiKey", api_key)])
    } else {
        req.header(EMBY_TOKEN_HEADER, api_key)
    }
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
        .send(server_profile(plugin), |client| {
            media_server_request(client, Method::POST, &url, api_key, plugin).json(&body)
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
        .send(server_profile(plugin), |client| {
            media_server_request(client, Method::POST, &url, api_key, plugin)
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
        SettingField::new("url", "Server URL", FieldType::Url)
            .required()
            .with_placeholder("http://localhost:8096"),
        SettingField::new("apikey", "API Key", FieldType::Password).required(),
        SettingField::new("librarypath", "Library Path", FieldType::Text)
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

    let raw_paths = repo::get_media_entry_paths_for_items(&[info.id]).await?;
    if raw_paths.is_empty() {
        tracing::warn!(
            id = info.id,
            title = info.title,
            "{plugin}: no filesystem entries"
        );
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

            fn category(&self) -> &'static str {
                "media"
            }

            fn subscribed_events(&self) -> &[EventType] {
                &[
                    EventType::MediaItemDownloadSuccess,
                    EventType::MediaItemsDeleted,
                    EventType::ActivePlaybackSessionsRequested,
                    EventType::ArtworkRequested,
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

            async fn on_artwork_requested(
                &self,
                server: &str,
                reference: &str,
                ctx: &PluginContext,
            ) -> anyhow::Result<HookResponse> {
                if server != $name {
                    return Ok(HookResponse::Empty);
                }
                let url = ctx
                    .require_setting("url")?
                    .trim_end_matches('/')
                    .to_string();
                let api_key = ctx.require_setting("apikey")?;
                Ok(HookResponse::Artwork(
                    get_artwork(&ctx.http, &url, api_key, $name, reference).await?,
                ))
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

/// The largest artwork riven will relay — see the Plex counterpart. Only exists
/// so a misbehaving upstream cannot stream an unbounded body through riven.
const MAX_ARTWORK_BYTES: usize = 8 * 1024 * 1024;

/// Fetch one artwork image, with the API key in a header rather than the URL.
///
/// `item_id` arrives from the browser, so it is constrained to the id shape
/// Emby and Jellyfin actually use (hex GUIDs, occasionally with dashes) before
/// being interpolated into a path. The request carries an admin key, so an
/// unchecked value here would let a caller aim it at any endpoint on the media
/// server.
async fn get_artwork(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    api_key: &str,
    server: &str,
    item_id: &str,
) -> anyhow::Result<riven_core::events::Artwork> {
    anyhow::ensure!(
        !item_id.is_empty()
            && item_id.len() <= 64
            && item_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "refusing to fetch artwork for an item id that is not alphanumeric"
    );

    let url = format!(
        "{}/Items/{item_id}/Images/Primary",
        base_url.trim_end_matches('/')
    );
    let response = http
        .send(server_profile(server), |client| {
            media_server_request(client, Method::GET, &url, api_key, server)
                .header("accept", "image/*")
        })
        .await?
        .error_for_status()?;

    // An error page served back under riven's own origin would be an HTML
    // injection point, so anything that is not an image is refused.
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    anyhow::ensure!(
        content_type.starts_with("image/"),
        "{server} returned {content_type:?} for artwork, which is not an image"
    );

    let bytes = response.bytes().await?;
    anyhow::ensure!(
        bytes.len() <= MAX_ARTWORK_BYTES,
        "{server} artwork is {} bytes, over the {MAX_ARTWORK_BYTES} limit",
        bytes.len()
    );

    Ok(riven_core::events::Artwork {
        content_type,
        bytes: bytes.to_vec(),
    })
}

async fn get_active_sessions(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    api_key: &str,
    server: &'static str,
) -> anyhow::Result<Vec<ActivePlaybackSession>> {
    let url = format!("{base_url}/Sessions");
    tracing::debug!(server, target_url = %url, "fetching active playback sessions from media server");
    let resp: Vec<MediaServerSession> = http
        .get_json(server_profile(server), url.clone(), |client| {
            media_server_request(client, Method::GET, &url, api_key, server)
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
                .map_or(PlaybackMethod::Unknown, map_media_server_playback_method);

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
                    .map_or(PlaybackState::Unknown, map_media_server_playback_state),
                playback_method,
                position_seconds: position.and_then(|v| u64::try_from(v / 10_000_000).ok()),
                duration_seconds: duration.and_then(|v| u64::try_from(v / 10_000_000).ok()),
                device_name: session.device_name,
                client_name: session.client,
                // A riven-relative path, not a media-server URL. `?api_key=`
                // here put the Emby/Jellyfin admin key into a response every
                // authenticated user can request, and from there into the DOM
                // and the browser's cache. riven proxies the image and keeps
                // the key server-side. Only the item id travels.
                image_url: item_id.map(|id| artwork_path(server, &id)),
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
