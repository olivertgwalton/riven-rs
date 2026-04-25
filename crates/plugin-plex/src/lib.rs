use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use riven_core::events::{DownloadSuccessInfo, EventType, HookResponse};
use riven_core::http::profiles;
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::{FilesystemSettings, LibraryProfileMembership, PluginSettings};
use riven_core::types::{ActivePlaybackSession, PlaybackMethod, PlaybackState};
use riven_db::repo;

const SECTIONS_CACHE_TTL: Duration = Duration::from_secs(300);

pub struct PlexPlugin {
    sections_cache: Arc<RwLock<Option<(Instant, Vec<PlexSection>)>>>,
}

impl Default for PlexPlugin {
    fn default() -> Self {
        Self {
            sections_cache: Arc::new(RwLock::new(None)),
        }
    }
}

register_plugin!(PlexPlugin);

#[async_trait]
impl Plugin for PlexPlugin {
    fn name(&self) -> &'static str {
        "plex"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::MediaItemDownloadSuccess,
            EventType::ActivePlaybackSessionsRequested,
        ]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(settings.has("plextoken") && settings.has("plexserverurl"))
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("plextoken", "Plex Token", "password").required(),
            SettingField::new("plexserverurl", "Server URL", "url")
                .required()
                .with_placeholder("http://localhost:32400"),
            SettingField::new("plexlibrarypath", "Library Path", "text")
                .with_default("/mount")
                .with_placeholder("/mount")
                .with_description("Path Plex uses to reference the VFS mount."),
        ]
    }

    async fn on_download_success(
        &self,
        info: &DownloadSuccessInfo<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let id = info.id;
        tracing::info!(id, "plex: handling download success event");
        let plex_token = ctx.require_setting("plextoken")?;
        let plex_url = ctx.require_setting("plexserverurl")?.trim_end_matches('/');

        let entries = repo::get_media_entries_recursive(&ctx.db_pool, id).await?;
        if entries.is_empty() {
            anyhow::bail!("no filesystem entries found for media item {id}");
        }
        tracing::debug!(id, count = entries.len(), "plex: found filesystem entries");

        let fs_settings = load_filesystem_settings(&ctx.db_pool).await;
        let library_path =
            effective_library_path(&ctx.settings, fs_settings.as_ref(), &ctx.vfs_mount_path);

        let sections = self
            .cached_library_sections(&ctx.http, plex_url, plex_token)
            .await?;
        tracing::debug!(count = sections.len(), "plex: fetched library sections");

        let section_locations: Vec<String> = sections
            .iter()
            .flat_map(|s| s.locations.iter().map(|l| l.path.clone()))
            .collect();

        let mut refresh_tasks = Vec::new();
        let mut all_vfs_dirs: Vec<String> = Vec::new();
        for entry in &entries {
            let dir_path = entry
                .path
                .rsplit_once('/')
                .map(|(dir, _)| dir)
                .unwrap_or(&entry.path);

            let profile_keys =
                LibraryProfileMembership::from_json(entry.library_profiles.as_ref());
            let vfs_dirs = entry_vfs_dirs(
                dir_path,
                &library_path,
                &profile_keys,
                fs_settings.as_ref(),
            );

            for full_path in &vfs_dirs {
                for section in &sections {
                    for location in &section.locations {
                        if full_path.starts_with(&location.path) {
                            refresh_tasks.push((section.key.clone(), full_path.clone()));
                        }
                    }
                }
                all_vfs_dirs.push(full_path.clone());
            }
        }

        if refresh_tasks.is_empty() {
            tracing::warn!(
                id,
                tried_paths = ?all_vfs_dirs,
                plex_section_locations = ?section_locations,
                "plex: no library sections matched any entry paths — set plexlibrarypath to the path prefix Plex uses to see the VFS mount"
            );
            return Ok(HookResponse::Empty);
        }

        let results = futures::future::join_all(refresh_tasks.into_iter().map(
            |(section_key, path)| {
                let http = ctx.http.clone();
                let token = plex_token.to_string();
                let url = plex_url.to_string();
                async move {
                    let result = refresh_section(&http, &url, &token, &section_key, &path).await;
                    (section_key, path, result)
                }
            },
        ))
        .await;

        for (section_key, path, result) in results {
            match result {
                Ok(()) => tracing::info!(
                    section = section_key,
                    path,
                    "plex library section refreshed"
                ),
                Err(e) => tracing::warn!(
                    section = section_key,
                    path,
                    error = %e,
                    "plex library section refresh failed"
                ),
            }
        }

        Ok(HookResponse::Empty)
    }

    async fn on_active_playback_sessions_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let plex_token = ctx.require_setting("plextoken")?;
        let plex_url = ctx.require_setting("plexserverurl")?.trim_end_matches('/');
        let sessions = get_active_sessions(&ctx.http, plex_url, plex_token).await?;
        Ok(HookResponse::ActivePlaybackSessions(sessions))
    }
}

async fn load_filesystem_settings(pool: &sqlx::PgPool) -> Option<FilesystemSettings> {
    riven_db::repo::get_setting(pool, "filesystem")
        .await
        .ok()
        .flatten()
        .and_then(|v| serde_json::from_value(v).ok())
}

/// Returns the effective Plex library path: the explicit plugin setting if configured,
/// otherwise the VFS mount path from filesystem settings, otherwise the app-level VFS mount path.
fn effective_library_path(
    settings: &riven_core::settings::PluginSettings,
    fs_settings: Option<&FilesystemSettings>,
    app_vfs_mount_path: &str,
) -> String {
    if let Some(explicit) = settings.get("plexlibrarypath") {
        return explicit.trim_end_matches('/').to_string();
    }
    let from_fs = fs_settings
        .map(|s| s.mount_path.trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty());
    if let Some(path) = from_fs {
        return path;
    }
    let app_path = app_vfs_mount_path.trim_end_matches('/');
    if !app_path.is_empty() {
        return app_path.to_string();
    }
    "/mount".to_string()
}

/// Returns all VFS directory paths an entry appears at, given its canonical dir path and profile keys.
fn entry_vfs_dirs(
    canonical_dir: &str,
    plex_library_path: &str,
    profile_keys: &LibraryProfileMembership,
    fs_settings: Option<&FilesystemSettings>,
) -> Vec<String> {
    let base = plex_library_path.trim_end_matches('/');
    let mut paths = Vec::new();
    let mut any_exclusive = false;

    if let Some(settings) = fs_settings {
        for key in &profile_keys.0 {
            if let Some(profile) = settings.library_profiles.get(key)
                && profile.enabled
            {
                paths.push(format!(
                    "{base}{}{canonical_dir}",
                    profile.library_path
                ));
                if profile.exclusive {
                    any_exclusive = true;
                }
            }
        }
    }

    if !any_exclusive {
        paths.push(format!("{base}{canonical_dir}"));
    }

    paths
}

impl PlexPlugin {
    async fn cached_library_sections(
        &self,
        http: &riven_core::http::HttpClient,
        plex_url: &str,
        token: &str,
    ) -> anyhow::Result<Vec<PlexSection>> {
        {
            let cache = self.sections_cache.read().await;
            if let Some((fetched_at, ref sections)) = *cache
                && fetched_at.elapsed() < SECTIONS_CACHE_TTL
            {
                tracing::debug!("using cached plex library sections");
                return Ok(sections.clone());
            }
        }

        let sections = get_library_sections(http, plex_url, token).await?;
        *self.sections_cache.write().await = Some((Instant::now(), sections.clone()));
        Ok(sections)
    }
}

async fn get_library_sections(
    http: &riven_core::http::HttpClient,
    plex_url: &str,
    token: &str,
) -> anyhow::Result<Vec<PlexSection>> {
    let url = format!("{plex_url}/library/sections");
    tracing::debug!(target_url = %url, "fetching plex library sections");
    let resp: PlexSectionsResponse = http
        .get_json(profiles::PLEX, url.clone(), |client| {
            client
                .get(&url)
                .header("x-plex-token", token)
                .header("accept", "application/json")
        })
        .await?;

    Ok(resp.media_container.directory)
}

async fn refresh_section(
    http: &riven_core::http::HttpClient,
    plex_url: &str,
    token: &str,
    section_key: &str,
    path: &str,
) -> anyhow::Result<()> {
    let encoded_path = urlencoding::encode(path);
    let url = format!("{plex_url}/library/sections/{section_key}/refresh?path={encoded_path}");
    tracing::debug!(target_url = %url, section_key, path, "refreshing plex library section");
    let response = http
        .send(profiles::PLEX, |client| {
            client
                .post(&url)
                .header("x-plex-token", token)
                .header("accept", "application/json")
        })
        .await?;
    response.error_for_status()?;
    Ok(())
}

async fn get_active_sessions(
    http: &riven_core::http::HttpClient,
    plex_url: &str,
    token: &str,
) -> anyhow::Result<Vec<ActivePlaybackSession>> {
    let url = format!("{plex_url}/status/sessions");
    tracing::debug!(target_url = %url, "fetching plex active sessions");
    let resp: PlexSessionsResponse = http
        .get_json(profiles::PLEX, url.clone(), |client| {
            client
                .get(&url)
                .header("x-plex-token", token)
                .header("accept", "application/json")
        })
        .await?;

    Ok(resp
        .media_container
        .metadata
        .into_iter()
        .map(|item| {
            let PlexSessionMetadata {
                item_type,
                title,
                grandparent_title,
                duration,
                parent_index,
                index,
                thumb,
                view_offset,
                transcode_session,
                player,
                user,
            } = item;
            let (device_name, client_name, playback_state) = match player {
                Some(player) => (
                    player.title,
                    player.product,
                    map_playback_state(player.state.as_deref()),
                ),
                None => (None, None, PlaybackState::Unknown),
            };
            let playback_method = map_playback_method(transcode_session.is_some());

            ActivePlaybackSession {
                server: "plex".to_string(),
                user_name: user.and_then(|user| user.title),
                parent_title: grandparent_title.clone(),
                item_title: title
                    .or(grandparent_title)
                    .unwrap_or_else(|| "Unknown item".to_string()),
                item_type,
                season_number: parent_index,
                episode_number: index,
                playback_state,
                playback_method,
                position_seconds: view_offset.and_then(|v| u64::try_from(v / 1000).ok()),
                duration_seconds: duration.and_then(|v| u64::try_from(v / 1000).ok()),
                device_name,
                client_name,
                image_url: thumb
                    .as_ref()
                    .map(|thumb| format!("{plex_url}{thumb}?X-Plex-Token={token}")),
            }
        })
        .collect())
}

fn map_playback_state(state: Option<&str>) -> PlaybackState {
    match state.unwrap_or_default().to_ascii_lowercase().as_str() {
        "playing" => PlaybackState::Playing,
        "paused" => PlaybackState::Paused,
        "buffering" => PlaybackState::Buffering,
        "stopped" => PlaybackState::Idle,
        _ => PlaybackState::Unknown,
    }
}

fn map_playback_method(is_transcoding: bool) -> PlaybackMethod {
    if is_transcoding {
        PlaybackMethod::Transcode
    } else {
        PlaybackMethod::DirectPlay
    }
}

#[derive(Deserialize)]
struct PlexSectionsResponse {
    #[serde(rename = "MediaContainer")]
    media_container: PlexMediaContainer,
}

#[derive(Deserialize)]
struct PlexMediaContainer {
    #[serde(rename = "Directory", default)]
    directory: Vec<PlexSection>,
}

#[derive(Deserialize, Clone)]
struct PlexSection {
    key: String,
    #[serde(rename = "Location", default)]
    locations: Vec<PlexLocation>,
}

#[derive(Deserialize, Clone)]
struct PlexLocation {
    path: String,
}

#[derive(Deserialize)]
struct PlexSessionsResponse {
    #[serde(rename = "MediaContainer")]
    media_container: PlexSessionsContainer,
}

#[derive(Deserialize)]
struct PlexSessionsContainer {
    #[serde(rename = "Metadata", default)]
    metadata: Vec<PlexSessionMetadata>,
}

#[derive(Deserialize)]
struct PlexSessionMetadata {
    #[serde(rename = "type")]
    item_type: Option<String>,
    #[serde(rename = "title")]
    title: Option<String>,
    #[serde(rename = "grandparentTitle")]
    grandparent_title: Option<String>,
    #[serde(rename = "duration")]
    duration: Option<i64>,
    #[serde(rename = "parentIndex")]
    parent_index: Option<i32>,
    #[serde(rename = "index")]
    index: Option<i32>,
    #[serde(rename = "thumb")]
    thumb: Option<String>,
    #[serde(rename = "ViewOffset")]
    view_offset: Option<i64>,
    #[serde(rename = "TranscodeSession")]
    transcode_session: Option<serde_json::Value>,
    #[serde(rename = "Player")]
    player: Option<PlexPlayer>,
    #[serde(rename = "User")]
    user: Option<PlexUser>,
}

#[derive(Deserialize)]
struct PlexPlayer {
    #[serde(rename = "title")]
    title: Option<String>,
    #[serde(rename = "product")]
    product: Option<String>,
    #[serde(rename = "state")]
    state: Option<String>,
}

#[derive(Deserialize)]
struct PlexUser {
    #[serde(rename = "title")]
    title: Option<String>,
}

mod urlencoding {
    pub fn encode(s: &str) -> String {
        let mut result = String::new();
        for c in s.chars() {
            match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' | '/' => result.push(c),
                _ => {
                    for b in c.to_string().as_bytes() {
                        result.push_str(&format!("%{b:02X}"));
                    }
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests;
