use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashSet;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::http::profiles;
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::{ActivePlaybackSession, PlaybackMethod, PlaybackState};
use riven_db::repo;

#[derive(Default)]
pub struct PlexPlugin;

register_plugin!(PlexPlugin);

#[async_trait]
impl Plugin for PlexPlugin {
    fn name(&self) -> &'static str {
        "plex"
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

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::MediaItemDownloadSuccess { id, .. } => {
                let plex_token = ctx.require_setting("plextoken")?;
                let plex_url = ctx.require_setting("plexserverurl")?.trim_end_matches('/');
                let library_path = ctx.settings.get_or("plexlibrarypath", "/mount");

                let entries = repo::get_media_entries(&ctx.db_pool, *id).await?;
                if entries.is_empty() {
                    anyhow::bail!("no filesystem entries found for media item {id}");
                }

                let sections = get_library_sections(&ctx.http, plex_url, plex_token).await?;

                for entry in &entries {
                    let dir_path = entry
                        .path
                        .rsplit_once('/')
                        .map(|(dir, _)| dir)
                        .unwrap_or(&entry.path);

                    let full_path = format!("{library_path}{dir_path}");

                    for section in &sections {
                        for location in &section.locations {
                            if full_path.starts_with(&location.path) {
                                refresh_section(
                                    &ctx.http,
                                    plex_url,
                                    plex_token,
                                    &section.key,
                                    &full_path,
                                )
                                .await?;
                                tracing::info!(
                                    section = section.key,
                                    path = full_path,
                                    "plex library section refreshed"
                                );
                            }
                        }
                    }
                }

                Ok(HookResponse::Empty)
            }
            RivenEvent::MediaItemsDeleted { deleted_paths, .. } => {
                if deleted_paths.is_empty() {
                    return Ok(HookResponse::Empty);
                }

                let plex_token = ctx.require_setting("plextoken")?;
                let plex_url = ctx.require_setting("plexserverurl")?.trim_end_matches('/');
                let library_path = ctx.settings.get_or("plexlibrarypath", "/mount");
                let sections = get_library_sections(&ctx.http, plex_url, plex_token).await?;

                let dirs: HashSet<String> = deleted_paths
                    .iter()
                    .map(|path| {
                        path.rsplit_once('/')
                            .map(|(dir, _)| dir)
                            .unwrap_or(path.as_str())
                            .to_string()
                    })
                    .collect();

                for dir_path in dirs {
                    let full_path = format!("{library_path}{dir_path}");
                    for section in &sections {
                        for location in &section.locations {
                            if full_path.starts_with(&location.path) {
                                refresh_section(
                                    &ctx.http,
                                    plex_url,
                                    plex_token,
                                    &section.key,
                                    &full_path,
                                )
                                .await?;
                                tracing::info!(
                                    section = section.key,
                                    path = full_path,
                                    "plex library section refreshed after delete"
                                );
                            }
                        }
                    }
                }

                Ok(HookResponse::Empty)
            }
            RivenEvent::ActivePlaybackSessionsRequested => {
                let plex_token = ctx.require_setting("plextoken")?;
                let plex_url = ctx.require_setting("plexserverurl")?.trim_end_matches('/');
                let sessions = get_active_sessions(&ctx.http, plex_url, plex_token).await?;
                Ok(HookResponse::ActivePlaybackSessions(sessions))
            }
            _ => Ok(HookResponse::Empty),
        }
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
    http.send(profiles::PLEX, |client| {
        client
            .post(&url)
            .header("x-plex-token", token)
            .header("accept", "application/json")
    })
    .await?;
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
                position_seconds: view_offset.map(|value| value / 1000),
                duration_seconds: duration.map(|value| value / 1000),
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

#[derive(Deserialize)]
struct PlexSection {
    key: String,
    #[serde(rename = "Location", default)]
    locations: Vec<PlexLocation>,
}

#[derive(Deserialize)]
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
