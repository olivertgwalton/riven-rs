use super::*;

pub(crate) async fn get_library_sections(
    http: &riven_core::http::HttpClient,
    plex_url: &str,
    token: &str,
) -> anyhow::Result<Vec<PlexSection>> {
    let url = format!("{plex_url}/library/sections");
    tracing::debug!(target_url = %url, "fetching plex library sections");
    let resp: PlexSectionsResponse = http
        .get_json(PROFILE, url.clone(), |client| {
            client
                .get(&url)
                .header("x-plex-token", token)
                .header("accept", "application/json")
        })
        .await?;

    Ok(resp.media_container.directory)
}

pub(crate) async fn refresh_section(
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
        .send(PROFILE, |client| {
            client
                .post(&url)
                .header("x-plex-token", token)
                .header("accept", "application/json")
        })
        .await?;
    response.error_for_status()?;
    Ok(())
}

pub(crate) async fn get_active_sessions(
    http: &riven_core::http::HttpClient,
    plex_url: &str,
    token: &str,
) -> anyhow::Result<Vec<ActivePlaybackSession>> {
    let url = format!("{plex_url}/status/sessions");
    tracing::debug!(target_url = %url, "fetching plex active sessions");
    let resp: PlexSessionsResponse = http
        .get_json(PROFILE, url.clone(), |client| {
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

pub(crate) fn map_playback_state(state: Option<&str>) -> PlaybackState {
    match state.unwrap_or_default().to_ascii_lowercase().as_str() {
        "playing" => PlaybackState::Playing,
        "paused" => PlaybackState::Paused,
        "buffering" => PlaybackState::Buffering,
        "stopped" => PlaybackState::Idle,
        _ => PlaybackState::Unknown,
    }
}

pub(crate) fn map_playback_method(is_transcoding: bool) -> PlaybackMethod {
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
pub(crate) struct PlexSection {
    pub(crate) key: String,
    #[serde(rename = "Location", default)]
    pub(crate) locations: Vec<PlexLocation>,
}

#[derive(Deserialize, Clone)]
pub(crate) struct PlexLocation {
    pub(crate) path: String,
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

pub(crate) mod urlencoding {
    pub(crate) fn encode(s: &str) -> String {
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
