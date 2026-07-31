use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[serde(rename_all = "lowercase")]
pub enum PlaybackState {
    Playing,
    Paused,
    Buffering,
    Idle,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackMethod {
    DirectPlay,
    DirectStream,
    Transcode,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ActivePlaybackSession {
    pub server: String,
    pub user_name: Option<String>,
    pub parent_title: Option<String>,
    pub item_title: String,
    pub item_type: Option<String>,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub playback_state: PlaybackState,
    pub playback_method: PlaybackMethod,
    pub position_seconds: Option<u64>,
    pub duration_seconds: Option<u64>,
    pub device_name: Option<String>,
    pub client_name: Option<String>,
    /// Artwork for the item, as a path on this riven instance — fetch it from
    /// here, not from the media server.
    //
    // Not a media-server URL, and deliberately so. Plugins used to build an
    // absolute one with the server's credential in the query string
    // (`?X-Plex-Token=…`, `?api_key=…`), which put an admin credential for
    // Plex/Emby/Jellyfin into a response any authenticated user can request, and
    // from there into the DOM and the browser's history. It is now
    // `artwork_path`, which riven resolves server-side.
    pub image_url: Option<String>,
}

/// The riven path that proxies one artwork image.
///
/// `server` selects the plugin that will be asked to fetch it; `reference` is
/// that plugin's own handle for the image and is opaque to everyone else. Kept
/// here so the plugins that mint these paths and the route that parses them
/// cannot drift apart.
pub fn artwork_path(server: &str, reference: &str) -> String {
    format!(
        "/artwork/{}?ref={}",
        url::form_urlencoded::byte_serialize(server.as_bytes()).collect::<String>(),
        url::form_urlencoded::byte_serialize(reference.as_bytes()).collect::<String>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Plex references are paths, so the slashes have to survive into the query
    /// string encoded — an unencoded one would be read as part of the route.
    #[test]
    fn a_reference_containing_slashes_is_encoded_into_the_query() {
        assert_eq!(
            artwork_path("plex", "/library/metadata/17/thumb/1700000000"),
            "/artwork/plex?ref=%2Flibrary%2Fmetadata%2F17%2Fthumb%2F1700000000"
        );
    }

    #[test]
    fn an_emby_item_id_needs_no_escaping() {
        assert_eq!(
            artwork_path("jellyfin", "a1b2c3"),
            "/artwork/jellyfin?ref=a1b2c3"
        );
    }
}
