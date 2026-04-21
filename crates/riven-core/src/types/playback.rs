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
    pub image_url: Option<String>,
}
