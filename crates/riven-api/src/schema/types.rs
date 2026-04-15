use async_graphql::SimpleObject;
use riven_core::types::{MediaItemState, MediaItemType};
use riven_db::entities::*;

/// Episode with its primary filesystem entry (media file only).
#[derive(SimpleObject)]
pub struct EpisodeFull {
    #[graphql(flatten)]
    pub item: MediaItem,
    pub filesystem_entry: Option<FileSystemEntry>,
    pub filesystem_entries: Vec<FileSystemEntry>,
}

/// Season with its episodes and their file info.
#[derive(SimpleObject)]
pub struct SeasonFull {
    #[graphql(flatten)]
    pub item: MediaItem,
    pub episodes: Vec<EpisodeFull>,
}

/// Media item (movie or show) with filesystem entry and, for shows, full season/episode tree.
#[derive(SimpleObject)]
pub struct MediaItemFull {
    #[graphql(flatten)]
    pub item: MediaItem,
    pub filesystem_entry: Option<FileSystemEntry>,
    pub filesystem_entries: Vec<FileSystemEntry>,
    pub seasons: Vec<SeasonFull>,
}

/// Lightweight episode state used for live state subscriptions.
#[derive(SimpleObject)]
pub struct EpisodeState {
    pub id: i64,
    pub episode_number: Option<i32>,
    pub state: MediaItemState,
}

/// Lightweight season state used for live state subscriptions.
#[derive(SimpleObject)]
pub struct SeasonState {
    pub id: i64,
    pub season_number: Option<i32>,
    pub state: MediaItemState,
    pub is_requested: bool,
    pub expected_file_count: i64,
    pub episodes: Vec<EpisodeState>,
}

/// Lightweight media state tree used for live state subscriptions.
#[derive(SimpleObject)]
pub struct MediaItemStateTree {
    pub id: i64,
    pub state: MediaItemState,
    pub imdb_id: Option<String>,
    pub tmdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub expected_file_count: i64,
    pub seasons: Vec<SeasonState>,
}

#[derive(SimpleObject)]
pub struct ItemsPage {
    pub items: Vec<MediaItemListRow>,
    pub page: i64,
    pub limit: i64,
    pub total_items: i64,
    pub total_pages: i64,
}

#[derive(SimpleObject)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub valid: bool,
    /// JSON array of SettingField descriptors for rendering the settings form.
    pub schema: serde_json::Value,
}

#[derive(SimpleObject)]
pub struct InstanceStatus {
    pub setup_completed: bool,
}

#[derive(SimpleObject)]
pub struct DiscoveredStream {
    pub key: String,
    pub title: String,
    pub info_hash: String,
    pub magnet: String,
    pub parsed_data: Option<serde_json::Value>,
    pub rank: Option<i64>,
    pub file_size_bytes: Option<i64>,
    pub is_cached: bool,
    pub item_type: MediaItemType,
    pub season_number: Option<i32>,
}
