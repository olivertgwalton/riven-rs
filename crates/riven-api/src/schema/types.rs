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

/// One configurable settings surface — either the instance-wide "general"
/// settings or a single plugin. The frontend renders `schema` + `values`
/// generically; the plugin-only fields are null for the general section.
#[derive(SimpleObject)]
pub struct SettingsSection {
    pub id: String,
    pub title: String,
    /// "general" | "plugin".
    pub kind: String,
    /// JSON array of SettingField descriptors for rendering the form.
    pub schema: serde_json::Value,
    /// Typed values object keyed by field key.
    pub values: serde_json::Value,
    /// Setup grouping key (plugins only; see `setupGroups`).
    pub category: Option<String>,
    pub enabled: Option<bool>,
    pub valid: Option<bool>,
    pub configured: Option<bool>,
    pub missing_required_fields: Vec<String>,
    pub version: Option<String>,
}

#[derive(SimpleObject)]
pub struct InstanceStatus {
    pub setup_completed: bool,
    /// Whether the minimum viable configuration exists to finish setup.
    pub ready_to_complete: bool,
    pub enabled_valid_plugin_count: i32,
    pub enabled_profile_count: i32,
    /// Human-readable reasons setup can't be completed yet (empty when ready).
    pub blockers: Vec<String>,
}

/// An ordered setup section that plugins are grouped under (by `PluginInfo.category`).
#[derive(SimpleObject)]
pub struct SetupGroup {
    pub id: String,
    pub title: String,
    pub description: String,
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
