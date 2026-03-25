use async_graphql::SimpleObject;
use riven_db::entities::*;

/// Episode with its primary filesystem entry (media file only).
#[derive(SimpleObject)]
pub struct EpisodeFull {
    #[graphql(flatten)]
    pub item: MediaItem,
    pub filesystem_entry: Option<FileSystemEntry>,
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
    pub seasons: Vec<SeasonFull>,
}

#[derive(SimpleObject)]
pub struct ItemsPage {
    pub items: Vec<MediaItem>,
    pub page: i64,
    pub limit: i64,
    pub total_items: i64,
    pub total_pages: i64,
}

#[derive(SimpleObject)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub valid: bool,
    /// JSON array of SettingField descriptors for rendering the settings form.
    pub schema: serde_json::Value,
}
