use serde::{Deserialize, Serialize};

use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContentServiceJob;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexJob {
    pub id: i64,
    pub item_type: MediaItemType,
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
}

impl IndexJob {
    pub fn from_item(item: &MediaItem) -> Self {
        Self {
            id: item.id,
            item_type: item.item_type,
            imdb_id: item.imdb_id.clone(),
            tvdb_id: item.tvdb_id.clone(),
            tmdb_id: item.tmdb_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapeJob {
    pub id: i64,
    pub item_type: MediaItemType,
    pub imdb_id: Option<String>,
    pub title: String,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    #[serde(default = "default_true")]
    pub auto_download: bool,
    /// Number of times this job has been re-pushed because every scraper
    /// plugin was temporarily deferred. Incremented in `finalize` before re-pushing;
    /// existing jobs in Redis deserialise to 0 via the `default`.
    #[serde(default)]
    pub rate_limit_retries: u32,
}

impl ScrapeJob {
    pub fn for_movie(item: &MediaItem) -> Self {
        Self {
            id: item.id,
            item_type: item.item_type,
            imdb_id: item.imdb_id.clone(),
            title: item.title.clone(),
            season: None,
            episode: None,
            auto_download: true,
            rate_limit_retries: 0,
        }
    }

    pub fn for_season(
        season: &MediaItem,
        show_title: String,
        show_imdb_id: Option<String>,
    ) -> Self {
        Self {
            id: season.id,
            item_type: season.item_type,
            imdb_id: show_imdb_id,
            title: show_title,
            season: season.season_number,
            episode: None,
            auto_download: true,
            rate_limit_retries: 0,
        }
    }

    pub fn for_episode(ep: &MediaItem, show_title: String, show_imdb_id: Option<String>) -> Self {
        Self {
            id: ep.id,
            item_type: ep.item_type,
            imdb_id: show_imdb_id,
            title: show_title,
            season: ep.season_number,
            episode: ep.episode_number,
            auto_download: true,
            rate_limit_retries: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadJob {
    pub id: i64,
    pub info_hash: String,
    pub magnet: String,
    #[serde(default)]
    pub preferred_info_hash: Option<String>,
}

/// First step of the download flow — the riven-ts `rank-streams` grandchild job.
/// Loads streams, runs the cache check, builds cached candidates, and hands the
/// ranked result to `DownloadJob` via Redis state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankStreamsJob {
    pub id: i64,
    #[serde(default)]
    pub preferred_info_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseScrapeResultsJob {
    pub id: i64,
    #[serde(default = "default_true")]
    pub auto_download: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexPluginJob {
    pub id: i64,
    pub plugin_name: String,
    pub item_type: MediaItemType,
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapePluginJob {
    pub id: i64,
    pub plugin_name: String,
    pub item_type: MediaItemType,
    pub imdb_id: Option<String>,
    pub title: String,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    #[serde(default = "default_true")]
    pub auto_download: bool,
    /// Carried from the parent `ScrapeJob` so `finalize` can reconstruct it.
    #[serde(default)]
    pub rate_limit_retries: u32,
}
