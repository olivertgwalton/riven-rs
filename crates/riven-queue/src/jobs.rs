use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use riven_core::events::RivenEvent;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;

/// One per-plugin invocation of a hook event. For fan-in events the
/// `scope` discriminator names the orchestrator's flow keys
/// (`riven:flow:<prefix>:<scope>:results` etc). For broadcast events
/// (notifications) `scope` is unused.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginHookJob {
    pub plugin_name: String,
    pub event: RivenEvent,
    /// Fan-in scope. Required for fan-in events; ignored for broadcast.
    /// For orchestrator-driven flows (scrape/index) this is the media item id.
    /// For caller-await flows (content, cache-check, etc.) the caller picks
    /// a unique value so concurrent calls don't share flow keys.
    #[serde(default)]
    pub scope: Option<i64>,
}

/// Per-item state-machine job.
///
/// Each step is a separate job execution; after enqueueing children (scrape /
/// rank-streams) the worker exits, and the child flow's finalize hook
/// re-pushes this job at the next step.
///
/// `next_scrape_attempt_at` is set by `Validate` after a download failure to
/// defer the next scrape by 30 minutes.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessStep {
    /// Trigger scrape children. If `next_scrape_attempt_at` is in the future,
    /// the job re-pushes itself at that time instead.
    Scrape,
    /// Trigger download children (rank-streams + find-valid-torrent + persist).
    Download,
    /// Inspect the post-download state. If still incomplete: schedule scrape
    /// +30 min. If Show/Season with incomplete children: fan out child jobs.
    /// If Completed: emit success.
    Validate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessMediaItemJob {
    pub id: i64,
    pub step: ProcessStep,
    /// Wall-clock to gate the next Scrape attempt. None means "scrape immediately".
    #[serde(default)]
    pub next_scrape_attempt_at: Option<DateTime<Utc>>,
    /// First-push timestamp; preserved across step re-pushes so the final
    /// "completed in Xh" log measures the real wall-clock cost.
    pub started_at: DateTime<Utc>,
}

impl ProcessMediaItemJob {
    pub fn new(id: i64) -> Self {
        Self {
            id,
            step: ProcessStep::Scrape,
            next_scrape_attempt_at: None,
            started_at: Utc::now(),
        }
    }

    pub fn at_step(mut self, step: ProcessStep) -> Self {
        self.step = step;
        self
    }

    pub fn with_next_scrape_attempt(mut self, at: DateTime<Utc>) -> Self {
        self.next_scrape_attempt_at = Some(at);
        self
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankStreamsJob {
    pub id: i64,
    #[serde(default)]
    pub preferred_info_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseScrapeResultsJob {
    pub id: i64,
}

