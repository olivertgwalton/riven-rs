use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use riven_core::events::RivenEvent;
use riven_core::types::{MediaItemType, ScrapeResponse};
use riven_db::entities::MediaItem;

/// One per-plugin invocation of a hook event. For fan-in events the awaiting
/// orchestrator reads the handler's [`HookOutcome`] back through apalis's
/// task-result storage; broadcast events (notifications) ignore the result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginHookJob {
    pub plugin_name: String,
    pub event: RivenEvent,
}

/// What a plugin-hook child reports back to the orchestrator awaiting it.
///
/// This is the task's stored result, so it must be (de)serializable with the
/// queue's JSON codec. The orchestrator aggregates `Response` payloads,
/// counts `RateLimited` for backoff, and ignores the rest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookOutcome {
    /// The plugin answered; `None` means the response carried nothing worth
    /// aggregating (e.g. a scraper that found no streams).
    Response(Option<serde_json::Value>),
    /// The plugin deferred with a rate-limit/retry-later error.
    RateLimited,
    /// The plugin failed; the error was already logged in the hook worker.
    Failed,
    /// Nothing to report: broadcast events, stale children, missing plugins.
    Skipped,
}

/// Handler return type for plugin-hook workers. The outer `Result` layer is
/// required by apalis-redis's wire format: `RedisAck` stores the handler's
/// Ok-value JSON verbatim while `check_status` decodes it as
/// `Result<Res, String>` — so the Ok value must itself be that `Result`.
pub type HookAck = Result<HookOutcome, String>;

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
    #[serde(default)]
    pub tvdb_id: Option<String>,
    pub title: String,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    /// Number of times this job has been re-pushed because every scraper
    /// plugin was temporarily deferred. Incremented before re-pushing;
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
            tvdb_id: None,
            title: item.title.clone(),
            season: None,
            episode: None,
            rate_limit_retries: 0,
        }
    }

    /// Build a scrape job for a season or episode item. Season rows never
    /// carry an `episode_number` (the column is absent from `create_season`'s
    /// INSERT, so it's always NULL at the DB level), so reading both numbers
    /// straight off `item` covers both cases with no branch needed.
    pub fn for_episode_or_season(
        item: &MediaItem,
        show_title: String,
        show_imdb_id: Option<String>,
        show_tvdb_id: Option<String>,
    ) -> Self {
        Self {
            id: item.id,
            item_type: item.item_type,
            imdb_id: show_imdb_id,
            tvdb_id: show_tvdb_id,
            title: show_title,
            season: item.season_number,
            episode: item.episode_number,
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
    /// How many times this job has been requeued because a download plugin
    /// was rate-limited mid-walk. Drives the same escalating backoff the
    /// scrape flow uses; `serde(default)` keeps jobs serialized before the
    /// field existed deserializable.
    #[serde(default)]
    pub rate_limit_retries: u32,
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
    /// Per-scraper stream maps collected by the scrape orchestrator. Carried
    /// in the job payload (Redis task data) — the same bytes the old fan-in
    /// design parked in a flow-results hash between the two workers.
    /// `default` so a pre-upgrade job still queued in Redis decodes (to an
    /// empty run) instead of killing the parse worker's poll stream.
    #[serde(default)]
    pub responses: Vec<ScrapeResponse>,
}
