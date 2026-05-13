//! Per-item state-machine processor.
//!
//! Each item in the library moves through a small state machine:
//!
//! ```text
//!     Scrape ──(success)──► Download ──(success)──► Validate ──► (Completed)
//!       ▲                       │                       │
//!       │                       └──(error)─►            │
//!       │                              push Validate    │
//!       └──────────────(reschedule +30 min)─────────────┘
//! ```
//!
//! Each `step` is its own apalis job execution; child flows (scrape /
//! rank-streams + download) re-push this job at the next step on completion.
//!
//! Single-owner model: no other code path may push scrape / download jobs
//! directly. The retry-library, fan-out, and event handlers all go through
//! `push_process_media_item`. This prevents multiple writers re-scheduling
//! the same scrape into the future indefinitely.

use chrono::{DateTime, Duration, Utc};
use riven_core::types::{MediaItemState, MediaItemType};
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::context::load_show_context;
use crate::{JobQueue, ProcessMediaItemJob, ProcessStep, ScrapeJob};

/// Worker entry point. Returns quickly: each step either fans out child jobs
/// or schedules a future-dated re-push of itself.
pub async fn run(job: &ProcessMediaItemJob, queue: &JobQueue) {
    let id = job.id;

    let Some(item) = repo::get_media_item(&queue.db_pool, id)
        .await
        .ok()
        .flatten()
    else {
        tracing::debug!(id, step = ?job.step, "process-media-item: item gone");
        return;
    };

    // Hard-stop on terminal/sticky states.
    if matches!(
        item.state,
        MediaItemState::Failed | MediaItemState::Paused
    ) {
        tracing::debug!(id, state = ?item.state, "process-media-item: terminal state, stopping");
        return;
    }

    // Already done — a prior step's writes flipped this to Completed. For
    // Show/Season this can happen when a season pack matched every requested
    // episode in one shot.
    if item.state == MediaItemState::Completed {
        log_completion(&item, job.started_at);
        return;
    }

    tracing::debug!(id, step = ?job.step, state = ?item.state, "process-media-item step");

    match job.step {
        ProcessStep::Scrape => handle_scrape(job, &item, queue).await,
        ProcessStep::Download => handle_download(job, &item, queue).await,
        ProcessStep::Validate => handle_validate(job, &item, queue).await,
    }
}

/// Step 1: enqueue scrape children. The scrape flow's finalize advances us
/// to `Download` on success or stops on no-new-streams (the per-item failure
/// counter handles backoff via `FAILED_ATTEMPTS_COOLDOWN_SQL`).
async fn handle_scrape(job: &ProcessMediaItemJob, item: &MediaItem, queue: &JobQueue) {
    if let Some(at) = job.next_scrape_attempt_at
        && at > Utc::now()
    {
        tracing::debug!(id = item.id, run_at = %at, "deferring scrape until backoff expires");
        queue.push_process_media_item_at(job.clone(), at).await;
        return;
    }

    match item.item_type {
        MediaItemType::Movie => {
            queue.push_scrape(ScrapeJob::for_movie(item)).await;
        }
        MediaItemType::Season => {
            let ctx = load_show_context(&queue.db_pool, item).await;
            queue
                .push_scrape(ScrapeJob::for_season(
                    item,
                    ctx.title,
                    ctx.imdb_id,
                    ctx.tvdb_id,
                ))
                .await;
        }
        MediaItemType::Episode => {
            let ctx = load_show_context(&queue.db_pool, item).await;
            queue
                .push_scrape(ScrapeJob::for_episode(
                    item,
                    ctx.title,
                    ctx.imdb_id,
                    ctx.tvdb_id,
                ))
                .await;
        }
        MediaItemType::Show => {
            // Shows fan out to their requested seasons (each becomes its own
            // ProcessMediaItem). The Show itself has no scrape job — its
            // state cascades from children.
            fan_out_to_children(item, queue).await;
        }
    }
}

/// Step 2: enqueue the download flow (rank-streams → find-valid-torrent →
/// persist). The persist step advances us to `Validate` on success/failure;
/// if no streams are available right now, jump straight to `Validate` so the
/// next-scrape backoff kicks in.
async fn handle_download(job: &ProcessMediaItemJob, item: &MediaItem, queue: &JobQueue) {
    if !queue.push_download_from_best_stream(item.id).await {
        tracing::debug!(id = item.id, "no streams available; advancing to Validate");
        queue
            .push_process_media_item(job.clone().at_step(ProcessStep::Validate))
            .await;
    }
}

/// Step 3: post-download triage. Decides between completion log, fan-out to
/// incomplete children, and re-scrape-after-failure.
async fn handle_validate(job: &ProcessMediaItemJob, item: &MediaItem, queue: &JobQueue) {
    // The download flow's filesystem_entries inserts already triggered a
    // recompute; just re-load the row to read the post-write state.
    let item = match repo::get_media_item(&queue.db_pool, item.id).await {
        Ok(Some(i)) => i,
        _ => return,
    };

    match item.state {
        MediaItemState::Completed => log_completion(&item, job.started_at),
        MediaItemState::PartiallyCompleted | MediaItemState::Ongoing => {
            // Show/Season got *some* of its children but not all — kick off
            // a fresh ProcessMediaItem per incomplete child.
            fan_out_to_children(&item, queue).await;
        }
        MediaItemState::Failed | MediaItemState::Paused => {
            // Sticky state — no further action.
        }
        MediaItemState::Scraped
        | MediaItemState::Indexed
        | MediaItemState::Unreleased => {
            // Download didn't make this item Completed — schedule a re-scrape
            // 30 minutes from now.
            let at = Utc::now() + Duration::minutes(30);
            tracing::debug!(
                id = item.id,
                run_at = %at,
                "scheduling re-scrape after download failure"
            );
            queue
                .push_process_media_item_at(
                    job.clone()
                        .at_step(ProcessStep::Scrape)
                        .with_next_scrape_attempt(at),
                    at,
                )
                .await;
        }
    }
}

/// Push a fresh `ProcessMediaItem` for each incomplete child of `parent`.
/// Used both at scrape-step entry (Show → seasons) and validate-step
/// (Season → episodes after a partial pack download).
async fn fan_out_to_children(parent: &MediaItem, queue: &JobQueue) {
    match parent.item_type {
        MediaItemType::Show => {
            let seasons = repo::get_all_requested_seasons_for_show(&queue.db_pool, parent.id)
                .await
                .unwrap_or_default();
            for season in seasons {
                if matches!(
                    season.state,
                    MediaItemState::Completed
                        | MediaItemState::Failed
                        | MediaItemState::Paused
                ) {
                    continue;
                }
                queue
                    .push_process_media_item(ProcessMediaItemJob::new(season.id))
                    .await;
            }
        }
        MediaItemType::Season => {
            let episodes = repo::get_incomplete_episodes_for_season(&queue.db_pool, parent.id)
                .await
                .unwrap_or_default();
            for ep in episodes {
                queue
                    .push_process_media_item(ProcessMediaItemJob::new(ep.id))
                    .await;
            }
        }
        MediaItemType::Movie | MediaItemType::Episode => {
            // Leaf: no children to fan out to.
        }
    }
}

fn log_completion(item: &MediaItem, started_at: DateTime<Utc>) {
    let elapsed = Utc::now().signed_duration_since(started_at);
    let secs = elapsed.num_seconds().max(0);
    tracing::info!(
        id = item.id,
        title = %item.title,
        elapsed_secs = secs,
        "process-media-item: completed"
    );
}
