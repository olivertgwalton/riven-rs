//! Top-level event orchestrator.
//!
//! All event-driven control flow funnels through [`MainOrchestrator::on_event`].
//! That's the single transition table — adding a new event-driven behaviour
//! means adding a match arm here, not scattering side-effects across
//! `application/`. Events are the only public input.
//!
//! The `LibraryOrchestrator` is still used for *lifecycle* operations (upsert
//! a request, sync request state, retry an item request) — those aren't
//! event-driven and don't belong here.

use std::sync::Arc;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures::stream::{self, StreamExt};
use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::context;
use crate::orchestrator::LibraryOrchestrator;
use crate::{IndexJob, JobQueue, ProcessMediaItemJob, ProcessStep};

/// Owns the queue and dispatches events to typed actor calls.
pub struct MainOrchestrator {
    queue: Arc<JobQueue>,
}

impl MainOrchestrator {
    pub fn new(queue: Arc<JobQueue>) -> Self {
        Self { queue }
    }

    /// Single transition table for all RivenEvent-driven orchestration.
    /// Every event we act on maps to one actor call here. Events we don't
    /// act on (logging-only events, plugin-internal events) fall
    /// through the wildcard.
    pub async fn on_event(&self, event: &RivenEvent) {
        match event {
            // ── Item-request lifecycle ───────────────────────────────────────
            RivenEvent::ItemRequestCreated {
                item_id,
                requested_seasons,
                ..
            } => {
                self.process_item_request(*item_id, repo::ItemRequestUpsertAction::Created,
                    requested_seasons.as_deref()).await;
            }
            RivenEvent::ItemRequestUpdated {
                item_id,
                requested_seasons,
                ..
            } => {
                self.process_item_request(*item_id, repo::ItemRequestUpsertAction::Updated,
                    requested_seasons.as_deref()).await;
            }

            // ── Index ────────────────────────────────────────────────────────
            RivenEvent::MediaItemIndexSuccess { id, .. } => {
                self.handle_index_success(*id).await;
            }

            // ── Scrape ───────────────────────────────────────────────────────
            //
            // On scrape success, advance the per-item state machine to the
            // Download step. We only do this for `is_requested` items; one-off
            // discovery scrapes (UI flow) hit this event with is_requested=false
            // and should stop here.
            RivenEvent::MediaItemScrapeSuccess { id, .. } => {
                if let Some(item) = self.load_item(*id).await
                    && item.is_requested
                {
                    self.queue
                        .push_process_media_item(
                            ProcessMediaItemJob::new(*id).at_step(ProcessStep::Download),
                        )
                        .await;
                }
            }
            // Scrape produced no new streams. `record_scrape_failure` already
            // ran inside scrape `parse_results`, bumping `failed_attempts` and
            // recomputing state (which may flip to Failed). No further action
            // here — the next `retry_library` cycle picks the show up after
            // its `failed_attempts` cooldown elapses.
            RivenEvent::MediaItemScrapeErrorNoNewStreams { .. } => {}

            // ── Download ─────────────────────────────────────────────────────
            //
            // Download finished (success, partial, or error) — advance to
            // Validate, which centralises the post-download decision: emit
            // completion, fan out to incomplete children, or schedule a
            // re-scrape +30 min.
            RivenEvent::MediaItemDownloadSuccess { id, .. }
            | RivenEvent::MediaItemDownloadPartialSuccess { id }
            | RivenEvent::MediaItemDownloadError { id, .. } => {
                self.queue
                    .push_process_media_item(
                        ProcessMediaItemJob::new(*id).at_step(ProcessStep::Validate),
                    )
                    .await;
            }

            _ => {}
        }
    }

    // ── Actors ────────────────────────────────────────────────────────────────

    /// `processItemRequest` actor. Runs the index → process pipeline for a
    /// freshly-created or -updated item request.
    async fn process_item_request(
        &self,
        item_id: i64,
        action: repo::ItemRequestUpsertAction,
        requested_seasons: Option<&[i32]>,
    ) {
        let Some(item) = self.load_item(item_id).await else {
            return;
        };
        LibraryOrchestrator::new(&self.queue)
            .enqueue_after_request_action(&item, action, requested_seasons)
            .await;
    }

    /// `media-item.index.success` handler. Routes by release status:
    ///   - `Unreleased` → schedule reindex
    ///   - `Ongoing` → schedule reindex *and* start processing
    ///   - else → start processing
    async fn handle_index_success(&self, id: i64) {
        let Some(item) = self.load_item(id).await else {
            return;
        };
        // Sync request state first so any state transitions on the item
        // request itself land before we start enqueueing further work.
        LibraryOrchestrator::new(&self.queue)
            .sync_item_request_state(&item)
            .await;

        match item.state {
            MediaItemState::Unreleased => {
                self.schedule_reindex(&item).await;
            }
            MediaItemState::Ongoing => {
                self.schedule_reindex(&item).await;
                if item.is_requested {
                    self.process_media_item(&item).await;
                }
            }
            _ => {
                self.queue.clear_scheduled_index(item.id).await;
                if item.is_requested {
                    self.process_media_item(&item).await;
                }
            }
        }
    }

    /// Process-media-item actor. A Show fans out to its requested seasons; a
    /// Movie/Season/Episode is processed directly.
    async fn process_media_item(&self, item: &MediaItem) {
        if matches!(
            item.state,
            MediaItemState::Failed | MediaItemState::Paused | MediaItemState::Completed
        ) {
            return;
        }
        match item.item_type {
            MediaItemType::Show => {
                let seasons = repo::get_all_requested_seasons_for_show(&self.queue.db_pool, item.id)
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
                    self.queue
                        .push_process_media_item(ProcessMediaItemJob::new(season.id))
                        .await;
                }
            }
            _ => {
                self.queue
                    .push_process_media_item(ProcessMediaItemJob::new(item.id))
                    .await;
            }
        }
    }

    /// `scheduleReindex` actor. Computes next reindex time from item air-date
    /// metadata + reindex config and pushes a delayed IndexJob.
    async fn schedule_reindex(&self, item: &MediaItem) {
        let run_at = self.next_reindex_at(item).await;
        self.queue
            .schedule_index_at(IndexJob::from_item(item), run_at)
            .await;
    }

    async fn next_reindex_at(&self, item: &MediaItem) -> DateTime<Utc> {
        let config = self.queue.reindex_config.read().await.clone();
        let offset_minutes = config.schedule_offset_minutes.min(i64::MAX as u64) as i64;
        let fallback_days = config.unknown_air_date_offset_days.min(i64::MAX as u64) as i64;

        let target_date = match item.item_type {
            MediaItemType::Show => {
                match repo::get_next_unreleased_air_date_for_show(&self.queue.db_pool, item.id)
                    .await
                {
                    Ok(Some(date)) => Some(date),
                    Ok(None) | Err(_) => {
                        if item.state == MediaItemState::Unreleased {
                            item.aired_at
                        } else {
                            None
                        }
                    }
                }
            }
            _ => item.aired_at,
        };

        schedule_datetime(target_date, offset_minutes, fallback_days)
    }

    /// Retry-library actor. Periodically called by the worker to nudge items
    /// stuck in retryable states.
    pub async fn retry_library(&self) {
        // Item requests in `failed`/`requested` → re-trigger index.
        let requests = match repo::get_retryable_item_requests(&self.queue.db_pool).await {
            Ok(r) => r,
            Err(error) => {
                tracing::error!(%error, "retry_library: failed to fetch item requests");
                vec![]
            }
        };
        let lib = LibraryOrchestrator::new(&self.queue);
        stream::iter(requests)
            .for_each_concurrent(32, |request| {
                let lib = &lib;
                async move {
                    lib.retry_item_request(&request).await;
                }
            })
            .await;

        // Media items in retryable states → push ProcessMediaItem. Movies and
        // shows only — children are reached via Show fan-out inside
        // ProcessMediaItem.
        for item_type in [MediaItemType::Movie, MediaItemType::Show] {
            let items =
                match repo::get_pending_items_for_retry(&self.queue.db_pool, item_type).await {
                    Ok(items) => items,
                    Err(error) => {
                        tracing::error!(%error, "retry_library: failed to fetch pending items");
                        vec![]
                    }
                };
            for item in items {
                self.process_media_item(&item).await;
            }
        }
    }

    async fn load_item(&self, id: i64) -> Option<MediaItem> {
        context::load_media_item_or_log(&self.queue.db_pool, id, "main_orchestrator").await
    }
}

fn schedule_datetime(
    target_date: Option<NaiveDate>,
    offset_minutes: i64,
    fallback_days: i64,
) -> DateTime<Utc> {
    match target_date {
        Some(date) => {
            let midnight = date
                .and_hms_opt(0, 0, 0)
                .expect("midnight should always be valid");
            DateTime::<Utc>::from_naive_utc_and_offset(midnight, Utc)
                + Duration::minutes(offset_minutes)
        }
        None => Utc::now() + Duration::days(fallback_days),
    }
}
