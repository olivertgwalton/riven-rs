//! Top-level event orchestrator.
//!
//! All event-driven control flow funnels through [`MainOrchestrator::on_event`].
//! That's the single transition table — adding a new event-driven behaviour
//! means adding a match arm here, not scattering side-effects across
//! `application/`. Events are the only public input.
//!
//! Request persistence and state synchronization are queue-independent
//! lifecycle functions. `LibraryOrchestrator` retains only the follow-up work
//! that actually needs a queue, such as enqueueing and retrying requests.

use std::sync::Arc;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures::stream::{self, StreamExt};
use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;
use tokio::sync::broadcast;

use crate::application::process_media_item::{fan_out_to_children, push_requested_seasons};
use crate::context;
use crate::lifecycle::{LibraryOrchestrator, sync_item_request_state};
use crate::{IndexJob, JobQueue, ProcessMediaItemJob, ProcessStep};

/// Owns the queue and dispatches events to typed actor calls.
pub struct MainOrchestrator {
    queue: Arc<JobQueue>,
}

/// Start the event listener that feeds [`RivenEvent`]s into the main
/// orchestrator. The queue layer owns this bridge because event transitions
/// are application behavior, independent of whichever API starts the app.
pub fn start_event_controller(queue: Arc<JobQueue>) {
    let mut events = queue.event_tx.subscribe();
    let orchestrator = MainOrchestrator::new(queue);
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => orchestrator.on_event(&event).await,
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    tracing::warn!(
                        dropped,
                        "event controller lagged; events were dropped and items may wait for the next retry cycle"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
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
            RivenEvent::ItemRequestCreated {
                item_id,
                requested_seasons,
                ..
            } => {
                self.process_item_request(
                    *item_id,
                    repo::ItemRequestUpsertAction::Created,
                    requested_seasons.as_deref(),
                )
                .await;
            }
            RivenEvent::ItemRequestUpdated {
                item_id,
                requested_seasons,
                ..
            } => {
                self.process_item_request(
                    *item_id,
                    repo::ItemRequestUpsertAction::Updated,
                    requested_seasons.as_deref(),
                )
                .await;
            }

            RivenEvent::MediaItemIndexSuccess { id, .. } => {
                self.handle_index_success(*id).await;
            }

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
            RivenEvent::MediaItemScrapeErrorNoNewStreams { id, .. } => {
                if let Some(item) = self.load_item(*id).await {
                    fan_out_to_children(&item, &self.queue).await;
                }
            }

            RivenEvent::MediaItemDownloadSuccess { id, .. } => {
                self.queue
                    .push_process_media_item(
                        ProcessMediaItemJob::new(*id).at_step(ProcessStep::Validate),
                    )
                    .await;
            }
            RivenEvent::MediaItemDownloadPartialSuccess { id }
            | RivenEvent::MediaItemDownloadError { id, .. } => {
                self.queue
                    .push_process_media_item(
                        ProcessMediaItemJob::new(*id).at_step(ProcessStep::Validate),
                    )
                    .await;
                if let Some(item) = self.load_item(*id).await {
                    fan_out_to_children(&item, &self.queue).await;
                }
            }

            _ => {}
        }
    }

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
        sync_item_request_state(&item).await;

        let needs_reindex = match item.item_type {
            MediaItemType::Show => {
                item.show_status == Some(ShowStatus::Continuing)
                    || repo::get_next_unreleased_air_date_for_show(item.id)
                        .await
                        .ok()
                        .flatten()
                        .is_some()
            }
            _ => item.state == MediaItemState::Unreleased,
        };
        if needs_reindex {
            self.schedule_reindex(&item).await;
        } else {
            self.queue.clear_scheduled_index(item.id).await;
        }
        if item.state != MediaItemState::Unreleased && item.is_requested {
            self.process_media_item(&item).await;
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
                push_requested_seasons(item.id, &self.queue).await;
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
                match repo::get_next_unreleased_air_date_for_show(item.id).await {
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
        match repo::get_ongoing_container_ids().await {
            Ok(ids) => {
                if let Err(error) = repo::force_recompute(&ids).await {
                    tracing::error!(
                        %error,
                        items = ids.len(),
                        "library sweep: could not refresh the state of ongoing shows/seasons; their state may be stale until the next sweep"
                    );
                }
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    "library sweep: could not list ongoing shows/seasons, skipping their state refresh this pass"
                );
            }
        }

        let requests = match repo::get_retryable_item_requests().await {
            Ok(r) => r,
            Err(error) => {
                tracing::error!(
                    %error,
                    "library sweep: could not list requests that need retrying, skipping them this pass"
                );
                vec![]
            }
        };
        let request_count = requests.len();
        let lib = LibraryOrchestrator::new(&self.queue);
        stream::iter(requests)
            .for_each_concurrent(32, |request| {
                let lib = &lib;
                async move {
                    lib.retry_item_request(&request).await;
                }
            })
            .await;

        // Movie/Show cover the common case (a stuck show fans out to its
        // seasons/episodes on reprocess). Season/Episode are retried directly
        // too: a leaf item can be actionable while its parent's rolled-up
        // state hasn't caught up (e.g. an ongoing anime season), which would
        // otherwise orphan it forever since it never surfaces via the
        // Movie/Show sweep.
        let mut retried = 0usize;
        for item_type in [
            MediaItemType::Movie,
            MediaItemType::Show,
            MediaItemType::Season,
            MediaItemType::Episode,
        ] {
            let items = match repo::get_pending_items_for_retry(item_type).await {
                Ok(items) => items,
                Err(error) => {
                    tracing::error!(
                        %error,
                        item_type = ?item_type,
                        "library sweep: could not list items that need retrying, skipping this type this pass"
                    );
                    vec![]
                }
            };
            retried += items.len();
            for item in items {
                self.process_media_item(&item).await;
            }
        }

        if request_count > 0 || retried > 0 {
            tracing::info!(
                requests = request_count,
                items = retried,
                "library sweep: re-queued incomplete items whose retry cooldown has expired"
            );
        }
    }

    pub async fn transition_newly_aired(&self) {
        let ids = match repo::transition_unreleased_aired().await {
            Ok(ids) => ids,
            Err(error) => {
                tracing::error!(
                    %error,
                    "air-date check: could not move newly aired items out of Unreleased; they stay unreleased until the next check"
                );
                return;
            }
        };
        if ids.is_empty() {
            return;
        }
        tracing::info!(
            count = ids.len(),
            "air-date check: items have now aired and are queued for scraping"
        );
        for id in ids {
            if let Some(item) = self.load_item(id).await {
                self.process_media_item(&item).await;
            }
        }
    }

    async fn load_item(&self, id: i64) -> Option<MediaItem> {
        context::load_media_item_or_log(id, "main_orchestrator").await
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
