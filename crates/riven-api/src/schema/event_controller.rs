use std::sync::Arc;

use riven_core::events::RivenEvent;
use riven_db::repo;
use riven_queue::JobQueue;
use riven_queue::context;
use riven_queue::orchestrator::LibraryOrchestrator;
use tokio::sync::broadcast;

pub fn start(job_queue: Arc<JobQueue>) {
    let mut rx = job_queue.event_tx.subscribe();
    tokio::spawn(async move {
        loop {
            let event = match rx.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            };

            react_to_event(job_queue.as_ref(), &event).await;
        }
    });
}

async fn react_to_event(job_queue: &JobQueue, event: &RivenEvent) {
    let orchestrator = LibraryOrchestrator::new(job_queue);

    match event {
        RivenEvent::ItemRequestCreated {
            item_id,
            requested_seasons,
            ..
        } => {
            let Some(item) = repo::get_media_item(&job_queue.db_pool, *item_id)
                .await
                .ok()
                .flatten()
            else {
                return;
            };

            orchestrator
                .enqueue_after_request_action(
                    &item,
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
            let Some(item) = repo::get_media_item(&job_queue.db_pool, *item_id)
                .await
                .ok()
                .flatten()
            else {
                return;
            };

            orchestrator
                .enqueue_after_request_action(
                    &item,
                    repo::ItemRequestUpsertAction::Updated,
                    requested_seasons.as_deref(),
                )
                .await;
        }
        RivenEvent::MediaItemIndexSuccess { id, .. } => {
            let Some(item) = repo::get_media_item(&job_queue.db_pool, *id)
                .await
                .ok()
                .flatten()
            else {
                return;
            };

            let requested_seasons =
                context::load_requested_seasons(&job_queue.db_pool, &item).await;
            orchestrator
                .enqueue_after_index(&item, requested_seasons.as_deref())
                .await;
        }
        RivenEvent::MediaItemScrapeSuccess { id, .. } => {
            let Some(item) = repo::get_media_item(&job_queue.db_pool, *id)
                .await
                .ok()
                .flatten()
            else {
                return;
            };

            if item.is_requested {
                orchestrator.queue_download_for_item(&item).await;
            }
        }
        RivenEvent::MediaItemScrapeErrorNoNewStreams { id, .. }
        | RivenEvent::MediaItemDownloadPartialSuccess { id }
        | RivenEvent::MediaItemDownloadError { id, .. } => {
            orchestrator.fan_out_download_failure(*id).await;
        }
        _ => {}
    }
}
