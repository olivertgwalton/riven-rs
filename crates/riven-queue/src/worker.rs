use std::sync::Arc;
use std::time::Duration;

use riven_core::types::*;
use riven_db::repo;

use crate::orchestrator::LibraryOrchestrator;
use crate::{IndexJob, JobQueue};

/// Periodic scheduler.
pub struct Scheduler {
    db_pool: sqlx::PgPool,
    job_queue: Arc<JobQueue>,
}

impl Scheduler {
    pub fn new(db_pool: sqlx::PgPool, job_queue: Arc<JobQueue>) -> Self {
        Self { db_pool, job_queue }
    }

    pub async fn run(self) {
        let mut content_tick = tokio::time::interval(Duration::from_secs(120));
        let mut retry_tick = tokio::time::interval(Duration::from_secs(60));
        let mut cleanup_tick = tokio::time::interval(Duration::from_secs(60 * 60));

        loop {
            tokio::select! {
                _ = content_tick.tick()    => self.job_queue.push_content_service().await,
                _ = retry_tick.tick()      => self.retry_library().await,
                _ = cleanup_tick.tick()    => self.cleanup_runtime_state().await,
            }
        }
    }

    async fn cleanup_runtime_state(&self) {
        match repo::delete_stale_flow_artifacts(&self.db_pool, 6).await {
            Ok(count) if count > 0 => {
                tracing::info!(count, "deleted stale flow artifacts");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(%error, "failed to delete stale flow artifacts");
            }
        }

        let mut redis = self.job_queue.redis.clone();
        crate::prune_queue_history(&mut redis).await;
    }

    /// Retry pending top-level items.
    async fn retry_library(&self) {
        let orchestrator = LibraryOrchestrator::new(&self.job_queue);

        let requests = match repo::get_retryable_item_requests(&self.db_pool, 50).await {
            Ok(requests) => requests,
            Err(error) => {
                tracing::error!(%error, "failed to fetch retryable item requests");
                vec![]
            }
        };

        for request in requests {
            orchestrator.retry_item_request(&request).await;
        }

        for item_type in [MediaItemType::Movie, MediaItemType::Show] {
            let items = match repo::get_pending_items_for_retry(&self.db_pool, item_type, 50).await
            {
                Ok(items) => items,
                Err(e) => {
                    tracing::error!(error = %e, "failed to fetch pending items for retry");
                    vec![]
                }
            };

            for item in items {
                match item.state {
                    MediaItemState::Indexed if item.indexed_at.is_none() => {
                        self.job_queue.push_index(IndexJob::from_item(&item)).await;
                    }
                    MediaItemState::Indexed | MediaItemState::PartiallyCompleted => {
                        orchestrator.queue_scrape_for_item(&item, None).await;
                    }
                    MediaItemState::Scraped => {
                        orchestrator.queue_download_for_item(&item).await;
                    }
                    _ => {}
                }
            }
        }

        self.retry_ongoing().await;
    }

    /// Retry items stuck in Ongoing (partially completed with some unreleased episodes).
    async fn retry_ongoing(&self) {
        let orchestrator = LibraryOrchestrator::new(&self.job_queue);

        for item_type in [MediaItemType::Movie, MediaItemType::Season] {
            let items = match repo::get_stuck_ongoing_items(&self.db_pool, item_type, 10, 20).await
            {
                Ok(items) => items,
                Err(e) => {
                    tracing::error!(error = %e, "failed to fetch stuck ongoing items");
                    vec![]
                }
            };
            for item in &items {
                orchestrator.queue_download_for_item(item).await;
            }
        }
    }
}
