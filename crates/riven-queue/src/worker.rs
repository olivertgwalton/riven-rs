use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use riven_core::types::*;
use riven_db::repo;

use crate::orchestrator::LibraryOrchestrator;
use crate::JobQueue;

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
        let mut cleanup_tick = tokio::time::interval(Duration::from_secs(60 * 60));
        // Check for stale workers every 60s (2× the apalis default keep-alive of 30s).
        // Rescued jobs will release their own dedup keys on completion, mirroring
        let mut worker_recovery_tick = tokio::time::interval(Duration::from_secs(60));
        let retry_wait =
            Self::retry_wait_duration(self.job_queue.retry_interval_secs.load(Ordering::SeqCst));
        let mut retry_sleep = std::pin::pin!(tokio::time::sleep(retry_wait));

        self.retry_library().await;

        loop {
            tokio::select! {
                _ = content_tick.tick()        => self.job_queue.push_content_service().await,
                _ = &mut retry_sleep           => {
                    self.retry_library().await;
                    let next_wait = Self::retry_wait_duration(
                        self.job_queue.retry_interval_secs.load(Ordering::SeqCst),
                    );
                    retry_sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + next_wait);
                }
                _ = cleanup_tick.tick()        => self.cleanup_runtime_state().await,
                _ = worker_recovery_tick.tick() => {
                    let mut redis = self.job_queue.redis.clone();
                    // 60s threshold: a worker missing two heartbeats is considered dead.
                    crate::recover_stale_workers(&mut redis, 60).await;
                }
            }
        }
    }

    fn retry_wait_duration(retry_interval_secs: u64) -> Duration {
        match retry_interval_secs {
            0 => Duration::from_secs(60 * 60),
            secs => Duration::from_secs(secs),
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

        let requests = match repo::get_retryable_item_requests(&self.db_pool).await {
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
            let items = match repo::get_pending_items_for_retry(&self.db_pool, item_type).await {
                Ok(items) => items,
                Err(e) => {
                    tracing::error!(error = %e, "failed to fetch pending items for retry");
                    vec![]
                }
            };

            for item in items {
                match item.state {
                    MediaItemState::Indexed | MediaItemState::PartiallyCompleted => {
                        self.job_queue.release_dedup("scrape", item.id).await;
                        orchestrator.queue_scrape_for_item(&item, None, true).await;
                    }
                    MediaItemState::Scraped => {
                        self.job_queue.release_dedup("download", item.id).await;
                        orchestrator.queue_download_for_item(&item).await;
                    }
                    _ => {}
                }
            }
        }
    }
}
