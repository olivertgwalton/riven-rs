use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::JobQueue;
use crate::main_orchestrator::MainOrchestrator;

/// Periodic scheduler.
pub struct Scheduler {
    job_queue: Arc<JobQueue>,
    cancel: CancellationToken,
}

impl Scheduler {
    pub fn new(job_queue: Arc<JobQueue>, cancel: CancellationToken) -> Self {
        Self { job_queue, cancel }
    }

    pub async fn run(self) {
        let mut content_tick = tokio::time::interval(Duration::from_secs(120));
        let mut cleanup_tick = tokio::time::interval(Duration::from_secs(60 * 60));
        // Check for stale workers every 60s (2× the apalis default keep-alive of 30s).
        // Rescued jobs will release their own dedup keys on completion, mirroring
        // the normal job lifecycle so dedup state stays consistent after recovery.
        let mut worker_recovery_tick = tokio::time::interval(Duration::from_secs(60));
        let retry_wait =
            Self::retry_wait_duration(self.job_queue.retry_interval_secs.load(Ordering::SeqCst));
        let mut retry_sleep = std::pin::pin!(tokio::time::sleep(retry_wait));

        self.retry_library().await;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    tracing::info!("scheduler shutting down");
                    return;
                }
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
                    let queues = self.job_queue.queue_names();
                    // 60s threshold: a worker missing two heartbeats is considered dead.
                    crate::recover_stale_workers(&mut redis, &queues, 60).await;
                }
            }
        }
    }

    fn retry_wait_duration(retry_interval_secs: u64) -> Duration {
        match retry_interval_secs {
            0 => Duration::from_secs(60 * 10),
            secs => Duration::from_secs(secs),
        }
    }

    async fn cleanup_runtime_state(&self) {
        let mut redis = self.job_queue.redis.clone();
        let queues = self.job_queue.queue_names();
        crate::prune_queue_history(&mut redis, &queues).await;
    }

    /// Retry-library actor. Delegated to `MainOrchestrator`, which is the
    /// single owner of the retry policy.
    async fn retry_library(&self) {
        MainOrchestrator::new(Arc::clone(&self.job_queue))
            .retry_library()
            .await;
    }
}
