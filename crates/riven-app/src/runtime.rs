use std::sync::Arc;
use std::time::Duration;

use riven_queue::JobQueue;
use riven_queue::worker::Scheduler;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct RuntimeTasks {
    monitor: JoinHandle<()>,
    scheduler: JoinHandle<()>,
}

pub(crate) fn start(
    queue: Arc<JobQueue>,
    cancel: CancellationToken,
    usenet_download_workers: Option<usize>,
) -> RuntimeTasks {
    let monitor = tokio::spawn(run_worker_monitor(
        queue.clone(),
        cancel.clone(),
        usenet_download_workers,
    ));
    let scheduler = tokio::spawn(run_scheduler(queue, cancel));
    RuntimeTasks { monitor, scheduler }
}

impl RuntimeTasks {
    pub(crate) async fn drain(self, api: JoinHandle<()>) {
        let drain = async {
            let (api, monitor, scheduler) = tokio::join!(api, self.monitor, self.scheduler);
            for (name, result) in [
                ("api", api),
                ("worker monitor", monitor),
                ("scheduler", scheduler),
            ] {
                if let Err(error) = result {
                    tracing::error!(?error, task = name, "runtime task failed during drain");
                }
            }
        };

        if tokio::time::timeout(Duration::from_secs(30), drain)
            .await
            .is_err()
        {
            tracing::warn!("drain timed out after 30s; proceeding to unmount");
        }
    }
}

async fn run_worker_monitor(
    queue: Arc<JobQueue>,
    cancel: CancellationToken,
    usenet_download_workers: Option<usize>,
) {
    let mut redis = queue.redis.clone();
    let queues = queue.queue_names();
    const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(60);
    const RESTART_BACKOFF: Duration = Duration::from_secs(5);

    while !cancel.is_cancelled() {
        let maintenance = async {
            if let Err(error) = riven_queue::clear_worker_registrations(&mut redis, &queues).await {
                tracing::error!(%error, "failed to recover startup worker registrations");
            }
            riven_queue::purge_orphaned_worker_sets(&mut redis, &queues).await;
            riven_queue::purge_orphaned_active_jobs(&mut redis, &queues).await;
            riven_queue::purge_stale_dedup_keys(&mut redis).await;
        };
        if tokio::time::timeout(MAINTENANCE_TIMEOUT, maintenance)
            .await
            .is_err()
        {
            tracing::warn!("pre-start Redis maintenance timed out; starting workers anyway");
        }

        let handle = tokio::spawn({
            let queue = queue.clone();
            async move {
                riven_queue::start_workers(queue, usenet_download_workers)
                    .run()
                    .await
            }
        });
        tokio::pin!(handle);
        let result = tokio::select! {
            result = &mut handle => result,
            _ = cancel.cancelled() => {
                handle.abort();
                break;
            }
        };
        match result {
            Ok(Ok(())) => tracing::warn!("apalis monitor exited, restarting"),
            Ok(Err(error)) => tracing::error!(%error, "apalis monitor error, restarting"),
            Err(error) if error.is_panic() => {
                tracing::error!("apalis monitor panicked, restarting")
            }
            Err(error) => tracing::error!(?error, "apalis monitor task failed, restarting"),
        }
        tokio::select! {
            _ = tokio::time::sleep(RESTART_BACKOFF) => {}
            _ = cancel.cancelled() => break,
        }
    }
}

async fn run_scheduler(queue: Arc<JobQueue>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        let result = tokio::spawn(Scheduler::new(queue.clone(), cancel.clone()).run()).await;
        if cancel.is_cancelled() {
            break;
        }
        match result {
            Ok(_) => tracing::warn!("scheduler exited unexpectedly, restarting"),
            Err(error) if error.is_panic() => {
                tracing::error!("scheduler panicked, restarting in 5s")
            }
            Err(error) => tracing::error!(?error, "scheduler task failed, restarting in 5s"),
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            _ = cancel.cancelled() => break,
        }
    }
}

#[cfg(unix)]
pub(crate) async fn wait_for_shutdown() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = sigterm.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
pub(crate) async fn wait_for_shutdown() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}
