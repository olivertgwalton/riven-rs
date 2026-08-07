use std::sync::Arc;
use std::time::Duration;

use riven_queue::JobQueue;
use riven_queue::worker::Scheduler;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct RuntimeTasks {
    monitor: JoinHandle<()>,
    scheduler: JoinHandle<()>,
    indexer_stats: JoinHandle<()>,
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
    let indexer_stats = spawn_indexer_stats_flusher(cancel.clone());
    let scheduler = tokio::spawn(run_scheduler(queue, cancel));
    RuntimeTasks {
        monitor,
        scheduler,
        indexer_stats,
    }
}

impl RuntimeTasks {
    pub(crate) async fn drain(self, api: JoinHandle<()>) {
        let drain = async {
            let (api, monitor, scheduler, indexer_stats) =
                tokio::join!(api, self.monitor, self.scheduler, self.indexer_stats);
            for (name, result) in [
                ("api", api),
                ("worker monitor", monitor),
                ("scheduler", scheduler),
                ("indexer stats flusher", indexer_stats),
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

/// Persist the per-indexer query/grab counters the scrape and download paths
/// keep in memory. Batched on a tick because a scrape is one request per page
/// per indexer, and none of those should wait on a database write.
pub(crate) fn spawn_indexer_stats_flusher(cancel: CancellationToken) -> JoinHandle<()> {
    const INTERVAL: Duration = Duration::from_secs(60);

    tokio::spawn(async move {
        let mut tick = tokio::time::interval(INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = cancel.cancelled() => break,
            }
            flush_indexer_stats().await;
        }
        // Whatever accumulated since the last tick is still worth keeping.
        flush_indexer_stats().await;
    })
}

async fn flush_indexer_stats() {
    for (indexer, delta) in riven_core::indexer_stats::drain() {
        if let Err(error) = riven_db::repo::add_indexer_stats(&indexer, delta).await {
            tracing::debug!(%error, %indexer, "indexer stats flush failed; keeping the delta");
            riven_core::indexer_stats::restore(&indexer, delta);
        }
    }
}

/// Resolve once a queue's claimed tasks have stopped moving, having first put
/// them back on the queue.
///
/// This is the only externally visible trace of a worker whose task stream has
/// died. apalis runs a worker's heartbeat and its task stream independently, so
/// a single `StreamError` — a Redis timeout, a blip, a failed `get_jobs.lua` —
/// ends `CallAll` permanently while the heartbeat carries on re-registering.
/// The worker therefore stays in the workers set with a fresh timestamp and a
/// healthy-looking process, and never fetches or runs anything again.
///
/// Which is why liveness is judged here on whether claimed work is progressing
/// rather than on the registration: any check of "is a worker registered"
/// cannot fire for this failure, because the registration is exactly the part
/// that survives.
async fn watch_for_stalled_queues(queue: &Arc<JobQueue>, cancel: &CancellationToken) {
    const POLL: Duration = Duration::from_secs(60);

    let mut redis = queue.redis.clone();
    let queues = queue.queue_names();
    let mut watch = riven_queue::InflightWatch::new();

    loop {
        tokio::select! {
            _ = tokio::time::sleep(POLL) => {}
            _ = cancel.cancelled() => return std::future::pending().await,
        }

        let stalled = watch
            .observe(&mut redis, &queues, riven_queue::INFLIGHT_STALL_AFTER)
            .await;
        if stalled.is_empty() {
            continue;
        }

        for queue_name in &stalled {
            tracing::error!(
                queue = %queue_name.queue,
                stuck_tasks = queue_name.stuck_tasks,
                stalled_for_secs = queue_name.stalled_for.as_secs(),
                "queue's claimed tasks have not moved; its worker's task stream is dead"
            );
        }

        // Put the stranded tasks back before rebuilding the workers. apalis
        // ships `reenqueue_orphaned_jobs.lua` for exactly this and never calls
        // it, so without this they stay claimed by a worker that will never
        // run them, and the items behind them stay blocked by their dedup keys.
        let names: Vec<String> = stalled.into_iter().map(|s| s.queue).collect();
        match riven_queue::clear_worker_registrations(&mut redis, &names).await {
            Ok(report) => tracing::info!(
                queues = ?names,
                requeued = report.jobs,
                "requeued tasks stranded by a dead task stream"
            ),
            Err(error) => tracing::error!(%error, "failed to requeue stranded tasks"),
        }
        return;
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
            () = watch_for_stalled_queues(&queue, &cancel) => {
                // A worker died without taking the monitor down with it, so the
                // monitor future is still pending and would park this loop
                // forever. Drop it and go around, rebuilding every worker with
                // a live task stream; the stranded tasks were requeued above.
                handle.abort();
                tracing::error!("restarting the monitor after a stalled queue");
                continue;
            }
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
