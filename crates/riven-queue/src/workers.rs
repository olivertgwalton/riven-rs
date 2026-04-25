use std::sync::Arc;
use std::time::Duration;

use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;

use crate::dedup::DedupGuard;
use crate::{
    ContentServiceJob, DownloadJob, IndexJob, IndexPluginJob, JobQueue, ParseScrapeResultsJob,
    PluginHookJob, RankStreamsJob, ScrapeJob, ScrapePluginJob,
};

// ── Job handlers ──────────────────────────────────────────────────────────────

// All flow `run` functions return `()` — they handle errors internally (log + emit events)
// and are intentionally infallible. Handlers always return Ok(()) so Apalis does not retry;
// retries are driven by the flows themselves re-enqueuing jobs as needed.

async fn handle_index_job(job: IndexJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("index", job.id, q.redis.clone());
    crate::flows::index_item::run(&job, &q).await;
    Ok(())
}

async fn handle_index_plugin_job(
    job: IndexPluginJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    crate::flows::index_item::run_plugin(&job, &q).await;
    Ok(())
}

async fn handle_scrape_job(job: ScrapeJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("scrape", job.id, q.redis.clone());
    crate::flows::scrape_item::run(job.id, &job, &q).await;
    Ok(())
}

async fn handle_scrape_plugin_job(
    job: ScrapePluginJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    crate::flows::scrape_item::run_plugin(&job, &q).await;
    Ok(())
}

async fn handle_parse_scrape_results_job(
    job: ParseScrapeResultsJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("parse", job.id, q.redis.clone());
    crate::flows::parse_scrape_results::run(job.id, &job, &q).await;
    Ok(())
}

async fn handle_download_job(job: DownloadJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("download", job.id, q.redis.clone());
    crate::flows::download_item::run(job.id, &job, &q).await;
    Ok(())
}

async fn handle_rank_streams_job(
    job: RankStreamsJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("rank-streams", job.id, q.redis.clone());
    crate::flows::rank_streams::run(job.id, &job, &q).await;
    Ok(())
}

async fn handle_content_service_job(
    _job: ContentServiceJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    crate::flows::request_content::run(&q).await;
    Ok(())
}

/// Handler for per-(plugin, event) hook fan-out jobs enqueued by `JobQueue::notify`.
/// One worker is registered per (plugin, event) pair declared via `subscribed_events()`
/// — this handler runs for *all* of them, dispatching to the named plugin via the
/// in-process registry (which still owns the typed `on_*` hook surface).
///
/// Failures are propagated upward as `Err` so the apalis tracing layer logs them,
/// the apalis board surfaces the failed job, and a retry layer (if added per-worker
/// later) can act on them.
async fn handle_plugin_hook_job(
    job: PluginHookJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    match q
        .registry
        .dispatch_to_plugin(&job.plugin_name, &job.event)
        .await
    {
        Some(Err(error)) => Err(error.into()),
        Some(Ok(_)) | None => Ok(()),
    }
}

// ── Monitor factory ───────────────────────────────────────────────────────────

/// Standard tower-style layers applied to every worker:
/// - `enable_tracing`: span per job (id, queue, duration, success/failure)
/// - `catch_panic`: a `.unwrap()` deep in handler code becomes a job error rather
///   than killing the worker task. Without this, one panicking plugin or flow
///   would silently stop processing for that queue until restart.
/// - `timeout`: hard ceiling on a single job's runtime. Hung external HTTP calls
///   or DB queries can no longer wedge a worker slot indefinitely.
macro_rules! register_worker {
    ($monitor:expr, $queue:expr, $name:literal, $storage:ident, $n:expr, $handler:ident, $timeout_secs:expr) => {{
        let q = Arc::clone(&$queue);
        $monitor.register(move |_| {
            WorkerBuilder::new($name)
                .backend(q.$storage.clone())
                .enable_tracing()
                .catch_panic()
                .timeout(Duration::from_secs($timeout_secs))
                .concurrency($n)
                .data(q.clone())
                .build($handler)
        })
    }};
}

pub fn start_workers(queue: Arc<JobQueue>) -> Monitor {
    let cpu_n = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);

    // Orchestrators fan-out sub-jobs and return immediately — no IO, no blocking.
    // Apalis workers return right away so cpu_n allows more items to be fanned out in parallel.
    let orchestrator_n = cpu_n;

    // Plugin workers spend almost all their time waiting on external HTTP calls
    // (scrapers, TMDB/TVDB). Our jobs combine both in one task, so we set this high enough that external API rate limits — not
    // our concurrency cap — are the bottleneck.
    let plugin_n = cpu_n.max(4) * 8;

    // Parse is CPU-bound (spawn_blocking stream ranking) then sequential DB writes.
    let parse_n = cpu_n.max(5);

    // Download workers call the torrent-client API
    // Higher values risk overwhelming the client with simultaneous requests.
    let download_n = cpu_n.max(10);

    // Per-worker timeouts (seconds): orchestrators are quick fan-out → 60s.
    // Plugin workers do external HTTP → 180s. Parse/rank do CPU + DB → 300s.
    // Download workers wait on debrid APIs that can be slow → 600s.
    // Content fan-out hits multiple plugins serially → 600s.
    let m = Monitor::new();
    let m = register_worker!(
        m, queue, "riven-index", index_storage, orchestrator_n, handle_index_job, 60
    );
    let m = register_worker!(
        m, queue, "riven-index-plugin", index_plugin_storage, plugin_n, handle_index_plugin_job, 180
    );
    let m = register_worker!(
        m, queue, "riven-scrape", scrape_storage, orchestrator_n, handle_scrape_job, 60
    );
    let m = register_worker!(
        m, queue, "riven-scrape-plugin", scrape_plugin_storage, plugin_n, handle_scrape_plugin_job, 180
    );
    let m = register_worker!(
        m, queue, "riven-parse", parse_storage, parse_n, handle_parse_scrape_results_job, 300
    );
    let m = register_worker!(
        m, queue, "riven-download", download_storage, download_n, handle_download_job, 600
    );
    let m = register_worker!(
        m, queue, "riven-rank-streams", rank_streams_storage, download_n, handle_rank_streams_job, 300
    );
    let m = register_worker!(
        m, queue, "riven-content", content_storage, 1, handle_content_service_job, 600
    );

    // Register one worker per (plugin, event) hook storage. Each worker pulls only
    // from its own queue, so a slow or failing plugin can't block others. The same
    // tracing / catch-panic / timeout layers documented on `register_worker!` apply
    // here — 120 s is generous for the network I/O most hooks do (webhook delivery,
    // media-server library refresh) and short enough that a hung request surfaces
    // promptly via the apalis board.
    let mut m = m;
    for ((plugin_name, event_type), storage) in &queue.plugin_hook_storages {
        let q = Arc::clone(&queue);
        let storage = storage.clone();
        let worker_name = format!("hook-{plugin_name}-{}", event_type.slug());
        m = m.register(move |_| {
            WorkerBuilder::new(worker_name.clone())
                .backend(storage.clone())
                .enable_tracing()
                .catch_panic()
                .timeout(Duration::from_secs(120))
                .concurrency(plugin_n)
                .data(q.clone())
                .build(handle_plugin_hook_job)
        });
    }
    m
}
