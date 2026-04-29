use std::sync::Arc;
use std::time::Duration;

use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;

use crate::dedup::DedupGuard;
use crate::{
    ContentServiceJob, DownloadJob, IndexJob, IndexPluginJob, JobQueue, ParseScrapeResultsJob,
    PluginHookJob, ProcessMediaItemJob, RankStreamsJob, ScrapeJob, ScrapePluginJob,
};

// ── Job handlers ──────────────────────────────────────────────────────────────

// All flow `run` functions return `()` — they handle errors internally (log + emit events)
// and are intentionally infallible. Handlers always return Ok(()) so Apalis does not retry;
// retries are driven by the flows themselves re-enqueuing jobs as needed.

async fn handle_index_job(job: IndexJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("index", job.id, q.redis.clone());
    crate::application::index::start(&job, &q).await;
    Ok(())
}

async fn handle_index_plugin_job(
    job: IndexPluginJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    crate::application::index::handle_plugin(&job, &q).await;
    Ok(())
}

async fn handle_scrape_job(job: ScrapeJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("scrape", job.id, q.redis.clone());
    crate::application::scrape::start(job.id, &job, &q).await;
    Ok(())
}

async fn handle_scrape_plugin_job(
    job: ScrapePluginJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    crate::application::scrape::handle_plugin(&job, &q).await;
    Ok(())
}

async fn handle_parse_scrape_results_job(
    job: ParseScrapeResultsJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("parse", job.id, q.redis.clone());
    crate::application::scrape::parse_results(job.id, &job, &q).await;
    Ok(())
}

async fn handle_download_job(job: DownloadJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("download", job.id, q.redis.clone());
    crate::application::download::run(job.id, &job, &q).await;
    Ok(())
}

async fn handle_rank_streams_job(
    job: RankStreamsJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("rank-streams", job.id, q.redis.clone());
    crate::application::download::run_rank_streams(job.id, &job, &q).await;
    Ok(())
}

async fn handle_content_service_job(
    _job: ContentServiceJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    crate::flows::request_content::run(&q).await;
    Ok(())
}

async fn handle_process_media_item_job(
    job: ProcessMediaItemJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    crate::application::process_media_item::run(&job, &q).await;
    Ok(())
}

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
    // Per-item state machine — orchestration only, fans out and returns.
    let m = register_worker!(
        m, queue, "riven-process-media-item", process_media_item_storage, orchestrator_n,
        handle_process_media_item_job, 60
    );

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
