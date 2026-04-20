use std::sync::Arc;

use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;

use crate::dedup::DedupGuard;
use crate::{
    ContentServiceJob, DownloadJob, IndexJob, IndexPluginJob, JobQueue, ParseScrapeResultsJob,
    RankStreamsJob, ScrapeJob, ScrapePluginJob,
};

// ── Job handlers ──────────────────────────────────────────────────────────────

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

// ── Monitor factory ───────────────────────────────────────────────────────────

macro_rules! register_worker {
    ($monitor:expr, $queue:expr, $name:literal, $storage:ident, $n:expr, $handler:ident) => {{
        let q = Arc::clone(&$queue);
        $monitor.register(move |_| {
            WorkerBuilder::new($name)
                .backend(q.$storage.clone())
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
        m,
        queue,
        "riven-index",
        index_storage,
        orchestrator_n,
        handle_index_job
    );
    let m = register_worker!(
        m,
        queue,
        "riven-index-plugin",
        index_plugin_storage,
        plugin_n,
        handle_index_plugin_job
    );
    let m = register_worker!(
        m,
        queue,
        "riven-scrape",
        scrape_storage,
        orchestrator_n,
        handle_scrape_job
    );
    let m = register_worker!(
        m,
        queue,
        "riven-scrape-plugin",
        scrape_plugin_storage,
        plugin_n,
        handle_scrape_plugin_job
    );
    let m = register_worker!(
        m,
        queue,
        "riven-parse",
        parse_storage,
        parse_n,
        handle_parse_scrape_results_job
    );
    let m = register_worker!(
        m,
        queue,
        "riven-download",
        download_storage,
        download_n,
        handle_download_job
    );
    let m = register_worker!(
        m,
        queue,
        "riven-rank-streams",
        rank_streams_storage,
        download_n,
        handle_rank_streams_job
    );
    register_worker!(
        m,
        queue,
        "riven-content",
        content_storage,
        1,
        handle_content_service_job
    )
}
