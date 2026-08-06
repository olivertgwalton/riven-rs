use std::sync::Arc;
use std::time::Duration;

use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;

use riven_core::events::{DispatchStrategy, EventType, HookResponse, RivenEvent};
use riven_core::http::{RateLimitedError, RetryLaterError};

use crate::context::{is_scrapeable, load_media_item_or_log};
use crate::dedup::DedupGuard;
use crate::jobs::{HookAck, HookOutcome};
use crate::{
    DownloadJob, HOOK_COLLECT_TIMEOUT_SECS, IndexJob, JobQueue, ParseScrapeResultsJob,
    PluginHookJob, ProcessMediaItemJob, RankStreamsJob, ScrapeJob,
};

async fn handle_index_job(job: IndexJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("index", job.id, q.redis.clone());
    crate::application::index::start(&job, &q).await;
    Ok(())
}

async fn handle_scrape_job(job: ScrapeJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("scrape", job.id, q.redis.clone());
    crate::application::scrape::start(job.id, &job, &q).await;
    Ok(())
}

async fn handle_parse_scrape_results_job(
    job: ParseScrapeResultsJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
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

async fn handle_process_media_item_job(
    job: ProcessMediaItemJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    crate::application::process_media_item::run(&job, &q).await;
    Ok(())
}

/// Per-(plugin, event) hook worker — one queue per plugin per subscribed
/// event, each running this handler.
///
/// `Inline` events never reach this handler — `JobQueue::new` skips creating
/// their queues. Broadcast events just dispatch; nobody reads their result.
/// Fan-in events return a [`HookOutcome`] which apalis stores as the task
/// result for the awaiting orchestrator (`fan_out_and_collect`) to read.
///
/// Fan-in outcomes are always `Ok` at the apalis layer — a failed plugin is
/// reported as `HookOutcome::Failed`, not a task error — so children are
/// never retried and the orchestrator's wait resolves on first completion.
async fn handle_plugin_hook_job(
    job: PluginHookJob,
    q: Data<Arc<JobQueue>>,
) -> Result<HookAck, BoxDynError> {
    let event_type = job.event.event_type();
    match event_type.dispatch_strategy() {
        DispatchStrategy::Broadcast => handle_broadcast(&job, &q).await,
        DispatchStrategy::FanIn => Ok(Ok(handle_fan_in(&job, &q).await)),
        DispatchStrategy::Inline => {
            tracing::error!(?event_type, "Inline event reached plugin-hook worker");
            handle_broadcast(&job, &q).await
        }
    }
}

async fn handle_broadcast(job: &PluginHookJob, q: &JobQueue) -> Result<HookAck, BoxDynError> {
    match q
        .registry
        .dispatch_to_plugin(&job.plugin_name, &job.event)
        .await
    {
        Some(Err(error)) => Err(error.into()),
        Some(Ok(_)) | None => Ok(Ok(HookOutcome::Skipped)),
    }
}

async fn handle_fan_in(job: &PluginHookJob, q: &JobQueue) -> HookOutcome {
    let event_type = job.event.event_type();

    if let Some(id) = job.event.media_item_id() {
        let maybe_item = load_media_item_or_log(id, "plugin-hook").await;
        match (&job.event, &maybe_item) {
            (_, None) => return HookOutcome::Skipped,
            (RivenEvent::MediaItemScrapeRequested { .. }, Some(item))
                if !is_scrapeable(item.state) =>
            {
                tracing::debug!(
                    id,
                    state = ?item.state,
                    plugin = %job.plugin_name,
                    "skipping stale scrape plugin-hook job; item no longer processable"
                );
                return HookOutcome::Skipped;
            }
            _ => {}
        }
    }

    match q
        .registry
        .dispatch_to_plugin(&job.plugin_name, &job.event)
        .await
    {
        Some(Ok(response)) => HookOutcome::Response(extract_fan_in_response(event_type, response)),
        Some(Err(ref error)) if error.is::<RateLimitedError>() || error.is::<RetryLaterError>() => {
            tracing::warn!(
                plugin = %job.plugin_name,
                ?event_type,
                "plugin hook deferred (rate limited)"
            );
            HookOutcome::RateLimited
        }
        Some(Err(error)) => {
            tracing::error!(
                plugin = %job.plugin_name,
                ?event_type,
                error = %error,
                "plugin hook failed"
            );
            HookOutcome::Failed
        }
        None => {
            tracing::warn!(plugin = %job.plugin_name, ?event_type, "plugin not found at dispatch time");
            HookOutcome::Skipped
        }
    }
}

/// Return the JSON value that should be stored under the per-plugin slot of
/// the fan-in flow's results hash. `None` means "this response carries no
/// useful payload for aggregation" (the empty-streams case for scrape, etc.).
fn extract_fan_in_response(
    event_type: EventType,
    response: HookResponse,
) -> Option<serde_json::Value> {
    match (event_type, response) {
        (EventType::MediaItemScrapeRequested, HookResponse::Scrape(streams)) => (!streams
            .is_empty())
        .then(|| serde_json::to_value(streams).ok())
        .flatten(),
        (EventType::MediaItemIndexRequested, HookResponse::Index(indexed)) => {
            serde_json::to_value(*indexed).ok()
        }
        (EventType::ContentServiceRequested, HookResponse::ContentService(response)) => {
            serde_json::to_value(*response).ok()
        }
        _ => None,
    }
}

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
    // No `.timeout(...)` layer — for jobs whose own work is deliberately
    // unbounded (see the call site for why), a flat outer deadline would just
    // reintroduce the thing the inner code was written to avoid.
    ($monitor:expr, $queue:expr, $name:literal, $storage:ident, $n:expr, $handler:ident) => {{
        let q = Arc::clone(&$queue);
        $monitor.register(move |_| {
            WorkerBuilder::new($name)
                .backend(q.$storage.clone())
                .enable_tracing()
                .catch_panic()
                .concurrency($n)
                .data(q.clone())
                .build($handler)
        })
    }};
}

/// `usenet_download_workers` is the connection-budget-derived concurrency for
/// the download/rank-streams workers when usenet is configured (each ingest is
/// capped to a fixed per-job connection budget, so `pool ÷ cap` workers fill
/// the pool without oversubscribing it). `None` => conservative default.
pub fn start_workers(queue: Arc<JobQueue>, usenet_download_workers: Option<usize>) -> Monitor {
    let cpu_n = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);

    let orchestrator_n = cpu_n.saturating_mul(3).div_ceil(2);

    let plugin_n = cpu_n.max(4) * 8;

    let parse_n = cpu_n.max(5);

    let download_n = usenet_download_workers.unwrap_or_else(|| cpu_n.max(10));

    let m = Monitor::new();
    // Index/scrape orchestrators await their plugin-hook children in-handler
    // (`fan_out_and_collect`), so a slot is held for the whole fan-in: the
    // timeout must exceed the child-wait budget, and concurrency matches the
    // hook workers' width — a waiting orchestrator is just an idle future, and
    // anything narrower would gate scrape throughput below the plugin queues'.
    let m = register_worker!(
        m,
        queue,
        "riven-index",
        index_storage,
        plugin_n,
        handle_index_job,
        HOOK_COLLECT_TIMEOUT_SECS + 60
    );
    let m = register_worker!(
        m,
        queue,
        "riven-scrape",
        scrape_storage,
        plugin_n,
        handle_scrape_job,
        HOOK_COLLECT_TIMEOUT_SECS + 60
    );
    let m = register_worker!(
        m,
        queue,
        "riven-parse",
        parse_storage,
        parse_n,
        handle_parse_scrape_results_job,
        300
    );
    // No outer job timeout — matches riven-ts, whose equivalent
    // find-valid-torrent loop has none either (BullMQ workers there just
    // renew their lock). A candidate's own work here (e.g. plugin-usenet's
    // PAR2-verifying ingest) is deliberately unbounded by design — see the
    // "No wall-clock timeout here" comment in plugin-usenet — for a healthy
    // release under heavy pool contention; a flat deadline on top of that
    // would just kill legitimate long-running ingests the same way the old
    // 600s timeout did.
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
        handle_rank_streams_job,
        300
    );
    let m = register_worker!(
        m,
        queue,
        "riven-process-media-item",
        process_media_item_storage,
        orchestrator_n,
        handle_process_media_item_job,
        60
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
                .timeout(Duration::from_secs(180))
                .concurrency(plugin_n)
                .data(q.clone())
                .build(handle_plugin_hook_job)
        });
    }
    m
}
