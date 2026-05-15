use std::sync::Arc;
use std::time::Duration;

use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;

use riven_core::events::{DispatchStrategy, EventType, HookResponse, RivenEvent};
use riven_core::http::{RateLimitedError, RetryLaterError};
use riven_core::types::MediaItemState;

use crate::context::load_media_item_or_log;
use crate::dedup::DedupGuard;
use crate::{
    DownloadJob, IndexJob, JobQueue, ParseScrapeResultsJob, PluginHookJob, ProcessMediaItemJob,
    RankStreamsJob, ScrapeJob,
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

async fn handle_scrape_job(job: ScrapeJob, q: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let _guard = DedupGuard::new("scrape", job.id, q.redis.clone());
    crate::application::scrape::start(job.id, &job, &q).await;
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
/// their queues. Broadcast events just dispatch and return. Fan-in events
/// also store the response in the flow's results hash and, on the last
/// child's completion, run the orchestrator's `finalize` inline.
async fn handle_plugin_hook_job(
    job: PluginHookJob,
    q: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    let event_type = job.event.event_type();
    match event_type.dispatch_strategy() {
        DispatchStrategy::Broadcast => handle_broadcast(&job, &q).await,
        DispatchStrategy::FanIn { prefix } => handle_fan_in(&job, &q, prefix).await,
        DispatchStrategy::Inline => {
            // Should be unreachable — Inline events are filtered out at queue
            // registration time. If a job is here, treat it as a broadcast so
            // it doesn't sit dead in the queue.
            tracing::error!(?event_type, "Inline event reached plugin-hook worker");
            handle_broadcast(&job, &q).await
        }
    }
}

async fn handle_broadcast(job: &PluginHookJob, q: &JobQueue) -> Result<(), BoxDynError> {
    match q
        .registry
        .dispatch_to_plugin(&job.plugin_name, &job.event)
        .await
    {
        Some(Err(error)) => Err(error.into()),
        Some(Ok(_)) | None => Ok(()),
    }
}

async fn handle_fan_in(
    job: &PluginHookJob,
    q: &JobQueue,
    prefix: &'static str,
) -> Result<(), BoxDynError> {
    let event_type = job.event.event_type();
    let Some(scope) = job.scope else {
        tracing::error!(?event_type, "fan-in plugin-hook job missing scope");
        return Ok(());
    };

    if let Some(id) = job.event.media_item_id() {
        let maybe_item = load_media_item_or_log(&q.db_pool, id, "plugin-hook").await;
        let drop_child = match (&job.event, &maybe_item) {
            (_, None) => true,
            (RivenEvent::MediaItemScrapeRequested { .. }, Some(item))
                if !is_scrapeable(item.state) =>
            {
                tracing::debug!(
                    id,
                    state = ?item.state,
                    plugin = %job.plugin_name,
                    "skipping stale scrape plugin-hook job; item no longer processable"
                );
                true
            }
            _ => false,
        };
        if drop_child {
            if q.flow_complete_child(prefix, scope).await {
                q.clear_flow_all(prefix, scope).await;
                finalize_event(q, &job.event, scope).await;
            }
            return Ok(());
        }
    }

    match q
        .registry
        .dispatch_to_plugin(&job.plugin_name, &job.event)
        .await
    {
        Some(Ok(response)) => {
            if let Some(payload) = extract_fan_in_response(event_type, response) {
                q.flow_store_result(prefix, scope, &job.plugin_name, &payload)
                    .await;
            }
        }
        Some(Err(ref error)) if error.is::<RateLimitedError>() || error.is::<RetryLaterError>() => {
            q.flow_increment_rate_limited(prefix, scope).await;
            tracing::warn!(
                plugin = %job.plugin_name,
                ?event_type,
                scope,
                "plugin hook deferred (rate limited)"
            );
        }
        Some(Err(error)) => {
            tracing::error!(
                plugin = %job.plugin_name,
                ?event_type,
                scope,
                error = %error,
                "plugin hook failed"
            );
        }
        None => {
            tracing::warn!(plugin = %job.plugin_name, ?event_type, "plugin not found at dispatch time");
        }
    }

    if q.flow_complete_child(prefix, scope).await {
        finalize_event(q, &job.event, scope).await;
    }
    Ok(())
}

/// States the scrape pipeline accepts. Kept in sync with the dispatch-time
/// gate in `scrape::start` and the post-fan-in gate in `parse_results`.
fn is_scrapeable(state: MediaItemState) -> bool {
    matches!(
        state,
        MediaItemState::Indexed
            | MediaItemState::Ongoing
            | MediaItemState::Scraped
            | MediaItemState::PartiallyCompleted
    )
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

/// Last-child completion handoff for orchestrator-driven fan-in flows. The
/// matching `finalize` runs inline here in whichever plugin-hook worker
/// drained the last child.
async fn finalize_event(queue: &JobQueue, event: &RivenEvent, scope: i64) {
    match event {
        RivenEvent::MediaItemScrapeRequested { .. } => {
            crate::application::scrape::finalize(scope, queue).await;
        }
        RivenEvent::MediaItemIndexRequested { .. } => {
            crate::application::index::finalize(scope, queue).await;
        }
        RivenEvent::ContentServiceRequested => {
            crate::flows::request_content::finalize(scope, queue).await;
        }
        _ => {
            tracing::error!(?event, "finalize_event called for non-fan-in event");
        }
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
    // Sized at `cpu * 1.5` so every flow worker (process-media-item, scrape,
    // download, rank-streams, etc.) can have many items in flight without
    // changing per-plugin load — the plugin-hook queues are the actual rate cap.
    let orchestrator_n = cpu_n.saturating_mul(3).div_ceil(2);

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
        handle_index_job,
        60
    );
    let m = register_worker!(
        m,
        queue,
        "riven-scrape",
        scrape_storage,
        orchestrator_n,
        handle_scrape_job,
        60
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
    let m = register_worker!(
        m,
        queue,
        "riven-download",
        download_storage,
        download_n,
        handle_download_job,
        600
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
    // Per-item state machine — orchestration only, fans out and returns.
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
