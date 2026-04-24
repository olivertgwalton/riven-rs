use std::collections::HashMap;
use std::sync::atomic::Ordering;

use futures::stream::{self, StreamExt};

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::repo;

use crate::context::{
    build_parse_item_context_with_hierarchy, load_media_item_hierarchy_or_log,
    load_media_item_or_log,
};
use crate::discovery::rank_streams;
use crate::flows::{run_plugin_hook, start_plugin_flow};
use crate::{JobQueue, ParseScrapeResultsJob, ScrapeJob, ScrapePluginJob};

/// How many times we will re-push a `ScrapeJob` whose entire scraper pool was
/// temporarily deferred before giving up and letting the retry scheduler handle it.
const MAX_RATE_LIMIT_REPUSH: u32 = 3;

/// Bump `failed_attempts`, then — if the item has now blown through the
/// configured ceiling — flip its state to `Failed` and cascade so the retry
/// scheduler stops picking it up forever.
async fn record_scrape_failure(queue: &JobQueue, item: &riven_db::entities::MediaItem) {
    let id = item.id;
    if let Err(err) = repo::increment_failed_attempts(&queue.db_pool, id).await {
        tracing::warn!(id, %err, "failed to increment failed_attempts");
        return;
    }

    let max = queue.maximum_scrape_attempts.load(Ordering::Relaxed);
    if max == 0 {
        return;
    }
    // `item.failed_attempts` was read before the increment above.
    let projected = item.failed_attempts.saturating_add(1) as u32;
    if projected < max {
        return;
    }

    tracing::warn!(
        id,
        failed_attempts = projected,
        max,
        "item exceeded maximum_scrape_attempts; marking Failed"
    );
    if let Err(err) = repo::mark_item_failed(&queue.db_pool, id).await {
        tracing::warn!(id, %err, "failed to mark item as Failed");
        return;
    }
    if let Err(err) = repo::cascade_state_update(&queue.db_pool, item).await {
        tracing::warn!(id, %err, "failed to cascade Failed state to parents");
    }
}

fn scrape_event(job: &ScrapeJob) -> RivenEvent {
    RivenEvent::MediaItemScrapeRequested {
        id: job.id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        title: job.title.clone(),
        season: job.season,
        episode: job.episode,
    }
}

pub async fn start(id: i64, job: &ScrapeJob, queue: &JobQueue) {
    tracing::debug!(id, "running scrape flow");
    let Some(item) = load_media_item_or_log(&queue.db_pool, id, "scrape").await else {
        return;
    };

    if !matches!(
        item.state,
        MediaItemState::Indexed
            | MediaItemState::Ongoing
            | MediaItemState::Scraped
            | MediaItemState::PartiallyCompleted
            | MediaItemState::Completed
    ) {
        tracing::debug!(id, state = ?item.state, "skipping scrape");
        return;
    }

    if let Err(err) = repo::clear_blacklisted_streams(&queue.db_pool, id).await {
        tracing::warn!(id, %err, "failed to clear blacklisted streams");
    }

    if start_plugin_flow(
        queue,
        "scrape",
        id,
        EventType::MediaItemScrapeRequested,
        |plugin_name| async move {
            queue
                .push_scrape_plugin(ScrapePluginJob {
                    id,
                    plugin_name,
                    item_type: job.item_type,
                    imdb_id: job.imdb_id.clone(),
                    title: job.title.clone(),
                    season: job.season,
                    episode: job.episode,
                    auto_download: job.auto_download,
                    rate_limit_retries: job.rate_limit_retries,
                })
                .await;
        },
    )
    .await
        == 0
    {
        finalize(job, queue).await;
    }
}

pub async fn handle_plugin(job: &ScrapePluginJob, queue: &JobQueue) {
    // Guard against items deleted while this job was waiting in the queue.
    // Without this check the plugin would make a full external HTTP request
    // before discovering in `finalize` that the item is gone.
    if load_media_item_or_log(&queue.db_pool, job.id, "scrape-plugin")
        .await
        .is_none()
    {
        // Last plugin to drain also clears the flow keys so they don't linger until TTL.
        if queue.flow_complete_child("scrape", job.id).await {
            queue.clear_flow("scrape", job.id).await;
            queue.clear_flow_results("scrape", job.id).await;
        }
        return;
    }

    let event = scrape_event(&ScrapeJob {
        id: job.id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        title: job.title.clone(),
        season: job.season,
        episode: job.episode,
        auto_download: job.auto_download,
        rate_limit_retries: job.rate_limit_retries,
    });

    let parent = ScrapeJob {
        id: job.id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        title: job.title.clone(),
        season: job.season,
        episode: job.episode,
        auto_download: job.auto_download,
        rate_limit_retries: job.rate_limit_retries,
    };

    if run_plugin_hook(
        queue,
        "scrape",
        job.id,
        &job.plugin_name,
        &event,
        "scraper",
        |response| match response {
            HookResponse::Scrape(streams) if !streams.is_empty() => Some(streams),
            _ => None,
        },
    )
    .await
    {
        finalize(&parent, queue).await;
    }
}

pub async fn finalize(job: &ScrapeJob, queue: &JobQueue) {
    let id = job.id;
    let Some(item) = load_media_item_or_log(&queue.db_pool, id, "scrape finalize").await else {
        queue.clear_flow_results("scrape", id).await;
        queue.clear_flow("scrape", id).await;
        queue.clear_flow_rate_limited("scrape", id).await;
        return;
    };

    // Skip items in terminal/non-processable states — stale plugin jobs can
    // still drain after an item is marked Failed; without this guard they
    // would fire spurious "no streams" notifications indefinitely.
    if matches!(item.state, MediaItemState::Failed | MediaItemState::Paused) {
        tracing::debug!(id, state = ?item.state, "skipping finalize for non-processable state");
        queue.clear_flow_results("scrape", id).await;
        queue.clear_flow("scrape", id).await;
        queue.clear_flow_rate_limited("scrape", id).await;
        return;
    }

    queue.clear_flow("scrape", id).await;

    let result_count = queue.flow_result_count("scrape", id).await;
    let rate_limited_count = queue.flow_rate_limited_count("scrape", id).await;
    queue.clear_flow_rate_limited("scrape", id).await;

    if result_count == 0 {
        queue.clear_flow_results("scrape", id).await;

        // If every plugin that ran deferred (none returned a genuine "no
        // streams" verdict) and we haven't exhausted our re-push budget,
        // re-queue this scrape job immediately rather than leaving the item
        // stuck in Indexed until the retry scheduler fires.
        if rate_limited_count > 0 && job.rate_limit_retries < MAX_RATE_LIMIT_REPUSH {
            tracing::warn!(
                id,
                rate_limited_count,
                retry = job.rate_limit_retries + 1,
                max = MAX_RATE_LIMIT_REPUSH,
                "all scrapers deferred; re-pushing scrape job"
            );
            queue
                .push_scrape(ScrapeJob {
                    rate_limit_retries: job.rate_limit_retries + 1,
                    ..job.clone()
                })
                .await;
            return;
        }

        tracing::info!(id, rate_limited_count, "no streams found by any scraper");
        let item_title = if item.item_type == MediaItemType::Season {
            format!("{} - {}", job.title, item.title)
        } else {
            item.title.clone()
        };
        record_scrape_failure(queue, &item).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type: item.item_type,
            })
            .await;
        return;
    }

    tracing::debug!(id, count = result_count, "pushing parse-scrape-results job");
    queue
        .push_parse_scrape_results(ParseScrapeResultsJob {
            id,
            auto_download: job.auto_download,
        })
        .await;
}

pub async fn parse_results(id: i64, _job: &ParseScrapeResultsJob, queue: &JobQueue) {
    tracing::debug!(id, "running parse-scrape-results flow");

    let Some(item) = load_media_item_or_log(&queue.db_pool, id, "parse-scrape-results").await
    else {
        return;
    };

    let processable = matches!(
        item.state,
        MediaItemState::Indexed
            | MediaItemState::Ongoing
            | MediaItemState::Scraped
            | MediaItemState::PartiallyCompleted
    );
    if !processable {
        tracing::debug!(id, state = ?item.state, "item not in processable state for scrape persist; skipping");
        return;
    }

    let hierarchy =
        load_media_item_hierarchy_or_log(&queue.db_pool, item.id, "parse-scrape-results hierarchy")
            .await;
    let parse_context =
        build_parse_item_context_with_hierarchy(&queue.db_pool, item, hierarchy.as_ref()).await;
    let item = parse_context.item;
    let item_title = parse_context.item_title;
    let item_type = parse_context.item_type;

    let responses: Vec<ScrapeResponse> = queue.flow_load_results("scrape", id).await;
    queue.clear_flow_results("scrape", id).await;

    let streams = responses
        .into_iter()
        .fold(HashMap::new(), |mut acc, streams| {
            acc.extend(streams);
            acc
        });

    let ranked = tokio::task::spawn_blocking(move || rank_streams(parse_context.parse, streams))
        .await
        .unwrap_or_default();

    let stream_count = stream::iter(ranked)
        .map(|candidate| {
            let pool = queue.db_pool.clone();
            async move {
                let stream = match repo::upsert_stream(
                    &pool,
                    &candidate.info_hash,
                    &build_magnet_uri(&candidate.info_hash),
                    candidate.parsed_data,
                    candidate.rank,
                    candidate.file_size_bytes,
                )
                .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, info_hash = %candidate.info_hash, title = %candidate.title, "failed to upsert stream");
                        return false;
                    }
                };
                match repo::link_stream_to_item(&pool, id, stream.id).await {
                    Ok(_) => true,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to link stream to item");
                        false
                    }
                }
            }
        })
        .buffer_unordered(4)
        .filter(|ok| futures::future::ready(*ok))
        .count()
        .await;

    if let Err(e) = repo::update_scraped(&queue.db_pool, id).await {
        tracing::error!(error = %e, "failed to update scraped timestamp");
    }

    tracing::info!(id, stream_count, "parse-scrape-results completed");

    if stream_count == 0 {
        record_scrape_failure(queue, &item).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type,
            })
            .await;
    } else {
        if let Err(e) = repo::refresh_state_cascade(&queue.db_pool, &item).await {
            tracing::error!(error = %e, "failed to refresh state after scrape");
        }
        if let Err(err) = repo::reset_failed_attempts(&queue.db_pool, id).await {
            tracing::warn!(id, %err, "failed to reset failed_attempts");
        }
        queue
            .notify(RivenEvent::MediaItemScrapeSuccess {
                id,
                title: item_title,
                item_type,
                stream_count,
            })
            .await;
    }
}
