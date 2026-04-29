use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

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

/// Backoff before the next attempt when every scraper deferred.
fn rate_limit_backoff(prior_retries: u32) -> Duration {
    let secs = match prior_retries {
        0 | 1 => 30 * 60,
        2..=4 => 2 * 60 * 60,
        5..=9 => 6 * 60 * 60,
        _ => 24 * 60 * 60,
    };
    Duration::from_secs(secs)
}

/// Remaining rate-limit retry budget given the configured ceiling and the
/// item's recorded "no streams" failures. `max == 0` disables the ceiling.
fn rate_limit_retry_budget(max: u32, failed_attempts: i32) -> u32 {
    if max == 0 {
        return u32::MAX;
    }
    max.saturating_sub(failed_attempts.max(0) as u32)
}

/// Bump `failed_attempts`. The DB trigger on the `failed_attempts` column
/// recomputes state — and applies the `failed_attempts >= max → Failed`
/// rule + parent cascade automatically.
async fn record_scrape_failure(queue: &JobQueue, item: &riven_db::entities::MediaItem) {
    let id = item.id;
    if let Err(err) = repo::increment_failed_attempts(&queue.db_pool, id).await {
        tracing::warn!(id, %err, "failed to increment failed_attempts");
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
            queue.clear_flow_all("scrape", job.id).await;
        }
        return;
    }

    let parent = ScrapeJob {
        id: job.id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        title: job.title.clone(),
        season: job.season,
        episode: job.episode,
        rate_limit_retries: job.rate_limit_retries,
    };
    let event = scrape_event(&parent);

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

    // Snapshot the counters and tear down everything except `flow_results`.
    // `flow_results` is owned by the next stage (parse_results) on the success
    // path; bail-out paths below clear it explicitly via `clear_flow_all`.
    let result_count = queue.flow_result_count("scrape", id).await;
    let rate_limited_count = queue.flow_rate_limited_count("scrape", id).await;
    queue.clear_flow("scrape", id).await;
    queue.clear_flow_rate_limited("scrape", id).await;

    let Some(item) = load_media_item_or_log(&queue.db_pool, id, "scrape finalize").await else {
        queue.clear_flow_all("scrape", id).await;
        return;
    };

    // Skip items in terminal/non-processable states — stale plugin jobs can
    // still drain after an item is marked Failed; without this guard they
    // would fire spurious "no streams" notifications indefinitely.
    if matches!(item.state, MediaItemState::Failed | MediaItemState::Paused) {
        tracing::debug!(id, state = ?item.state, "skipping finalize for non-processable state");
        queue.clear_flow_all("scrape", id).await;
        return;
    }

    if result_count == 0 {
        queue.clear_flow_all("scrape", id).await;

        // Every scraper deferred. Rate-limit failures don't count as scrape
        // failures; schedule a delayed retry until the per-item budget
        // (`max_scrape_attempts - failed_attempts`) is exhausted.
        if rate_limited_count > 0 {
            let max = queue.maximum_scrape_attempts.load(Ordering::Relaxed);
            let budget = rate_limit_retry_budget(max, item.failed_attempts);
            let next_attempt = job.rate_limit_retries + 1;
            if next_attempt < budget {
                let backoff = rate_limit_backoff(job.rate_limit_retries);
                tracing::warn!(
                    id,
                    rate_limited_count,
                    attempt = next_attempt,
                    budget,
                    backoff_secs = backoff.as_secs(),
                    "all scrapers deferred; scheduling delayed scrape retry"
                );
                queue
                    .push_scrape_after(
                        ScrapeJob {
                            rate_limit_retries: next_attempt,
                            ..job.clone()
                        },
                        backoff,
                    )
                    .await;
                return;
            }
            tracing::warn!(
                id,
                rate_limited_count,
                attempts = job.rate_limit_retries + 1,
                budget,
                "scrape rate-limit budget exhausted; leaving item in current state"
            );
            return;
        }

        tracing::info!(id, "no streams found by any scraper");
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
        .push_parse_scrape_results(ParseScrapeResultsJob { id })
        .await;
}

pub async fn parse_results(id: i64, _job: &ParseScrapeResultsJob, queue: &JobQueue) {
    tracing::debug!(id, "running parse-scrape-results flow");

    // Drain results unconditionally up front: this stage is the sole consumer,
    // so reading and clearing in one round-trip means every downstream bail-out
    // path can return without juggling Redis cleanup.
    let responses: Vec<ScrapeResponse> = queue.drain_flow_results("scrape", id).await;

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

    let streams = responses
        .into_iter()
        .fold(HashMap::new(), |mut acc, streams| {
            acc.extend(streams);
            acc
        });

    let ranked = tokio::task::spawn_blocking(move || rank_streams(parse_context.parse, streams))
        .await
        .unwrap_or_default();

    let new_stream_count = stream::iter(ranked)
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
                    Ok(inserted) => inserted,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to link stream to item");
                        false
                    }
                }
            }
        })
        .buffer_unordered(4)
        .filter(|inserted| futures::future::ready(*inserted))
        .count()
        .await;

    if let Err(e) = repo::update_scraped(&queue.db_pool, id).await {
        tracing::error!(error = %e, "failed to update scraped timestamp");
    }

    tracing::info!(id, new_stream_count, "parse-scrape-results completed");

    if new_stream_count == 0 {
        record_scrape_failure(queue, &item).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type,
            })
            .await;
    } else {
        // The new media_item_streams rows already triggered a state recompute;
        // resetting failed_attempts triggers another that incorporates the
        // counter.
        if let Err(err) = repo::reset_failed_attempts(&queue.db_pool, id).await {
            tracing::warn!(id, %err, "failed to reset failed_attempts");
        }
        queue
            .notify(RivenEvent::MediaItemScrapeSuccess {
                id,
                title: item_title,
                item_type,
                stream_count: new_stream_count,
            })
            .await;
    }
}
