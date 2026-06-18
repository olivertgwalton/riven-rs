use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::stream::{self, StreamExt};

use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::repo;

use crate::context::{
    build_parse_item_context_with_hierarchy, load_media_item_hierarchy_or_log,
    load_media_item_or_log,
};
use crate::discovery::rank_streams;
use crate::{IndexJob, JobQueue, ParseScrapeResultsJob, ScrapeJob};

fn rate_limit_backoff(prior_retries: u32) -> Duration {
    let secs = match prior_retries {
        0 => 2 * 60,
        1 => 5 * 60,
        2 => 15 * 60,
        3 | 4 => 60 * 60,
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
    max.saturating_sub(failed_attempts.max(0).cast_unsigned())
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
        tvdb_id: job.tvdb_id.clone(),
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
    ) {
        tracing::debug!(id, state = ?item.state, "skipping scrape");
        return;
    }

    if item.indexed_at.is_none()
        && matches!(item.item_type, MediaItemType::Movie | MediaItemType::Show)
    {
        tracing::warn!(
            id,
            item_type = ?item.item_type,
            "scrape requested for never-indexed item; enqueuing index instead of scraping blind"
        );
        queue.push_index(IndexJob::from_item(&item)).await;
        return;
    }

    if let Err(err) = repo::clear_blacklisted_streams(&queue.db_pool, id).await {
        tracing::warn!(id, %err, "failed to clear blacklisted streams");
    }

    queue.flow_set_context("scrape", id, job).await;

    if queue.fan_out_plugin_hook(scrape_event(job), id).await == 0 {
        finalize(id, queue).await;
    }
}

pub async fn finalize(id: i64, queue: &JobQueue) {
    let Some(job) = queue.flow_get_context::<ScrapeJob>("scrape", id).await else {
        tracing::warn!(id, "scrape finalize: missing flow context, clearing flow");
        queue.clear_flow_all("scrape", id).await;
        return;
    };

    let result_count = queue.flow_result_count("scrape", id).await;
    let rate_limited_count = queue.flow_rate_limited_count("scrape", id).await;
    queue.clear_flow("scrape", id).await;
    queue.clear_flow_rate_limited("scrape", id).await;
    queue.flow_clear_context("scrape", id).await;

    let Some(item) = load_media_item_or_log(&queue.db_pool, id, "scrape finalize").await else {
        queue.clear_flow_all("scrape", id).await;
        return;
    };

    if matches!(
        item.state,
        MediaItemState::Failed | MediaItemState::Paused | MediaItemState::Completed
    ) {
        tracing::debug!(id, state = ?item.state, "skipping finalize for non-processable state");
        queue.clear_flow_all("scrape", id).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorIncorrectState { id })
            .await;
        return;
    }

    if result_count == 0 {
        queue.clear_flow_all("scrape", id).await;

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
        queue
            .notify(RivenEvent::MediaItemScrapeErrorIncorrectState { id })
            .await;
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
