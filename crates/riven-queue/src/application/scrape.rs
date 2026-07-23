use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::stream::{self, StreamExt};

use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::repo;

use crate::context::{
    build_parse_item_context_with_hierarchy, is_scrapeable, load_media_item_hierarchy_or_log,
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

/// Bump `failed_attempts`. `increment_failed_attempts` recomputes state —
/// applying the `failed_attempts >= max → Failed` rule + parent cascade.
async fn record_scrape_failure(item: &riven_db::entities::MediaItem) {
    let id = item.id;
    if let Err(err) = repo::increment_failed_attempts(id).await {
        tracing::warn!(
            id,
            title = %item.title,
            %err,
            "scrape: could not record the failed attempt, so this item's retry backoff will not grow"
        );
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
    let Some(item) = load_media_item_or_log(id, "scrape").await else {
        return;
    };

    tracing::debug!(
        id,
        title = %item.title,
        state = ?item.state,
        "scrape: asking every enabled scraper for streams"
    );

    if !is_scrapeable(item.state) {
        tracing::debug!(
            id,
            title = %item.title,
            state = ?item.state,
            "scrape: skipped, item is not in a scrapeable state (needs Indexed/Ongoing/Scraped/PartiallyCompleted)"
        );
        return;
    }

    if item.indexed_at.is_none()
        && matches!(item.item_type, MediaItemType::Movie | MediaItemType::Show)
    {
        tracing::warn!(
            id,
            title = %item.title,
            item_type = ?item.item_type,
            "scrape: item was never indexed, so there is no metadata to scrape with; indexing it first"
        );
        queue.push_index(IndexJob::from_item(&item)).await;
        return;
    }

    if let Err(err) = repo::clear_blacklisted_streams(id).await {
        tracing::warn!(
            id,
            title = %item.title,
            %err,
            "scrape: could not clear previously blacklisted streams; they stay excluded this run"
        );
    }

    queue.flow_set_context("scrape", id, job).await;

    let scrapers = queue.fan_out_plugin_hook(scrape_event(job), id).await;
    if scrapers == 0 {
        tracing::warn!(
            id,
            title = %item.title,
            "scrape: no scraper plugins are enabled, so nothing can be found"
        );
        finalize(id, queue).await;
        return;
    }

    tracing::debug!(
        id,
        title = %item.title,
        scrapers,
        "scrape: request sent to the scrapers, waiting for all of them to answer"
    );
}

pub async fn finalize(id: i64, queue: &JobQueue) {
    let Some(job) = queue.flow_get_context::<ScrapeJob>("scrape", id).await else {
        tracing::warn!(
            id,
            "scrape: results arrived after the run was already cleaned up (duplicate or expired scrape); discarding them"
        );
        queue.clear_flow_all("scrape", id).await;
        return;
    };

    let result_count = queue.flow_result_count("scrape", id).await;
    let rate_limited_count = queue.flow_rate_limited_count("scrape", id).await;
    queue.clear_flow("scrape", id).await;
    queue.clear_flow_rate_limited("scrape", id).await;
    queue.flow_clear_context("scrape", id).await;

    let Some(item) = load_media_item_or_log(id, "scrape finalize").await else {
        queue.clear_flow_all("scrape", id).await;
        return;
    };

    if matches!(
        item.state,
        MediaItemState::Failed | MediaItemState::Paused | MediaItemState::Completed
    ) {
        tracing::debug!(
            id,
            title = %item.title,
            state = ?item.state,
            "scrape: discarding results, the item reached a final state while the scrapers were running"
        );
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
                    title = %item.title,
                    rate_limited_scrapers = rate_limited_count,
                    attempt = next_attempt,
                    budget,
                    retry_in_secs = backoff.as_secs(),
                    "scrape: every scraper is rate-limited right now; retrying later"
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
                title = %item.title,
                rate_limited_scrapers = rate_limited_count,
                attempts = job.rate_limit_retries + 1,
                budget,
                "scrape: giving up on rate-limited retries for now; the item keeps its current state and waits for the next library pass"
            );
            return;
        }

        tracing::info!(
            id,
            title = %item.title,
            "scrape: no scraper returned any stream for this item"
        );
        let item_title = if item.item_type == MediaItemType::Season {
            format!("{} - {}", job.title, item.title)
        } else {
            item.title.clone()
        };
        record_scrape_failure(&item).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type: item.item_type,
            })
            .await;
        return;
    }

    tracing::debug!(
        id,
        title = %item.title,
        scrapers_with_results = result_count,
        "scrape: got results, queueing them for parsing and ranking"
    );
    queue
        .push_parse_scrape_results(ParseScrapeResultsJob { id })
        .await;
}

pub async fn parse_results(id: i64, _job: &ParseScrapeResultsJob, queue: &JobQueue) {
    let responses: Vec<ScrapeResponse> = queue.drain_flow_results("scrape", id).await;

    let Some(item) = load_media_item_or_log(id, "parse-scrape-results").await else {
        return;
    };

    tracing::debug!(
        id,
        title = %item.title,
        scrapers_with_results = responses.len(),
        "parse: parsing and ranking the scraper results"
    );

    let processable = is_scrapeable(item.state);
    if !processable {
        tracing::debug!(
            id,
            title = %item.title,
            state = ?item.state,
            "parse: dropping the results, the item moved out of a scrapeable state while they were being collected"
        );
        queue
            .notify(RivenEvent::MediaItemScrapeErrorIncorrectState { id })
            .await;
        return;
    }

    let hierarchy =
        load_media_item_hierarchy_or_log(item.id, "parse-scrape-results hierarchy").await;
    let parse_context = build_parse_item_context_with_hierarchy(item, hierarchy.as_ref()).await;
    let item = parse_context.item;
    let item_title = parse_context.item_title;
    let item_type = parse_context.item_type;

    let streams = responses
        .into_iter()
        .fold(HashMap::new(), |mut acc, streams| {
            acc.extend(streams);
            acc
        });
    let scraped_count = streams.len();

    let ranked = tokio::task::spawn_blocking(move || rank_streams(parse_context.parse, streams))
        .await
        .unwrap_or_default();

    tracing::debug!(
        id,
        title = %item_title,
        scraped = scraped_count,
        accepted = ranked.len(),
        "parse: filtered the scraped streams down to the ones matching this item and the active profiles"
    );

    let new_stream_count = stream::iter(ranked)
        .map(|candidate| {
            async move {
                let stream = match repo::upsert_stream(
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
                        tracing::error!(
                            id,
                            error = %e,
                            info_hash = %candidate.info_hash,
                            release = %candidate.title,
                            "parse: could not save this stream, dropping it from the candidates"
                        );
                        return false;
                    }
                };
                match repo::link_stream_to_item(id, stream.id).await {
                    Ok(inserted) => inserted,
                    Err(e) => {
                        tracing::error!(
                            id,
                            error = %e,
                            info_hash = %candidate.info_hash,
                            release = %candidate.title,
                            "parse: saved the stream but could not attach it to the item, so it will not be downloaded"
                        );
                        false
                    }
                }
            }
        })
        .buffer_unordered(4)
        .filter(|inserted| futures::future::ready(*inserted))
        .count()
        .await;

    if let Err(e) = repo::update_scraped(id).await {
        tracing::error!(
            id,
            error = %e,
            "parse: could not mark the item as scraped, so it may be scraped again on the next pass"
        );
    }

    if new_stream_count == 0 {
        tracing::info!(
            id,
            title = %item_title,
            "parse: no new streams, everything the scrapers returned was already known or rejected"
        );
        record_scrape_failure(&item).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type,
            })
            .await;
    } else {
        tracing::info!(
            id,
            title = %item_title,
            new_streams = new_stream_count,
            "parse: new streams stored and ready to download"
        );
        if let Err(err) = repo::reset_failed_attempts(id).await {
            tracing::warn!(
                id,
                %err,
                "parse: could not reset the failed-attempt counter, so this item keeps its longer retry backoff"
            );
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
