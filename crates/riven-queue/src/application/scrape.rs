use std::collections::HashMap;

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
        queue.release_dedup("scrape", id).await;
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
        queue.release_dedup("scrape", id).await;
        return;
    }

    let _ = repo::clear_blacklisted_streams(&queue.db_pool, id).await;

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
                })
                .await;
        },
    )
    .await
        == 0
    {
        finalize(id, &job.title, job.auto_download, queue).await;
    }
}

pub async fn handle_plugin(job: &ScrapePluginJob, queue: &JobQueue) {
    let event = scrape_event(&ScrapeJob {
        id: job.id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        title: job.title.clone(),
        season: job.season,
        episode: job.episode,
        auto_download: job.auto_download,
    });

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
        finalize(job.id, &job.title, job.auto_download, queue).await;
    }
}

pub async fn finalize(id: i64, requested_title: &str, auto_download: bool, queue: &JobQueue) {
    let Some(item) = load_media_item_or_log(&queue.db_pool, id, "scrape finalize").await else {
        queue.clear_flow_results("scrape", id).await;
        queue.clear_flow("scrape", id).await;
        queue.release_dedup("scrape", id).await;
        return;
    };

    queue.clear_flow("scrape", id).await;
    queue.release_dedup("scrape", id).await;
    if queue.flow_result_count("scrape", id).await == 0 {
        queue.clear_flow_results("scrape", id).await;
        tracing::info!(id, "no streams found by any scraper");
        let item_title = if item.item_type == MediaItemType::Season {
            format!("{requested_title} - {}", item.title)
        } else {
            item.title.clone()
        };
        let _ = repo::increment_failed_attempts(&queue.db_pool, id).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type: item.item_type,
            })
            .await;
        retry_existing_download_if_scraped(&item, auto_download, queue).await;
        return;
    }

    let result_count = queue.flow_result_count("scrape", id).await;
    tracing::debug!(id, count = result_count, "pushing parse-scrape-results job");
    queue
        .push_parse_scrape_results(ParseScrapeResultsJob { id, auto_download })
        .await;
}

pub async fn parse_results(id: i64, job: &ParseScrapeResultsJob, queue: &JobQueue) {
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

    if let Err(e) = repo::refresh_state_cascade(&queue.db_pool, &item).await {
        tracing::error!(error = %e, "failed to refresh state after scrape");
    }

    tracing::info!(id, stream_count, "parse-scrape-results completed");

    if stream_count == 0 {
        let _ = repo::increment_failed_attempts(&queue.db_pool, id).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type,
            })
            .await;
        retry_existing_download_if_scraped(&item, job.auto_download, queue).await;
    } else {
        let _ = repo::reset_failed_attempts(&queue.db_pool, id).await;
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

async fn retry_existing_download_if_scraped(
    item: &riven_db::entities::MediaItem,
    auto_download: bool,
    queue: &JobQueue,
) {
    if !auto_download {
        return;
    }

    if matches!(
        item.state,
        MediaItemState::Scraped | MediaItemState::PartiallyCompleted
    ) {
        queue.push_download_from_best_stream(item.id).await;
    }
}
