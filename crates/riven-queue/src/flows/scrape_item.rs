use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::repo;

use crate::orchestrator::LibraryOrchestrator;

use super::{load_item_or_log, run_plugin_hook, start_plugin_flow};
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

pub async fn run(id: i64, job: &ScrapeJob, queue: &JobQueue) {
    tracing::debug!(id, "running scrape flow");
    let Some(item) = load_item_or_log(id, &queue.db_pool, "scrape").await else {
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
        return;
    }
}

pub async fn run_plugin(job: &ScrapePluginJob, queue: &JobQueue) {
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
    let Some(item) = load_item_or_log(id, &queue.db_pool, "scrape finalize").await else {
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
        LibraryOrchestrator::new(queue)
            .fan_out_download_failure(id)
            .await;
        return;
    }

    let result_count = queue.flow_result_count("scrape", id).await;
    tracing::debug!(id, count = result_count, "pushing parse-scrape-results job");
    queue
        .push_parse_scrape_results(ParseScrapeResultsJob { id, auto_download })
        .await;
}
