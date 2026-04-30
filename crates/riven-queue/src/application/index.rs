use chrono::{Duration, Utc};
use riven_core::events::RivenEvent;
use riven_core::types::*;

use riven_db::repo;

use crate::context::{load_media_item_or_log, load_requested_seasons};
use crate::indexing::apply_indexed_media_item;
use crate::{IndexJob, JobQueue};

fn index_event(job: &IndexJob) -> RivenEvent {
    RivenEvent::MediaItemIndexRequested {
        id: job.id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        tvdb_id: job.tvdb_id.clone(),
        tmdb_id: job.tmdb_id.clone(),
    }
}

pub async fn start(job: &IndexJob, queue: &JobQueue) {
    if load_media_item_or_log(&queue.db_pool, job.id, "indexing")
        .await
        .is_none()
    {
        return;
    }

    if queue.fan_out_plugin_hook(index_event(job), job.id).await == 0 {
        // No indexer subscribed — finalize will see zero results and reschedule.
        finalize(job.id, queue).await;
    }
}

pub async fn finalize(id: i64, queue: &JobQueue) {
    let Some(item) = load_media_item_or_log(&queue.db_pool, id, "index finalize").await else {
        queue.clear_flow_all("index", id).await;
        return;
    };

    let requested_seasons = load_requested_seasons(&queue.db_pool, &item).await;
    let responses: Vec<IndexedMediaItem> = queue.drain_flow_results("index", id).await;
    queue.clear_flow("index", id).await;

    if responses.is_empty() {
        tracing::warn!(id, "no indexer plugin responded; retrying in 24h");
        if let Err(err) = repo::increment_failed_attempts(&queue.db_pool, id).await {
            tracing::warn!(id, %err, "failed to increment failed_attempts");
        }
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id,
                error: "no indexer plugin responded".into(),
            })
            .await;
        queue
            .schedule_index_at(IndexJob::from_item(&item), Utc::now() + Duration::hours(24))
            .await;
        return;
    }

    let merged = responses
        .into_iter()
        .fold(IndexedMediaItem::default(), |acc, indexed| {
            acc.merge(indexed)
        });

    if let Err(e) =
        apply_indexed_media_item(&queue.db_pool, &item, &merged, requested_seasons.as_deref())
            .await
    {
        tracing::error!(id, error = %e, "failed to persist indexed data");
        if let Err(err) = repo::increment_failed_attempts(&queue.db_pool, id).await {
            tracing::warn!(id, %err, "failed to increment failed_attempts");
        }
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id,
                error: e.to_string(),
            })
            .await;
        return;
    }

    let fresh = match riven_db::repo::get_media_item(&queue.db_pool, id).await {
        Ok(Some(item)) => item,
        _ => item,
    };

    let title = merged.title.clone().unwrap_or_else(|| fresh.title.clone());
    queue
        .notify(RivenEvent::MediaItemIndexSuccess {
            id,
            title: title.clone(),
            item_type: fresh.item_type,
        })
        .await;
    tracing::info!(id, "index flow completed");
}
