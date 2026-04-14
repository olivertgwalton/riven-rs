use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::types::*;

use riven_db::repo;

use crate::context::{load_media_item_or_log, load_requested_seasons};
use crate::flows::{run_plugin_hook, start_plugin_flow};
use crate::indexing::apply_indexed_media_item;
use crate::{IndexJob, IndexPluginJob, JobQueue};

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
    let Some(_item) = load_media_item_or_log(&queue.db_pool, job.id, "indexing").await else {
        return;
    };

    if start_plugin_flow(
        queue,
        "index",
        job.id,
        EventType::MediaItemIndexRequested,
        |plugin_name| async move {
            queue
                .push_index_plugin(IndexPluginJob {
                    id: job.id,
                    plugin_name,
                    item_type: job.item_type,
                    imdb_id: job.imdb_id.clone(),
                    tvdb_id: job.tvdb_id.clone(),
                    tmdb_id: job.tmdb_id.clone(),
                })
                .await;
        },
    )
    .await
        == 0
    {
        tracing::warn!(id = job.id, "no indexer subscribers found");
        let _ = repo::increment_failed_attempts(&queue.db_pool, job.id).await;
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id: job.id,
                error: "no indexer plugin responded".into(),
            })
            .await;
    }
}

pub async fn handle_plugin(job: &IndexPluginJob, queue: &JobQueue) {
    // Guard against items deleted while this job was waiting in the queue.
    if load_media_item_or_log(&queue.db_pool, job.id, "index-plugin")
        .await
        .is_none()
    {
        if queue.flow_complete_child("index", job.id).await {
            queue.clear_flow("index", job.id).await;
            queue.clear_flow_results("index", job.id).await;
        }
        return;
    }

    let event = index_event(&IndexJob {
        id: job.id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        tvdb_id: job.tvdb_id.clone(),
        tmdb_id: job.tmdb_id.clone(),
    });

    if run_plugin_hook(
        queue,
        "index",
        job.id,
        &job.plugin_name,
        &event,
        "indexer",
        |response| match response {
            HookResponse::Index(indexed) => Some(*indexed),
            _ => None,
        },
    )
    .await
    {
        finalize(job.id, queue).await;
    }
}

pub async fn finalize(id: i64, queue: &JobQueue) {
    let Some(item) = load_media_item_or_log(&queue.db_pool, id, "index finalize").await else {
        queue.clear_flow("index", id).await;
        return;
    };

    let requested_seasons = load_requested_seasons(&queue.db_pool, &item).await;
    let responses: Vec<IndexedMediaItem> = queue.flow_load_results("index", id).await;
    queue.clear_flow_results("index", id).await;
    queue.clear_flow("index", id).await;

    if responses.is_empty() {
        tracing::warn!(id, "no indexer plugin responded");
        let _ = repo::increment_failed_attempts(&queue.db_pool, id).await;
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id,
                error: "no indexer plugin responded".into(),
            })
            .await;
        return;
    }

    let merged = responses
        .into_iter()
        .fold(IndexedMediaItem::default(), |acc, indexed| {
            acc.merge(indexed)
        });

    if let Err(e) =
        apply_indexed_media_item(&queue.db_pool, &item, &merged, requested_seasons.as_deref()).await
    {
        tracing::error!(id, error = %e, "failed to persist indexed data");
        let _ = repo::increment_failed_attempts(&queue.db_pool, id).await;
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

    let _ = queue.indexed_tx.send(fresh);
    tracing::info!(id, "index flow completed");
}
