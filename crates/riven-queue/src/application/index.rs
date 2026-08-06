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
    let id = job.id;
    let Some(item) = load_media_item_or_log(id, "indexing").await else {
        return;
    };

    tracing::debug!(
        id,
        title = %item.title,
        item_type = ?job.item_type,
        imdb_id = job.imdb_id.as_deref().unwrap_or("-"),
        tmdb_id = job.tmdb_id.as_deref().unwrap_or("-"),
        "index: asking the metadata providers to describe this item"
    );
    let outcomes = queue.fan_out_and_collect(&index_event(job)).await;
    if outcomes.is_empty() {
        tracing::warn!(
            id,
            title = %item.title,
            "index: no metadata provider is enabled, so this item cannot be indexed"
        );
    }
    let responses: Vec<IndexedMediaItem> = crate::dispatch::decode_hook_responses(outcomes);

    // Reload: the indexers took wall-clock time and the item may have moved.
    let Some(item) = load_media_item_or_log(id, "index finalize").await else {
        return;
    };
    let requested_seasons = load_requested_seasons(&item).await;

    if responses.is_empty() {
        tracing::warn!(
            id,
            title = %item.title,
            imdb_id = item.imdb_id.as_deref().unwrap_or("-"),
            tmdb_id = item.tmdb_id.as_deref().unwrap_or("-"),
            "index: no metadata provider recognised this item (usually a missing or wrong external id); retrying in 24h"
        );
        if let Err(err) = repo::increment_failed_attempts(id).await {
            tracing::warn!(
                id,
                %err,
                "index: could not record the failed attempt, so this item's retry backoff will not grow"
            );
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

    if let Err(e) = apply_indexed_media_item(&item, &merged, requested_seasons.as_deref()).await {
        tracing::error!(
            id,
            title = %item.title,
            error = %e,
            "index: metadata was fetched but could not be saved; the item stays un-indexed"
        );
        if let Err(err) = repo::increment_failed_attempts(id).await {
            tracing::warn!(
                id,
                %err,
                "index: could not record the failed attempt, so this item's retry backoff will not grow"
            );
        }
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id,
                error: e.to_string(),
            })
            .await;
        return;
    }

    let fresh = match riven_db::repo::get_media_item(id).await {
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
    tracing::info!(
        id,
        title = %title,
        item_type = ?fresh.item_type,
        "index: metadata saved, the item is ready to scrape"
    );
}
