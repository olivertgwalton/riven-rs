use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::repo;

use crate::orchestrator::LibraryOrchestrator;
use crate::{IndexJob, IndexPluginJob, JobQueue};

use super::{load_item_or_log, run_plugin_hook, start_plugin_flow};

fn index_event(job: &IndexJob) -> RivenEvent {
    RivenEvent::MediaItemIndexRequested {
        id: job.id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        tvdb_id: job.tvdb_id.clone(),
        tmdb_id: job.tmdb_id.clone(),
    }
}

async fn load_requested_seasons(
    queue: &JobQueue,
    item: &riven_db::entities::MediaItem,
) -> Option<Vec<i32>> {
    if let Some(req_id) = item.item_request_id {
        repo::get_item_request_by_id(&queue.db_pool, req_id)
            .await
            .ok()
            .flatten()
            .and_then(|req| req.seasons)
            .and_then(|s| serde_json::from_value(s).ok())
    } else {
        None
    }
}

pub async fn run(job: &IndexJob, queue: &JobQueue) {
    let Some(_item) = load_item_or_log(job.id, &queue.db_pool, "indexing").await else {
        queue.release_dedup("index", job.id).await;
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
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id: job.id,
                error: "no indexer plugin responded".into(),
            })
            .await;
        queue.release_dedup("index", job.id).await;
        return;
    }
}

pub async fn run_plugin(job: &IndexPluginJob, queue: &JobQueue) {
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
    let Some(item) = load_item_or_log(id, &queue.db_pool, "index finalize").await else {
        queue.clear_flow("index", id).await;
        queue.release_dedup("index", id).await;
        return;
    };

    let requested_seasons = load_requested_seasons(queue, &item).await;
    let responses: Vec<IndexedMediaItem> = queue.flow_load_results("index", id).await;
    queue.clear_flow_results("index", id).await;
    queue.clear_flow("index", id).await;
    queue.release_dedup("index", id).await;

    if responses.is_empty() {
        tracing::warn!(id, "no indexer plugin responded");
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

    if let Err(e) = repo::update_media_item_index(&queue.db_pool, id, &merged).await {
        tracing::error!(id, error = %e, "failed to persist indexed data");
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id,
                error: e.to_string(),
            })
            .await;
        return;
    }

    if item.item_type == MediaItemType::Movie {
        if let Ok(Some(fresh)) = repo::get_media_item(&queue.db_pool, id).await {
            let _ = repo::refresh_state(&queue.db_pool, &fresh).await;
        }
    }

    if item.item_type == MediaItemType::Show {
        if let Some(seasons) = &merged.seasons {
            for season_data in seasons {
                let season_requested = if season_data.number == 0 {
                    requested_seasons
                        .as_ref()
                        .map(|s| s.contains(&0))
                        .unwrap_or(false)
                } else {
                    requested_seasons
                        .as_ref()
                        .map(|s| s.contains(&season_data.number))
                        .unwrap_or(true)
                };

                if requested_seasons.is_some() && !season_requested {
                    continue;
                }

                let season = match repo::create_season(
                    &queue.db_pool,
                    id,
                    season_data.number,
                    season_data.title.as_deref(),
                    season_data.tvdb_id.as_deref(),
                    season_data.number == 0,
                    item.item_request_id,
                    season_requested,
                )
                .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, season = season_data.number, "failed to create season");
                        continue;
                    }
                };

                for ep_data in &season_data.episodes {
                    if let Err(e) = repo::create_episode(
                        &queue.db_pool,
                        season.id,
                        ep_data.number,
                        ep_data.title.as_deref(),
                        ep_data.tvdb_id.as_deref(),
                        ep_data.aired_at,
                        ep_data.runtime,
                        ep_data.absolute_number,
                        item.item_request_id,
                        season_requested,
                        Some(season_data.number),
                    )
                    .await
                    {
                        tracing::error!(error = %e, episode = ep_data.number, "failed to create episode");
                    }
                }

                let _ = repo::refresh_state(&queue.db_pool, &season).await;
            }
        }

        if let Ok(Some(show_item)) = repo::get_media_item(&queue.db_pool, id).await {
            let _ = repo::refresh_state(&queue.db_pool, &show_item).await;
        }
    }

    let fresh_item = match repo::get_media_item(&queue.db_pool, id).await {
        Ok(Some(i)) => i,
        _ => {
            tracing::error!(id, "could not re-fetch item after indexing");
            return;
        }
    };

    let title = merged.title.clone().unwrap_or_else(|| item.title.clone());
    queue
        .notify(RivenEvent::MediaItemIndexSuccess {
            id,
            title: title.clone(),
            item_type: item.item_type,
        })
        .await;
    tracing::info!(id, "index flow completed");

    LibraryOrchestrator::new(queue)
        .enqueue_after_index(&fresh_item, requested_seasons.as_deref())
        .await;
}
