use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::repo;

use crate::JobQueue;
use crate::ScrapeJob;

use super::load_item_or_log;

/// Run the index item flow.
/// Dispatches to indexer plugins (TMDB, TVDB), merges results, and persists.
pub async fn run(id: i64, queue: &JobQueue) {
    let Some(item) = load_item_or_log(id, &queue.db_pool, "indexing").await else {
        return;
    };

    let event = RivenEvent::MediaItemIndexRequested {
        id: item.id,
        item_type: item.item_type,
        imdb_id: item.imdb_id.clone(),
        tvdb_id: item.tvdb_id.clone(),
        tmdb_id: item.tmdb_id.clone(),
    };

    let results = queue.registry.dispatch(&event).await;

    let mut merged = IndexedMediaItem::default();
    let mut got_response = false;

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::Index(indexed)) => {
                tracing::debug!(plugin = plugin_name, id, "indexer responded");
                got_response = true;
                merged = merged.merge(*indexed);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(plugin = plugin_name, id, error = %e, "indexer hook failed");
            }
        }
    }

    if !got_response {
        tracing::warn!(id, "no indexer plugin responded — item will stay at Indexed and be retried");
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id,
                error: "no indexer plugin responded".into(),
            })
            .await;
        return;
    }

    // Persist indexed data
    if let Err(e) = repo::update_media_item_index(&queue.db_pool, id, &merged).await {
        tracing::error!(id, error = %e, "failed to persist indexed data — item will stay at Indexed");
        queue
            .notify(RivenEvent::MediaItemIndexError {
                id,
                error: e.to_string(),
            })
            .await;
        return;
    }

    // For movies, check if the release date is in the future and mark Unreleased.
    if item.item_type == MediaItemType::Movie {
        if let Some(aired_at) = merged.aired_at {
            if aired_at > chrono::Utc::now().date_naive() {
                let _ =
                    repo::update_media_item_state(&queue.db_pool, id, MediaItemState::Unreleased)
                        .await;
            }
        }
    }

    // For shows, create seasons and episodes
    if item.item_type == MediaItemType::Show {
        if let Some(seasons) = &merged.seasons {
            let requested_seasons: Option<Vec<i32>> = if let Some(req_id) = item.item_request_id {
                repo::get_item_request_by_id(&queue.db_pool, req_id)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|req| req.seasons)
                    .and_then(|s| serde_json::from_value(s).ok())
            } else {
                None
            };

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

                if let Ok(season_state) = repo::compute_state(&queue.db_pool, &season).await {
                    if season_state != season.state {
                        let _ = repo::update_media_item_state(&queue.db_pool, season.id, season_state).await;
                    }
                }
            }
        }

        if let Ok(Some(show_item)) = repo::get_media_item(&queue.db_pool, id).await {
            if let Ok(show_state) = repo::compute_state(&queue.db_pool, &show_item).await {
                if show_state != show_item.state {
                    let _ =
                        repo::update_media_item_state(&queue.db_pool, id, show_state).await;
                }
            }
        }
    }

    let title = merged.title.clone().unwrap_or_else(|| item.title.clone());
    queue
        .notify(RivenEvent::MediaItemIndexSuccess {
            id,
            title: title.clone(),
            item_type: item.item_type,
        })
        .await;
    tracing::info!(id, "index flow completed");

    // Re-fetch the item from the DB so all ScrapeJob fields (especially imdb_id
    // and title) reflect what the indexer wrote — mirroring riven-ts where
    // persistMovieIndexerData / persistShowIndexerData creates the entity with
    // full data before requestScrape is triggered.
    let fresh_item = match repo::get_media_item(&queue.db_pool, id).await {
        Ok(Some(i)) => i,
        _ => {
            tracing::error!(id, "could not re-fetch item after indexing");
            return;
        }
    };

    // Immediately queue scraping after successful indexing
    match fresh_item.item_type {
        MediaItemType::Movie => {
            queue
                .push_scrape(ScrapeJob {
                    id: fresh_item.id,
                    item_type: fresh_item.item_type,
                    imdb_id: fresh_item.imdb_id.clone(),
                    title: fresh_item.title.clone(),
                    season: fresh_item.season_number,
                    episode: fresh_item.episode_number,
                })
                .await;
        }
        MediaItemType::Episode => {
            let show_imdb_id = if let Some(parent_id) = fresh_item.parent_id {
                if let Ok(Some(season)) = repo::get_media_item(&queue.db_pool, parent_id).await {
                    if let Some(show_id) = season.parent_id {
                        repo::get_media_item(&queue.db_pool, show_id)
                            .await
                            .ok()
                            .flatten()
                            .and_then(|s| s.imdb_id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            queue
                .push_scrape(ScrapeJob {
                    id: fresh_item.id,
                    item_type: fresh_item.item_type,
                    imdb_id: show_imdb_id.or(fresh_item.imdb_id.clone()),
                    title: title.clone(),
                    season: fresh_item.season_number,
                    episode: fresh_item.episode_number,
                })
                .await;
        }
        MediaItemType::Show => {
            let show_imdb_id = fresh_item.imdb_id.clone();
            if let Ok(seasons) =
                riven_db::repo::get_requested_seasons_for_show(&queue.db_pool, fresh_item.id).await
            {
                for season in seasons {
                    queue
                        .push_scrape(ScrapeJob {
                            id: season.id,
                            item_type: season.item_type,
                            imdb_id: show_imdb_id.clone(),
                            title: title.clone(),
                            season: season.season_number,
                            episode: None,
                        })
                        .await;
                }
            }
        }
        _ => {}
    }
}
