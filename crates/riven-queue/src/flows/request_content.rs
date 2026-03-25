use std::collections::HashSet;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::repo;

use crate::{IndexJob, JobQueue};

/// Run the content service request flow.
/// Dispatches to all content service plugins, aggregates results, and persists new items.
pub async fn run(queue: &JobQueue) {
    tracing::debug!("running content service request flow");

    let event = RivenEvent::ContentServiceRequested;
    let results = queue.registry.dispatch(&event).await;

    let mut all_movies = Vec::new();
    let mut all_shows = Vec::new();
    let mut seen_movie_ids = HashSet::new();
    let mut seen_show_ids = HashSet::new();

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::ContentService(response)) => {
                tracing::debug!(
                    plugin = plugin_name,
                    movies = response.movies.len(),
                    shows = response.shows.len(),
                    "content service responded"
                );

                for movie in response.movies {
                    if seen_movie_ids.insert(movie.movie_key()) {
                        all_movies.push(movie);
                    }
                }

                for show in response.shows {
                    if seen_show_ids.insert(show.show_key()) {
                        all_shows.push(show);
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(plugin = plugin_name, error = %e, "content service hook failed");
            }
        }
    }

    let mut new_items = 0usize;
    let mut updated_items = 0usize;

    // Persist movies
    for movie in &all_movies {
        let title = movie
            .imdb_id
            .as_deref()
            .or(movie.tmdb_id.as_deref())
            .unwrap_or("Unknown");

        let request = match repo::create_item_request(
            &queue.db_pool,
            movie.imdb_id.as_deref(),
            movie.tmdb_id.as_deref(),
            None,
            ItemRequestType::Movie,
            movie.requested_by.as_deref(),
            movie.external_request_id.as_deref(),
            None,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "failed to create item request for movie");
                continue;
            }
        };

        match repo::create_movie(
            &queue.db_pool,
            title,
            movie.imdb_id.as_deref(),
            movie.tmdb_id.as_deref(),
            Some(request.id),
        )
        .await
        {
            Ok((item, was_created)) => {
                if was_created {
                    new_items += 1;
                    queue
                        .push_index(IndexJob {
                            id: item.id,
                            item_type: MediaItemType::Movie,
                            imdb_id: item.imdb_id.clone(),
                            tvdb_id: None,
                            tmdb_id: item.tmdb_id.clone(),
                        })
                        .await;
                } else {
                    updated_items += 1;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to create movie");
            }
        }
    }

    // Persist shows
    for show in &all_shows {
        let title = show
            .imdb_id
            .as_deref()
            .or(show.tvdb_id.as_deref())
            .unwrap_or("Unknown");

        let request = match repo::create_item_request(
            &queue.db_pool,
            show.imdb_id.as_deref(),
            None,
            show.tvdb_id.as_deref(),
            ItemRequestType::Show,
            show.requested_by.as_deref(),
            show.external_request_id.as_deref(),
            show.requested_seasons.as_deref(),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "failed to create item request for show");
                continue;
            }
        };

        match repo::create_show(
            &queue.db_pool,
            title,
            show.imdb_id.as_deref(),
            show.tvdb_id.as_deref(),
            Some(request.id),
        )
        .await
        {
            Ok((item, was_created)) => {
                if was_created {
                    new_items += 1;
                    queue
                        .push_index(IndexJob {
                            id: item.id,
                            item_type: MediaItemType::Show,
                            imdb_id: item.imdb_id.clone(),
                            tvdb_id: item.tvdb_id.clone(),
                            tmdb_id: None,
                        })
                        .await;
                } else {
                    updated_items += 1;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to create show");
            }
        }
    }

    // Delete items no longer present in any content service.
    let active_external_ids: Vec<String> = all_movies
        .iter()
        .filter_map(|m| m.external_request_id.clone())
        .chain(all_shows.iter().filter_map(|s| s.external_request_id.clone()))
        .collect();

    if !active_external_ids.is_empty() {
        match repo::delete_items_removed_from_content_services(
            &queue.db_pool,
            &active_external_ids,
        )
        .await
        {
            Ok(n) if n > 0 => {
                tracing::info!(count = n, "deleted items removed from content services")
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, "failed to clean up removed content service items")
            }
        }
    }

    let count = all_movies.len() + all_shows.len();

    if new_items > 0 {
        queue
            .notify(RivenEvent::ItemRequestCreateSuccess {
                count,
                new_items,
                updated_items,
            })
            .await;
    }

    tracing::debug!(count, new_items, updated_items, "content service flow completed");
}
