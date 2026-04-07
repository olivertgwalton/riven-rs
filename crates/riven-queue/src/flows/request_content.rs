use riven_core::events::{HookResponse, RivenEvent};
use riven_core::plugin::ContentCollection;
use riven_db::repo;

use crate::JobQueue;
use crate::orchestrator::LibraryOrchestrator;

/// Run the content service request flow.
/// Dispatches to all content service plugins, aggregates results, and persists new items.
pub async fn run(queue: &JobQueue) {
    tracing::debug!("running content service request flow");

    let event = RivenEvent::ContentServiceRequested;
    let results = queue.registry.dispatch(&event).await;

    let mut content = ContentCollection::default();
    let mut any_plugin_errored = false;

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
                    content.insert_movie(movie);
                }

                for show in response.shows {
                    content.insert_show(show);
                }
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(
                    plugin = plugin_name,
                    error = %error,
                    "content service hook failed"
                );
                any_plugin_errored = true;
            }
        }
    }

    let orchestrator = LibraryOrchestrator::new(queue);
    let response = content.into_response();
    let all_movies = response.movies;
    let all_shows = response.shows;

    let mut new_items = 0usize;
    let mut updated_items = 0usize;

    for movie in &all_movies {
        let title = movie
            .imdb_id
            .as_deref()
            .or(movie.tmdb_id.as_deref())
            .unwrap_or("Unknown");

        match orchestrator
            .upsert_requested_movie(
                title,
                movie.imdb_id.as_deref(),
                movie.tmdb_id.as_deref(),
                movie.requested_by.as_deref(),
                movie.external_request_id.as_deref(),
            )
            .await
        {
            Ok(outcome) => {
                match outcome.action {
                    repo::ItemRequestUpsertAction::Created => new_items += 1,
                    repo::ItemRequestUpsertAction::Updated => updated_items += 1,
                    repo::ItemRequestUpsertAction::Unchanged => {}
                }

                orchestrator.enqueue_after_request(&outcome, None).await;
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to upsert requested movie");
            }
        }
    }

    for show in &all_shows {
        let title = show
            .imdb_id
            .as_deref()
            .or(show.tvdb_id.as_deref())
            .unwrap_or("Unknown");

        match orchestrator
            .upsert_requested_show(
                title,
                show.imdb_id.as_deref(),
                show.tvdb_id.as_deref(),
                show.requested_by.as_deref(),
                show.external_request_id.as_deref(),
                show.requested_seasons.as_deref(),
            )
            .await
        {
            Ok(outcome) => {
                match outcome.action {
                    repo::ItemRequestUpsertAction::Created => new_items += 1,
                    repo::ItemRequestUpsertAction::Updated => updated_items += 1,
                    repo::ItemRequestUpsertAction::Unchanged => {}
                }

                orchestrator
                    .enqueue_after_request(&outcome, show.requested_seasons.as_deref())
                    .await;
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to upsert requested show");
            }
        }
    }

    let active_external_ids: Vec<String> = all_movies
        .iter()
        .filter_map(|movie| movie.external_request_id.clone())
        .chain(
            all_shows
                .iter()
                .filter_map(|show| show.external_request_id.clone()),
        )
        .collect();

    if !active_external_ids.is_empty() && !any_plugin_errored {
        match repo::delete_items_removed_from_content_services(&queue.db_pool, &active_external_ids)
            .await
        {
            Ok(count) if count > 0 => {
                tracing::info!(count, "deleted items removed from content services")
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(error = %error, "failed to clean up removed content service items")
            }
        }
    }

    let count = all_movies.len() + all_shows.len();

    if new_items > 0 || updated_items > 0 {
        queue
            .notify(RivenEvent::ItemRequestCreateSuccess {
                count,
                new_items,
                updated_items,
            })
            .await;
    }

    tracing::debug!(
        count,
        new_items,
        updated_items,
        "content service flow completed"
    );
}
