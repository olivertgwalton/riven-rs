use riven_core::events::RivenEvent;
use riven_core::plugin::ContentCollection;
use riven_core::types::ContentServiceResponse;
use riven_db::repo;

use crate::JobQueue;
use crate::orchestrator::LibraryOrchestrator;

/// Singleton flow scope for content-service polling. Only one content-service
/// fan-in runs at a time (the scheduler's 120s tick is the sole producer plus
/// the seerr webhook), so a fixed scope is safe.
const CONTENT_SCOPE: i64 = 0;

/// Kick off the content-service flow: fan out a `ContentServiceRequested`
/// hook job to every subscribed plugin's queue. The orchestrator's `finalize`
/// runs in whichever plugin-hook worker drains the last child.
pub async fn enqueue(queue: &JobQueue) {
    if queue
        .fan_out_plugin_hook(RivenEvent::ContentServiceRequested, CONTENT_SCOPE)
        .await
        == 0
    {
        tracing::debug!("content-service flow has no subscribers");
    }
}

/// Run the content service request flow's tail: aggregate every plugin's
/// `ContentServiceResponse`, persist new items, and prune content removed
/// from upstream. Invoked from the plugin-hook worker on last-child completion.
pub async fn finalize(scope: i64, queue: &JobQueue) {
    tracing::debug!("running content service finalize");

    let responses: Vec<ContentServiceResponse> = queue.drain_flow_results("content", scope).await;
    queue.clear_flow("content", scope).await;

    let mut content = ContentCollection::default();

    for response in responses {
        for movie in response.movies {
            content.insert_movie(movie);
        }
        for show in response.shows {
            content.insert_show(show);
        }
    }

    let orchestrator = LibraryOrchestrator::new(queue);
    let response = content.into_response();
    let all_movies = response.movies;
    let all_shows = response.shows;

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
                if let Some(event) = outcome.lifecycle_event(None) {
                    queue.notify(event).await;
                }
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
                if let Some(event) = outcome.lifecycle_event(show.requested_seasons.as_deref()) {
                    queue.notify(event).await;
                }
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

    // Removed-item cleanup: only fire when at least one plugin returned
    // results. We can't tell here whether a missing response means "plugin
    // errored" or "plugin returned empty"; conservatively skip the cleanup
    // when there's nothing to scope it against.
    if !active_external_ids.is_empty() {
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

    tracing::debug!(
        count = all_movies.len() + all_shows.len(),
        "content service flow completed"
    );
}
