use riven_core::events::RivenEvent;
use riven_core::plugin::ContentCollection;
use riven_core::types::ContentServiceResponse;
use riven_db::repo;

use crate::JobQueue;
use crate::lifecycle::{upsert_requested_movie, upsert_requested_show};

/// Run the content-service flow end to end: fan out a
/// `ContentServiceRequested` hook job to every subscribed plugin, await their
/// responses, persist new items, and prune content removed from upstream.
pub async fn run(queue: &JobQueue) {
    let outcomes = queue
        .fan_out_and_collect(&RivenEvent::ContentServiceRequested)
        .await;
    if outcomes.is_empty() {
        tracing::debug!("content-service flow has no subscribers");
        return;
    }
    // A service that errored or was rate-limited reports nothing, which is
    // indistinguishable from "it dropped all its requests" once the responses
    // are merged — so the prune below has to be skipped entirely.
    let unavailable = crate::dispatch::count_infrastructure_failures(&outcomes);
    let responses: Vec<ContentServiceResponse> = crate::dispatch::decode_hook_responses(outcomes);

    let mut content = ContentCollection::default();

    for response in responses {
        for movie in response.movies {
            content.insert_movie(movie);
        }
        for show in response.shows {
            content.insert_show(show);
        }
    }

    let response = content.into_response();
    let all_movies = response.movies;
    let all_shows = response.shows;

    for movie in &all_movies {
        let title = movie
            .imdb_id
            .as_deref()
            .or(movie.tmdb_id.as_deref())
            .unwrap_or("Unknown");

        match upsert_requested_movie(
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

        match upsert_requested_show(
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

    if unavailable > 0 {
        tracing::warn!(
            unavailable,
            "content service: some services did not answer, skipping the removed-content cleanup so their items are not deleted"
        );
    } else if !active_external_ids.is_empty() {
        match repo::delete_items_removed_from_content_services(&active_external_ids).await {
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
