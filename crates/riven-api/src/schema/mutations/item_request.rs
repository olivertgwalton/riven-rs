use async_graphql::*;
use riven_db::entities::ItemRequest;
use riven_db::repo::ItemRequestUpsertAction;
use riven_queue::JobQueue;
use riven_queue::orchestrator::LibraryOrchestrator;
use std::collections::HashSet;
use std::sync::Arc;

use super::MutationStatusText;

#[derive(Enum, Copy, Clone, PartialEq, Eq)]
pub(super) enum RequestItemMutationResponseErrorCode {
    Conflict,
    UnexpectedError,
}

// ── Input types ──

/// Input for requesting a movie to be tracked.
#[derive(InputObject)]
pub(super) struct MovieRequestInput {
    /// Title used as a placeholder until indexing fills in the canonical name.
    title: String,
    imdb_id: Option<String>,
    tmdb_id: Option<String>,
    /// Identifier of the external system (e.g. Seerr) that originated this request.
    requested_by: Option<String>,
    /// External request ID for correlation with the originating content service.
    external_request_id: Option<String>,
}

/// Input for requesting a show (and optionally specific seasons) to be tracked.
#[derive(InputObject)]
pub(super) struct ShowRequestInput {
    /// Title used as a placeholder until indexing fills in the canonical name.
    title: String,
    imdb_id: Option<String>,
    tvdb_id: Option<String>,
    /// Season numbers to request. When omitted all non-special seasons are requested.
    seasons: Option<Vec<i32>>,
    /// Identifier of the external system (e.g. Seerr) that originated this request.
    requested_by: Option<String>,
    /// External request ID for correlation with the originating content service.
    external_request_id: Option<String>,
}

// ── Response types ──

/// Structured response returned by `requestMovie` and `requestShow`.
#[derive(SimpleObject)]
pub(super) struct RequestItemMutationResponse {
    success: bool,
    message: String,
    status_text: MutationStatusText,
    error_code: Option<RequestItemMutationResponseErrorCode>,
    /// The item request that was created or updated; `null` on conflict.
    item: Option<ItemRequest>,
}

/// Returned by `requestItems` — a summary of a bulk upsert operation.
#[derive(SimpleObject)]
pub(super) struct RequestItemsResult {
    /// Total number of unique items processed after deduplication.
    count: i32,
    /// Newly created item requests.
    new_items: Vec<ItemRequest>,
    /// Item requests that were updated (e.g. new seasons added to an existing show request).
    updated_items: Vec<ItemRequest>,
}

// ── Resolver ──

#[derive(Default)]
pub struct ItemRequestMutations;

#[Object]
impl ItemRequestMutations {
    /// Request a movie to be tracked and indexed.
    ///
    /// Returns a structured response. If an identical request already exists
    /// the mutation succeeds without error but the `statusText` is `CONFLICT`
    /// and `item` is `null`.
    async fn request_movie(
        &self,
        ctx: &Context<'_>,
        input: MovieRequestInput,
    ) -> Result<RequestItemMutationResponse> {
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let outcome = match orchestrator
            .upsert_requested_movie(
                &input.title,
                input.imdb_id.as_deref(),
                input.tmdb_id.as_deref(),
                input.requested_by.as_deref(),
                input.external_request_id.as_deref(),
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Ok(RequestItemMutationResponse {
                    success: false,
                    message: error.to_string(),
                    status_text: MutationStatusText::BadRequest,
                    error_code: Some(RequestItemMutationResponseErrorCode::UnexpectedError),
                    item: None,
                });
            }
        };

        if outcome.action == ItemRequestUpsertAction::Unchanged {
            return Ok(RequestItemMutationResponse {
                success: false,
                message: "A request for this movie already exists.".to_string(),
                status_text: MutationStatusText::Conflict,
                error_code: Some(RequestItemMutationResponseErrorCode::Conflict),
                item: None,
            });
        }

        if let Some(event) = outcome.lifecycle_event(None) {
            job_queue.notify(event).await;
        }

        Ok(RequestItemMutationResponse {
            success: true,
            message: "Movie request created successfully.".to_string(),
            status_text: MutationStatusText::Created,
            error_code: None,
            item: Some(outcome.request),
        })
    }

    /// Request a show (and optionally specific seasons) to be tracked and indexed.
    ///
    /// If the show was already requested but new seasons are included the request
    /// is updated and `statusText` is `OK`. If nothing has changed `statusText`
    /// is `CONFLICT` and `item` is `null`.
    async fn request_show(
        &self,
        ctx: &Context<'_>,
        input: ShowRequestInput,
    ) -> Result<RequestItemMutationResponse> {
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let outcome = match orchestrator
            .upsert_requested_show(
                &input.title,
                input.imdb_id.as_deref(),
                input.tvdb_id.as_deref(),
                input.requested_by.as_deref(),
                input.external_request_id.as_deref(),
                input.seasons.as_deref(),
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Ok(RequestItemMutationResponse {
                    success: false,
                    message: error.to_string(),
                    status_text: MutationStatusText::BadRequest,
                    error_code: Some(RequestItemMutationResponseErrorCode::UnexpectedError),
                    item: None,
                });
            }
        };

        let (success, message, status_text) = match outcome.action {
            ItemRequestUpsertAction::Created => (
                true,
                "Show request created successfully.".to_string(),
                MutationStatusText::Created,
            ),
            ItemRequestUpsertAction::Updated => (
                true,
                "Show request updated successfully.".to_string(),
                MutationStatusText::Ok,
            ),
            ItemRequestUpsertAction::Unchanged => {
                return Ok(RequestItemMutationResponse {
                    success: false,
                    message: "A request for this show already exists.".to_string(),
                    status_text: MutationStatusText::Conflict,
                    error_code: Some(RequestItemMutationResponseErrorCode::Conflict),
                    item: None,
                });
            }
        };

        if let Some(event) = outcome.lifecycle_event(input.seasons.as_deref()) {
            job_queue.notify(event).await;
        }

        Ok(RequestItemMutationResponse {
            success,
            message,
            status_text,
            error_code: None,
            item: Some(outcome.request),
        })
    }

    /// Bulk-request movies and shows in a single call.
    ///
    /// Items are deduplicated by their primary external ID (TMDB for movies,
    /// TVDB for shows, with IMDB as fallback) so duplicate entries from a
    /// single content-service payload are collapsed before processing.
    ///
    /// Returns the count of unique items processed and separate lists of newly
    /// created vs updated item requests. Conflicts (already-requested items
    /// with no change) are silently skipped.
    async fn request_items(
        &self,
        ctx: &Context<'_>,
        movies: Vec<MovieRequestInput>,
        shows: Vec<ShowRequestInput>,
    ) -> Result<RequestItemsResult> {
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let mut seen: HashSet<String> = HashSet::new();
        let mut new_items: Vec<ItemRequest> = Vec::new();
        let mut updated_items: Vec<ItemRequest> = Vec::new();
        let mut count: i32 = 0;

        for movie in movies {
            let key = movie
                .tmdb_id
                .as_deref()
                .or(movie.imdb_id.as_deref())
                .map(str::to_owned);

            if let Some(ref k) = key
                && !seen.insert(k.clone())
            {
                continue;
            }

            count += 1;

            let outcome = orchestrator
                .upsert_requested_movie(
                    &movie.title,
                    movie.imdb_id.as_deref(),
                    movie.tmdb_id.as_deref(),
                    movie.requested_by.as_deref(),
                    movie.external_request_id.as_deref(),
                )
                .await
                .map_err(Error::from)?;

            match outcome.action {
                ItemRequestUpsertAction::Created => {
                    if let Some(event) = outcome.lifecycle_event(None) {
                        job_queue.notify(event).await;
                    }
                    new_items.push(outcome.request);
                }
                ItemRequestUpsertAction::Updated => {
                    if let Some(event) = outcome.lifecycle_event(None) {
                        job_queue.notify(event).await;
                    }
                    updated_items.push(outcome.request);
                }
                ItemRequestUpsertAction::Unchanged => {}
            }
        }

        for show in shows {
            let key = show
                .tvdb_id
                .as_deref()
                .or(show.imdb_id.as_deref())
                .map(str::to_owned);

            if let Some(ref k) = key
                && !seen.insert(k.clone())
            {
                continue;
            }

            count += 1;

            let seasons = show.seasons.as_deref();

            let outcome = orchestrator
                .upsert_requested_show(
                    &show.title,
                    show.imdb_id.as_deref(),
                    show.tvdb_id.as_deref(),
                    show.requested_by.as_deref(),
                    show.external_request_id.as_deref(),
                    seasons,
                )
                .await
                .map_err(Error::from)?;

            match outcome.action {
                ItemRequestUpsertAction::Created => {
                    if let Some(event) = outcome.lifecycle_event(show.seasons.as_deref()) {
                        job_queue.notify(event).await;
                    }
                    new_items.push(outcome.request);
                }
                ItemRequestUpsertAction::Updated => {
                    if let Some(event) = outcome.lifecycle_event(show.seasons.as_deref()) {
                        job_queue.notify(event).await;
                    }
                    updated_items.push(outcome.request);
                }
                ItemRequestUpsertAction::Unchanged => {}
            }
        }

        Ok(RequestItemsResult {
            count,
            new_items,
            updated_items,
        })
    }
}
