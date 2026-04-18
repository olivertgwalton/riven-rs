use async_graphql::*;
use chrono::Datelike;
use riven_core::events::RivenEvent;
use riven_core::types::{ContentRating, IndexedMediaItem};
use riven_db::repo;
use riven_queue::JobQueue;
use std::collections::HashMap;
use std::sync::Arc;

use crate::schema::auth::require_settings_access;
use crate::schema::typed_items::Movie;

use super::MutationStatusText;

// ── Input types ──

/// Input for the `indexMovie` mutation.
#[derive(InputObject)]
pub(super) struct IndexMovieInput {
    /// ID of the `ItemRequest` being indexed.
    id: i64,
    title: String,
    imdb_id: Option<String>,
    content_rating: Option<ContentRating>,
    rating: Option<f64>,
    poster_url: Option<String>,
    /// ISO date string (YYYY-MM-DD) for the theatrical release.
    release_date: Option<String>,
    country: Option<String>,
    language: Option<String>,
    /// Locale → title aliases, e.g. `{"de": ["Titel"]}`.
    aliases: Option<serde_json::Value>,
    genres: Vec<String>,
    runtime: Option<i32>,
}

// ── Response types ──

/// Structured response returned by `indexMovie`.
#[derive(SimpleObject)]
pub(super) struct IndexMovieMutationResponse {
    success: bool,
    message: String,
    status_text: MutationStatusText,
    movie: Option<Movie>,
}

// ── Resolver ──

#[derive(Default)]
pub struct MovieMutations;

#[Object]
impl MovieMutations {
    /// Persist indexer data for a movie and advance it to the scraping stage.
    ///
    /// Called by the indexer plugin after it has resolved metadata (title,
    /// content rating, release date, etc.) for a movie item request.
    async fn index_movie(
        &self,
        ctx: &Context<'_>,
        input: IndexMovieInput,
    ) -> Result<IndexMovieMutationResponse> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let item = repo::get_request_root_item(pool, input.id)
            .await?
            .ok_or_else(|| Error::new("Item request not found"))?;

        let aired_at = parse_naive_date(input.release_date.as_deref());

        let indexed = IndexedMediaItem {
            title: Some(input.title),
            imdb_id: input.imdb_id,
            poster_path: input.poster_url,
            year: aired_at.map(|d| d.year()),
            genres: Some(input.genres),
            country: input.country,
            language: input.language,
            aliases: parse_aliases(input.aliases),
            content_rating: input.content_rating,
            rating: input.rating,
            runtime: input.runtime,
            aired_at,
            ..Default::default()
        };

        if let Err(e) =
            riven_queue::indexing::apply_indexed_media_item(pool, &item, &indexed, None).await
        {
            return Ok(IndexMovieMutationResponse {
                success: false,
                message: e.to_string(),
                status_text: MutationStatusText::InternalServerError,
                movie: None,
            });
        }

        let fresh = repo::get_media_item(pool, item.id).await?.unwrap_or(item);

        job_queue
            .notify(RivenEvent::MediaItemIndexSuccess {
                id: fresh.id,
                title: fresh.title.clone(),
                item_type: fresh.item_type,
            })
            .await;

        Ok(IndexMovieMutationResponse {
            success: true,
            message: "Movie indexed successfully.".to_string(),
            status_text: MutationStatusText::Ok,
            movie: Some(Movie { item: fresh }),
        })
    }
}

// ── Helpers ──

pub(super) fn parse_aliases(
    value: Option<serde_json::Value>,
) -> Option<HashMap<String, Vec<String>>> {
    value.and_then(|v| serde_json::from_value(v).ok())
}

pub(super) fn parse_naive_date(s: Option<&str>) -> Option<chrono::NaiveDate> {
    s.and_then(|raw| chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok())
}
