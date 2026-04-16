use async_graphql::*;
use riven_core::events::RivenEvent;
use riven_core::types::{
    ContentRating, IndexedEpisode, IndexedMediaItem, IndexedSeason, ShowStatus,
};
use riven_db::repo;
use riven_queue::JobQueue;
use std::sync::Arc;

use crate::schema::typed_items::Show;
use crate::schema::auth::require_settings_access;

use super::MutationStatusText;
use super::movie::{parse_aliases, parse_naive_date};

// ── Input types ──

#[derive(InputObject)]
pub(super) struct IndexEpisodeInput {
    title: String,
    number: i32,
    absolute_number: Option<i32>,
    content_rating: Option<ContentRating>,
    runtime: Option<i32>,
    /// ISO date string (YYYY-MM-DD).
    aired_at: Option<String>,
    poster_path: Option<String>,
}

#[derive(InputObject)]
pub(super) struct IndexSeasonInput {
    number: i32,
    title: Option<String>,
    episodes: Vec<IndexEpisodeInput>,
}

/// Input for the `indexShow` mutation.
#[derive(InputObject)]
pub(super) struct IndexShowInput {
    /// ID of the `ItemRequest` being indexed.
    id: i64,
    title: String,
    imdb_id: Option<String>,
    content_rating: Option<ContentRating>,
    rating: Option<f64>,
    poster_url: Option<String>,
    country: Option<String>,
    language: Option<String>,
    /// Locale → title aliases, e.g. `{"de": ["Titel"]}`.
    aliases: Option<serde_json::Value>,
    status: ShowStatus,
    network: Option<String>,
    seasons: Vec<IndexSeasonInput>,
    genres: Vec<String>,
}

// ── Response types ──

/// Structured response returned by `indexShow`.
#[derive(SimpleObject)]
pub(super) struct IndexShowMutationResponse {
    success: bool,
    message: String,
    status_text: MutationStatusText,
    show: Option<Show>,
}

// ── Resolver ──

#[derive(Default)]
pub struct ShowMutations;

#[Object]
impl ShowMutations {
    /// Persist indexer data for a show (including seasons and episodes) and
    /// advance it to the scraping stage.
    ///
    /// Called by the indexer plugin after it has resolved metadata for a show
    /// item request.
    async fn index_show(
        &self,
        ctx: &Context<'_>,
        input: IndexShowInput,
    ) -> Result<IndexShowMutationResponse> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let item = repo::get_request_root_item(pool, input.id)
            .await?
            .ok_or_else(|| Error::new("Item request not found"))?;

        let requested_seasons = riven_queue::context::load_requested_seasons(pool, &item).await;

        let seasons: Vec<IndexedSeason> = input
            .seasons
            .into_iter()
            .map(|s| IndexedSeason {
                number: s.number,
                title: s.title,
                tvdb_id: None,
                episodes: s
                    .episodes
                    .into_iter()
                    .map(|e| IndexedEpisode {
                        number: e.number,
                        absolute_number: e.absolute_number,
                        title: Some(e.title),
                        tvdb_id: None,
                        aired_at: parse_naive_date(e.aired_at.as_deref()),
                        runtime: e.runtime,
                        poster_path: e.poster_path,
                        content_rating: e.content_rating,
                    })
                    .collect(),
            })
            .collect();

        let indexed = IndexedMediaItem {
            title: Some(input.title),
            imdb_id: input.imdb_id,
            poster_path: input.poster_url,
            genres: Some(input.genres),
            country: input.country,
            language: input.language,
            aliases: parse_aliases(input.aliases),
            content_rating: input.content_rating,
            rating: input.rating,
            network: input.network,
            status: Some(input.status),
            seasons: Some(seasons),
            ..Default::default()
        };

        if let Err(e) = riven_queue::indexing::apply_indexed_media_item(
            pool,
            &item,
            &indexed,
            requested_seasons.as_deref(),
        )
        .await
        {
            return Ok(IndexShowMutationResponse {
                success: false,
                message: e.to_string(),
                status_text: MutationStatusText::InternalServerError,
                show: None,
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

        Ok(IndexShowMutationResponse {
            success: true,
            message: "Show indexed successfully.".to_string(),
            status_text: MutationStatusText::Ok,
            show: Some(Show { item: fresh }),
        })
    }
}
