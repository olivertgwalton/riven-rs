use async_graphql::*;
use riven_core::events::RivenEvent;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;
use riven_db::repo;
use riven_queue::orchestrator::LibraryOrchestrator;
use riven_queue::{IndexJob, JobQueue};
use sqlx::PgPool;
use std::sync::Arc;

// ── Resolver ──

#[derive(Default)]
pub struct LibraryMutations;

#[Object]
impl LibraryMutations {
    /// Delete a specific filesystem entry (a single downloaded version) by its ID.
    /// Returns true if the entry was found and deleted.
    async fn delete_filesystem_entry(&self, ctx: &Context<'_>, id: i64) -> Result<bool> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::delete_filesystem_entry(pool, id).await?)
    }

    /// Reset items to Indexed state and clear failed_attempts.
    async fn reset_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::reset_items_by_ids(pool, ids).await? as i64)
    }

    /// Clear failed_attempts for items so they will be retried.
    async fn retry_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::retry_items_by_ids(pool, ids).await? as i64)
    }

    /// Remove items by ID.
    async fn remove_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let deleted_paths = repo::get_media_entry_paths_for_items(pool, &ids)
            .await
            .unwrap_or_default();
        let external_request_ids = repo::get_external_request_ids_for_items(pool, &ids)
            .await
            .unwrap_or_default();

        let count = repo::delete_items_by_ids(pool, ids.clone()).await? as i64;

        if !ids.is_empty() {
            job_queue
                .notify(RivenEvent::MediaItemsDeleted {
                    item_ids: ids,
                    external_request_ids,
                    deleted_paths,
                })
                .await;
        }

        Ok(count)
    }

    /// Pause items.
    async fn pause_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::pause_items_by_ids(pool, ids).await? as i64)
    }

    /// Unpause items (transitions back to Indexed).
    async fn unpause_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::unpause_items_by_ids(pool, ids).await? as i64)
    }

    /// Trigger a scrape for an existing item.
    /// For shows, optionally provide season_numbers to scrape specific seasons.
    /// If season_numbers is omitted, all requested seasons in Indexed state are scraped.
    async fn scrape_item(
        &self,
        ctx: &Context<'_>,
        id: i64,
        season_numbers: Option<Vec<i32>>,
    ) -> Result<String> {
        let pool = ctx.data::<PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let item = repo::get_media_item(pool, id)
            .await?
            .ok_or_else(|| Error::new("Item not found"))?;

        orchestrator
            .queue_scrape_for_item(&item, season_numbers.as_deref(), true)
            .await;

        Ok("Scrape queued".to_string())
    }

    /// Add a new media item to track and immediately queue it for indexing.
    /// For shows, `seasons` is an optional list of season numbers to request.
    /// If omitted, all non-special seasons are requested.
    async fn add_item(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        seasons: Option<Vec<i32>>,
    ) -> Result<MediaItem> {
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let outcome = match item_type {
            MediaItemType::Movie => orchestrator
                .upsert_requested_movie(
                    &title,
                    imdb_id.as_deref(),
                    tmdb_id.as_deref(),
                    None,
                    None,
                )
                .await
                .map_err(Error::from)?,
            MediaItemType::Show => orchestrator
                .upsert_requested_show(
                    &title,
                    imdb_id.as_deref(),
                    tvdb_id.as_deref(),
                    None,
                    None,
                    seasons.as_deref(),
                )
                .await
                .map_err(Error::from)?,
            _ => {
                return Err(Error::new(
                    "Only Movie and Show types can be added directly",
                ));
            }
        };

        orchestrator
            .enqueue_after_request(&outcome, seasons.as_deref())
            .await;

        Ok(outcome.item)
    }

    /// Create or reuse a non-requested media item, then index/scrape it so streams can be inspected.
    async fn discover_item(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        seasons: Option<Vec<i32>>,
    ) -> Result<MediaItem> {
        if !matches!(item_type, MediaItemType::Movie | MediaItemType::Show) {
            return Err(Error::new(
                "Only Movie and Show types can be discovered directly",
            ));
        }

        let pool = ctx.data::<PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let item = if let Some(existing) = repo::find_existing_media_item(
            pool,
            item_type,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
        )
        .await?
        {
            existing
        } else {
            repo::add_media_item_unrequested(
                pool,
                item_type,
                title,
                imdb_id.clone(),
                tmdb_id,
                tvdb_id,
            )
            .await?
        };

        if item.imdb_id.is_some() {
            orchestrator
                .queue_scrape_for_item(&item, seasons.as_deref(), false)
                .await;
        } else {
            job_queue.push_index(IndexJob::from_item(&item)).await;
        }

        Ok(item)
    }
}
