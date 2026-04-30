use async_graphql::*;
use riven_core::events::RivenEvent;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;
use riven_db::repo;
use riven_queue::orchestrator::LibraryOrchestrator;
use riven_queue::{IndexJob, JobQueue};
use sqlx::PgPool;
use std::sync::Arc;

use crate::schema::auth::require_library_access;

// ── Resolver ──

#[derive(Default)]
pub struct LibraryMutations;

#[Object]
impl LibraryMutations {
    /// Delete a specific filesystem entry (a single downloaded version) by its ID.
    /// Returns true if the entry was found and deleted. The DB trigger on
    /// `filesystem_entries` recomputes the owning item's state automatically.
    async fn delete_filesystem_entry(&self, ctx: &Context<'_>, id: i64) -> Result<bool> {
        require_library_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        let (deleted, _item_id) = repo::delete_filesystem_entry(pool, id).await?;
        Ok(deleted)
    }

    /// Reset items to Indexed state and clear failed_attempts.
    async fn reset_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        require_library_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::reset_items_by_ids(pool, ids).await? as i64)
    }

    /// Clear failed_attempts for items so they will be retried.
    async fn retry_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        require_library_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::retry_items_by_ids(pool, ids).await? as i64)
    }

    /// Remove items by ID.
    async fn remove_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        require_library_access(ctx)?;
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
        require_library_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::pause_items_by_ids(pool, ids).await? as i64)
    }

    /// Unpause items (derives next state from current facts).
    async fn unpause_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        require_library_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::unpause_items_by_ids(pool, ids).await? as i64)
    }

    /// Trigger a scrape for an existing item by entering its
    /// per-item state machine. For shows, optionally provide season_numbers
    /// to mark additional seasons requested before processing.
    async fn scrape_item(
        &self,
        ctx: &Context<'_>,
        id: i64,
        season_numbers: Option<Vec<i32>>,
    ) -> Result<String> {
        require_library_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let item = repo::get_media_item(pool, id)
            .await?
            .ok_or_else(|| Error::new("Item not found"))?;

        // For Show + specific seasons: flip is_requested on those seasons /
        // their episodes so ProcessMediaItem's Show fan-out picks them up.
        if item.item_type == MediaItemType::Show
            && let Some(seasons) = season_numbers.as_deref()
            && !seasons.is_empty()
            && let Err(err) =
                repo::mark_seasons_requested_and_get_episodes(pool, item.id, seasons).await
        {
            tracing::warn!(show_id = item.id, %err, "failed to mark seasons requested");
        }

        job_queue
            .push_process_media_item(riven_queue::ProcessMediaItemJob::new(item.id))
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
        require_library_access(ctx)?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let outcome = match item_type {
            MediaItemType::Movie => orchestrator
                .upsert_requested_movie(&title, imdb_id.as_deref(), tmdb_id.as_deref(), None, None)
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

        if let Some(event) = outcome.lifecycle_event(seasons.as_deref()) {
            job_queue.notify(event).await;
        }

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
        _seasons: Option<Vec<i32>>,
    ) -> Result<MediaItem> {
        require_library_access(ctx)?;
        if !matches!(item_type, MediaItemType::Movie | MediaItemType::Show) {
            return Err(Error::new(
                "Only Movie and Show types can be discovered directly",
            ));
        }

        let pool = ctx.data::<PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

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
            // Unrequested discovery: route through ProcessMediaItem. The
            // `is_requested=false` flag short-circuits the Download advance
            // in `MainOrchestrator::on_event`, so this scrapes without
            // continuing to a download.
            job_queue
                .push_process_media_item(riven_queue::ProcessMediaItemJob::new(item.id))
                .await;
        } else {
            job_queue.push_index(IndexJob::from_item(&item)).await;
        }

        Ok(item)
    }
}
