use async_graphql::*;
use riven_core::events::RivenEvent;
use riven_db::repo;
use riven_queue::JobQueue;
use std::sync::Arc;

use crate::schema::auth::{Capability, require, require_settings_access};

#[derive(Default)]
pub struct LibraryMutations;

#[Object]
impl LibraryMutations {
    /// Delete a specific filesystem entry (a single downloaded version) by its ID.
    /// Returns true if the entry was found and deleted. The DB trigger on
    /// `filesystem_entries` recomputes the owning item's state automatically.
    async fn delete_filesystem_entry(&self, ctx: &Context<'_>, id: i64) -> Result<bool> {
        require(ctx, Capability::DeleteItems)?;
        let (deleted, _item_id) = repo::delete_filesystem_entry(id).await?;
        Ok(deleted)
    }

    /// Reject a downloaded file: permanently blacklist the release behind it
    /// and remove its tracked entry, then clear the owning item's retry
    /// backoff so a replacement is searched for on the next scheduler pass.
    /// Use when a download turned out wrong — mismatched title, bad quality,
    /// wrong language, etc.
    async fn blacklist_filesystem_entry(&self, ctx: &Context<'_>, id: i64) -> Result<bool> {
        require(ctx, Capability::DeleteItems)?;
        Ok(repo::blacklist_and_remove_filesystem_entry(id).await?)
    }

    async fn reset_library(&self, ctx: &Context<'_>) -> Result<i64> {
        require_settings_access(ctx)?;
        Ok(repo::reset_library().await? as i64)
    }

    /// Reset items to Indexed state and clear failed_attempts.
    async fn reset_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        require(ctx, Capability::ResetItems)?;
        Ok(repo::reset_items_by_ids(ids).await? as i64)
    }

    /// Clear failed_attempts for items so they will be retried.
    async fn retry_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        require(ctx, Capability::RetryItems)?;
        Ok(repo::retry_items_by_ids(ids).await? as i64)
    }

    /// Remove items by ID.
    async fn remove_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        require(ctx, Capability::DeleteItems)?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let deleted_paths = repo::get_media_entry_paths_for_items(&ids)
            .await
            .unwrap_or_default();
        let external_request_ids = repo::get_external_request_ids_for_items(&ids)
            .await
            .unwrap_or_default();

        let count = repo::delete_items_by_ids(ids.clone()).await? as i64;

        if !ids.is_empty() {
            job_queue.cancel_items(&ids).await;
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
        require(ctx, Capability::PauseItems)?;
        Ok(repo::pause_items_by_ids(ids).await? as i64)
    }

    /// Unpause items (derives next state from current facts).
    async fn unpause_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        require(ctx, Capability::PauseItems)?;
        Ok(repo::unpause_items_by_ids(ids).await? as i64)
    }
}
