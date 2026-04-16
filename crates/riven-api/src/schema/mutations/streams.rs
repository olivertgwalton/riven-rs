use async_graphql::*;
use riven_core::plugin::PluginRegistry;
use riven_core::types::MediaItemType;
use riven_db::repo;
use riven_queue::{DownloadJob, JobQueue};
use sqlx::PgPool;
use std::sync::Arc;

use crate::schema::discovery::{discover_streams, ensure_download_target};
use crate::schema::types::DiscoveredStream;

// ── Resolver ──

#[derive(Default)]
pub struct StreamsMutations;

#[Object]
impl StreamsMutations {
    /// Discover stream candidates without creating or mutating media items.
    async fn discover_streams(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        seasons: Option<Vec<i32>>,
        cached_only: Option<bool>,
    ) -> Result<Vec<DiscoveredStream>> {
        let pool = ctx.data::<PgPool>()?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        discover_streams(
            pool,
            registry.as_ref(),
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            seasons.as_deref(),
            cached_only.unwrap_or(false),
        )
        .await
    }

    /// Create or prepare the real target item only after the user picks a specific stream.
    async fn download_discovered_stream(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        season_number: Option<i32>,
        info_hash: String,
        magnet: String,
        parsed_data: Option<serde_json::Value>,
        rank: Option<i64>,
    ) -> Result<String> {
        let pool = ctx.data::<PgPool>()?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let target = ensure_download_target(
            pool,
            registry.as_ref(),
            job_queue,
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            season_number,
        )
        .await?;

        let stream = repo::upsert_stream(pool, &info_hash, &magnet, parsed_data, rank).await?;
        repo::link_stream_to_item(pool, target.id, stream.id).await?;
        repo::refresh_state_cascade(pool, &target).await?;

        job_queue
            .push_download(DownloadJob {
                id: target.id,
                info_hash: info_hash.clone(),
                magnet,
                preferred_info_hash: Some(info_hash),
            })
            .await;

        Ok("Download queued".to_string())
    }
}
