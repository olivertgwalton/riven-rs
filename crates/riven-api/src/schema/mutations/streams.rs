use async_graphql::*;
use riven_core::plugin::PluginRegistry;
use riven_core::types::MediaItemType;
use riven_db::repo;
use riven_queue::{JobQueue, RankStreamsJob};
use sqlx::PgPool;
use std::sync::Arc;

use crate::schema::auth::require_library_access;
use crate::schema::discovery::{
    discover_streams, ensure_download_target, ensure_show_target, resolve_pack_seasons,
};
use crate::schema::types::DiscoveredStream;

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
        require_library_access(ctx)?;
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
    ///
    /// For TV, the stream is matched against its parsed seasons (or the
    /// caller-supplied `seasons` / `season_number`). A single-season pack links
    /// to that season; a multi-season pack links to the **show** so the download
    /// flow can fill every season it contains.
    async fn download_discovered_stream(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        season_number: Option<i32>,
        seasons: Option<Vec<i32>>,
        info_hash: String,
        magnet: String,
        parsed_data: Option<serde_json::Value>,
        rank: Option<i64>,
    ) -> Result<String> {
        require_library_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let target = if item_type == MediaItemType::Movie {
            ensure_download_target(
                pool,
                registry.as_ref(),
                job_queue,
                item_type,
                &title,
                imdb_id.as_deref(),
                tmdb_id.as_deref(),
                tvdb_id.as_deref(),
                None,
            )
            .await?
        } else {
            let pack_seasons =
                resolve_pack_seasons(parsed_data.as_ref(), seasons.as_deref(), season_number);
            match pack_seasons.as_slice() {
                [] => return Err(async_graphql::Error::new("No season selected for download")),
                [single] => {
                    ensure_download_target(
                        pool,
                        registry.as_ref(),
                        job_queue,
                        MediaItemType::Season,
                        &title,
                        imdb_id.as_deref(),
                        tmdb_id.as_deref(),
                        tvdb_id.as_deref(),
                        Some(*single),
                    )
                    .await?
                }
                many => {
                    ensure_show_target(
                        pool,
                        registry.as_ref(),
                        job_queue,
                        &title,
                        imdb_id.as_deref(),
                        tvdb_id.as_deref(),
                        many,
                    )
                    .await?
                }
            }
        };

        let stream =
            repo::upsert_stream(pool, &info_hash, &magnet, parsed_data, rank, None).await?;
        repo::link_stream_to_item(pool, target.id, stream.id).await?;

        job_queue
            .push_rank_streams(RankStreamsJob {
                id: target.id,
                preferred_info_hash: Some(info_hash),
            })
            .await;

        Ok("Download queued".to_string())
    }
}
