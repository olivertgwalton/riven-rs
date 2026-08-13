use async_graphql::*;
use redis::AsyncCommands;
use riven_core::nzb::{NZB_URL_TTL_SECS, nzb_indexer_redis_key, nzb_info_hash, nzb_url_redis_key};
use riven_core::plugin::PluginRegistry;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;
use riven_db::repo;
use riven_queue::{JobQueue, RankStreamsJob};
use std::sync::Arc;

use crate::schema::auth::{Capability, require};
use crate::schema::discovery::{
    discover_streams, ensure_download_target, ensure_show_target, resolve_pack_seasons,
};
use crate::schema::types::DiscoveredStream;

#[derive(Default)]
pub struct StreamsMutations;

/// Shared by every "user picked/pasted a specific release" mutation: create or
/// prepare the real target item only now, after the pick, mirroring
/// [`download_discovered_stream`]'s original branching so a manually-pasted
/// magnet/hash/NZB resolves a target exactly like a scraped one does.
///
/// For TV, the stream is matched against its parsed seasons (or the
/// caller-supplied `seasons` / `season_number`). A single-season pack links
/// to that season; a multi-season pack links to the **show** so the download
/// flow can fill every season it contains.
async fn resolve_manual_download_target(
    registry: &PluginRegistry,
    item_type: MediaItemType,
    title: &str,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    seasons: Option<&[i32]>,
    parsed_data: Option<&serde_json::Value>,
) -> Result<MediaItem> {
    if item_type == MediaItemType::Movie {
        ensure_download_target(
            registry, item_type, title, imdb_id, tmdb_id, tvdb_id, None, None,
        )
        .await
    } else if item_type == MediaItemType::Episode {
        ensure_download_target(
            registry,
            MediaItemType::Episode,
            title,
            imdb_id,
            tmdb_id,
            tvdb_id,
            season_number,
            episode_number,
        )
        .await
    } else {
        let pack_seasons = resolve_pack_seasons(parsed_data, seasons, season_number);
        match pack_seasons.as_slice() {
            [] => Err(async_graphql::Error::new("No season selected for download")),
            [single] => {
                ensure_download_target(
                    registry,
                    MediaItemType::Season,
                    title,
                    imdb_id,
                    tmdb_id,
                    tvdb_id,
                    Some(*single),
                    None,
                )
                .await
            }
            many => ensure_show_target(registry, title, imdb_id, tvdb_id, many).await,
        }
    }
}

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
        episode_number: Option<i32>,
        cached_only: Option<bool>,
    ) -> Result<Vec<DiscoveredStream>> {
        require(ctx, Capability::ScrapeItems)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        discover_streams(
            registry.as_ref(),
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            seasons.as_deref(),
            episode_number,
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
        episode_number: Option<i32>,
        seasons: Option<Vec<i32>>,
        info_hash: String,
        magnet: String,
        parsed_data: Option<serde_json::Value>,
        rank: Option<i64>,
    ) -> Result<String> {
        require(ctx, Capability::ScrapeItems)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let target = resolve_manual_download_target(
            registry.as_ref(),
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            season_number,
            episode_number,
            seasons.as_deref(),
            parsed_data.as_ref(),
        )
        .await?;

        let stream = repo::upsert_stream(&info_hash, &magnet, parsed_data, rank, None).await?;
        repo::link_stream_to_item(target.id, stream.id).await?;
        repo::mark_manual_scrape_only(target.id).await?;

        job_queue
            .push_rank_streams(RankStreamsJob {
                id: target.id,
                preferred_info_hash: Some(info_hash),
            })
            .await;

        Ok("Download queued".to_string())
    }

    /// Manually queue a specific NZB by URL, bypassing indexer search entirely
    /// — the usenet equivalent of [`download_discovered_stream`]'s explicit
    /// magnet/hash field. The URL is hashed into the same `nzb-`-prefixed
    /// `info_hash` a real newznab scrape would produce and stashed in Redis
    /// under the same keys ([`nzb_url_redis_key`]), so `plugin-usenet`'s
    /// download-time fetch can't tell the difference from a scraped result.
    ///
    /// TV has no parsed release metadata to infer seasons from here (there was
    /// no scrape), so the caller must supply `season_number`/`episode_number`
    /// or `seasons` explicitly.
    async fn download_explicit_nzb(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        season_number: Option<i32>,
        episode_number: Option<i32>,
        seasons: Option<Vec<i32>>,
        nzb_url: String,
    ) -> Result<String> {
        require(ctx, Capability::ScrapeItems)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let redis_conn = ctx.data::<redis::aio::ConnectionManager>()?;

        let nzb_url = nzb_url.trim();
        if !nzb_url.starts_with("http://") && !nzb_url.starts_with("https://") {
            return Err(async_graphql::Error::new(
                "NZB URL must start with http:// or https://",
            ));
        }

        let target = resolve_manual_download_target(
            registry.as_ref(),
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            season_number,
            episode_number,
            seasons.as_deref(),
            None,
        )
        .await?;

        let info_hash = nzb_info_hash(nzb_url);
        let mut redis_conn = redis_conn.clone();
        redis_conn
            .set_ex::<_, _, ()>(nzb_url_redis_key(&info_hash), nzb_url, NZB_URL_TTL_SECS)
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        redis_conn
            .set_ex::<_, _, ()>(
                nzb_indexer_redis_key(&info_hash),
                "manual",
                NZB_URL_TTL_SECS,
            )
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;

        let stream = repo::upsert_stream(&info_hash, "", None, None, None).await?;
        repo::link_stream_to_item(target.id, stream.id).await?;
        repo::mark_manual_scrape_only(target.id).await?;

        job_queue
            .push_rank_streams(RankStreamsJob {
                id: target.id,
                preferred_info_hash: Some(info_hash),
            })
            .await;

        Ok("Download queued".to_string())
    }
}
