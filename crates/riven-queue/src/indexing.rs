use anyhow::Result;

use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

/// Persist the metadata returned by an indexer plugin. State recomputation is
/// handled by DB triggers — every `media_items` insert/update fires the
/// state-derivation pipeline automatically.
pub async fn apply_indexed_media_item(
    db_pool: &sqlx::PgPool,
    item: &MediaItem,
    indexed: &IndexedMediaItem,
    requested_seasons: Option<&[i32]>,
) -> Result<()> {
    repo::update_media_item_index(db_pool, item.id, indexed).await?;

    // Backfill any external IDs the indexer resolved onto the parent
    // item_request. Prevents duplicate requests where a later request
    // arrives with a different ID set than the original (e.g. only tmdb_id
    // when the original was created with only imdb_id).
    if let Some(request_id) = item.item_request_id {
        repo::backfill_item_request_external_ids(
            db_pool,
            request_id,
            indexed.imdb_id.as_deref(),
            indexed.tvdb_id.as_deref(),
            indexed.tmdb_id.as_deref(),
        )
        .await?;
    }

    if item.item_type == MediaItemType::Movie {
        return Ok(());
    }

    if item.item_type == MediaItemType::Show
        && let Some(seasons) = &indexed.seasons
    {
        for season_data in seasons {
            let season_requested = if season_data.number == 0 {
                requested_seasons.as_ref().is_some_and(|s| s.contains(&0))
            } else {
                requested_seasons
                    .as_ref()
                    .is_none_or(|s| s.contains(&season_data.number))
            };

            if requested_seasons.is_some() && !season_requested {
                continue;
            }

            let season = repo::create_season(
                db_pool,
                item.id,
                season_data.number,
                season_data.title.as_deref(),
                season_data.tvdb_id.as_deref(),
                season_data.number == 0,
                item.item_request_id,
                season_requested,
            )
            .await?;

            for ep_data in &season_data.episodes {
                repo::create_episode(
                    db_pool,
                    season.id,
                    ep_data.number,
                    ep_data.title.as_deref(),
                    ep_data.tvdb_id.as_deref(),
                    ep_data.aired_at,
                    ep_data.aired_at_utc,
                    ep_data.runtime,
                    ep_data.absolute_number,
                    item.item_request_id,
                    season_requested,
                    Some(season_data.number),
                )
                .await?;
            }
        }

        // Once seasons have been (re-)materialized, recompute the
        // partial-request flag. Must run AFTER the season inserts above so
        // the denominator (count of non-special seasons) is current.
        if let Some(request_id) = item.item_request_id {
            repo::recompute_is_partial_request(db_pool, request_id, item.id).await?;
        }
    }

    Ok(())
}
