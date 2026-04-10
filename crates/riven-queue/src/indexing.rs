use anyhow::Result;

use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

pub async fn apply_indexed_media_item(
    db_pool: &sqlx::PgPool,
    item: &MediaItem,
    indexed: &IndexedMediaItem,
    requested_seasons: Option<&[i32]>,
) -> Result<()> {
    repo::update_media_item_index(db_pool, item.id, indexed).await?;

    if item.item_type == MediaItemType::Movie {
        if let Some(fresh) = repo::get_media_item(db_pool, item.id).await? {
            let _ = repo::refresh_state(db_pool, &fresh).await;
        }
        return Ok(());
    }

    if item.item_type == MediaItemType::Show {
        if let Some(seasons) = &indexed.seasons {
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
                        ep_data.runtime,
                        ep_data.absolute_number,
                        item.item_request_id,
                        season_requested,
                        Some(season_data.number),
                    )
                    .await?;
                }

                let _ = repo::refresh_state(db_pool, &season).await;
            }
        }

        if let Some(show_item) = repo::get_media_item(db_pool, item.id).await? {
            let _ = repo::refresh_state(db_pool, &show_item).await;
        }
    }

    Ok(())
}
