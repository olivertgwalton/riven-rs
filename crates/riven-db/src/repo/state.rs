use anyhow::Result;
use riven_core::types::*;
use sqlx::PgPool;

use crate::entities::*;

use super::hierarchy::{list_episodes, list_seasons};
use super::media::{get_media_item, update_media_item_state};

/// Compute the correct state for a media item based on its children/entries.
pub async fn compute_state(pool: &PgPool, item: &MediaItem) -> Result<MediaItemState> {
    match item.item_type {
        MediaItemType::Movie | MediaItemType::Episode => {
            let row = sqlx::query!(
                r#"SELECT
                     EXISTS(SELECT 1 FROM filesystem_entries WHERE media_item_id = $1 AND entry_type = 'media') AS has_media,
                     EXISTS(SELECT 1 FROM media_item_streams ms WHERE ms.media_item_id = $1 AND ms.stream_id NOT IN (SELECT stream_id FROM media_item_blacklisted_streams WHERE media_item_id = $1)) AS has_streams"#,
                item.id
            )
            .fetch_one(pool)
            .await?;

            if row.has_media.unwrap_or(false) {
                return Ok(MediaItemState::Completed);
            }

            if row.has_streams.unwrap_or(false) {
                return Ok(MediaItemState::Scraped);
            }

            if let Some(aired) = item.aired_at {
                if aired > chrono::Utc::now().date_naive() {
                    return Ok(MediaItemState::Unreleased);
                }
            }

            Ok(MediaItemState::Indexed)
        }

        MediaItemType::Season => {
            let episodes = list_episodes(pool, item.id).await?;
            if episodes.is_empty() {
                return Ok(MediaItemState::Indexed);
            }
            aggregate_child_states(&episodes)
        }

        MediaItemType::Show => {
            let seasons = list_seasons(pool, item.id).await?;
            // Only consider requested seasons — Season 0 (specials) and any other
            // non-requested seasons are excluded from the show's completion state,
            // matching TS's getStandardSeasons() behaviour.
            let requested: Vec<_> = seasons
                .into_iter()
                .filter(|s| s.is_requested)
                .collect();
            if requested.is_empty() {
                return Ok(MediaItemState::Indexed);
            }
            let state = aggregate_child_states(&requested)?;
            if state == MediaItemState::Completed
                && item.show_status == Some(ShowStatus::Continuing)
            {
                return Ok(MediaItemState::Ongoing);
            }
            Ok(state)
        }
    }
}

fn aggregate_child_states(children: &[MediaItem]) -> Result<MediaItemState> {
    if children.is_empty() {
        return Ok(MediaItemState::Indexed);
    }

    let (any_completed, any_unreleased, any_ongoing, any_scraped) =
        children.iter().fold((false, false, false, false), |(ac, au, ao, asc), c| {
            (
                ac || matches!(
                    c.state,
                    MediaItemState::Completed | MediaItemState::PartiallyCompleted | MediaItemState::Ongoing
                ),
                au || c.state == MediaItemState::Unreleased,
                ao || c.state == MediaItemState::Ongoing,
                asc || c.state == MediaItemState::Scraped,
            )
        });

    if children.iter().all(|c| c.state == MediaItemState::Completed) {
        return Ok(MediaItemState::Completed);
    }
    if children.iter().all(|c| c.state == MediaItemState::Unreleased) {
        return Ok(MediaItemState::Unreleased);
    }
    if any_completed && any_unreleased {
        return Ok(MediaItemState::Ongoing);
    }
    if any_completed {
        return Ok(MediaItemState::PartiallyCompleted);
    }
    if any_ongoing {
        return Ok(MediaItemState::Ongoing);
    }
    if any_scraped {
        return Ok(MediaItemState::Scraped);
    }
    if any_unreleased {
        return Ok(MediaItemState::Unreleased);
    }

    Ok(MediaItemState::Indexed)
}

/// Cascade state updates from an episode up through season and show.
pub async fn cascade_state_update(pool: &PgPool, item: &MediaItem) -> Result<()> {
    if item.item_type == MediaItemType::Episode {
        if let Some(season_id) = item.parent_id {
            if let Some(season) = get_media_item(pool, season_id).await? {
                let new_state = compute_state(pool, &season).await?;
                if new_state != season.state {
                    update_media_item_state(pool, season.id, new_state).await?;
                    if let Some(show_id) = season.parent_id {
                        if let Some(show) = get_media_item(pool, show_id).await? {
                            let show_state = compute_state(pool, &show).await?;
                            if show_state != show.state {
                                update_media_item_state(pool, show.id, show_state).await?;
                            }
                        }
                    }
                }
            }
        }
    } else if item.item_type == MediaItemType::Season {
        if let Some(show_id) = item.parent_id {
            if let Some(show) = get_media_item(pool, show_id).await? {
                let show_state = compute_state(pool, &show).await?;
                if show_state != show.state {
                    update_media_item_state(pool, show.id, show_state).await?;
                }
            }
        }
    }
    Ok(())
}
