use anyhow::Result;
use riven_core::types::*;
use sqlx::PgPool;

use crate::entities::*;

use super::media::{get_media_item, update_media_item_state};

fn determine_fixed_state(item: &MediaItem) -> Option<MediaItemState> {
    match item.state {
        MediaItemState::Paused | MediaItemState::Failed => Some(item.state),
        _ => None,
    }
}

async fn compute_leaf_state(
    pool: &PgPool,
    item: &MediaItem,
    allow_media_entries: bool,
) -> Result<MediaItemState> {
    if let Some(aired) = item.aired_at {
        if aired > chrono::Utc::now().date_naive() {
            return Ok(MediaItemState::Unreleased);
        }
    }

    if let Some(state) = determine_fixed_state(item) {
        return Ok(state);
    }

    let has_media = if allow_media_entries {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM filesystem_entries
                WHERE media_item_id = $1 AND entry_type = 'media'
            )",
        )
        .bind(item.id)
        .fetch_one(pool)
        .await
        .unwrap_or(false)
    } else {
        false
    };

    if has_media {
        return Ok(MediaItemState::Completed);
    }

    let has_streams = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
            SELECT 1 FROM media_item_streams ms
            WHERE ms.media_item_id = $1
              AND ms.stream_id NOT IN (
                  SELECT stream_id FROM media_item_blacklisted_streams
                  WHERE media_item_id = $1
              )
        )",
    )
    .bind(item.id)
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    if has_streams {
        return Ok(MediaItemState::Scraped);
    }

    Ok(MediaItemState::Indexed)
}

/// Derive the correct state for an item from its persisted data.
///
/// Season and Show variants use lightweight single-column queries rather than
/// fetching full MediaItem rows — this is the primary hot-path optimisation
/// since compute_state is called on every scrape and download.
pub async fn compute_state(pool: &PgPool, item: &MediaItem) -> Result<MediaItemState> {
    match item.item_type {
        MediaItemType::Movie | MediaItemType::Episode => compute_leaf_state(pool, item, true).await,

        MediaItemType::Season => {
            let states: Vec<MediaItemState> = sqlx::query_scalar::<_, MediaItemState>(
                "SELECT state FROM media_items WHERE item_type = 'episode' AND parent_id = $1",
            )
            .bind(item.id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

            if let Some(state) = aggregate_states(item, &states) {
                return Ok(state);
            }

            compute_leaf_state(pool, item, false).await
        }

        MediaItemType::Show => {
            let states: Vec<MediaItemState> = sqlx::query_scalar::<_, MediaItemState>(
                "SELECT state FROM media_items \
                 WHERE item_type = 'season' AND parent_id = $1 \
                   AND is_requested = true AND is_special = false",
            )
            .bind(item.id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

            if let Some(state) = aggregate_states(item, &states) {
                return Ok(state);
            }

            compute_leaf_state(pool, item, false).await
        }
    }
}

/// Aggregate child states using the same precedence as the TS subscriber.
fn aggregate_states(item: &MediaItem, states: &[MediaItemState]) -> Option<MediaItemState> {
    if states.is_empty() {
        return None;
    }

    if let Some(state) = determine_fixed_state(item) {
        return Some(state);
    }

    for propagated in [
        MediaItemState::Paused,
        MediaItemState::Failed,
        MediaItemState::Unreleased,
    ] {
        if states.iter().all(|state| *state == propagated) {
            return Some(propagated);
        }
    }

    if states
        .iter()
        .all(|state| *state == MediaItemState::Completed)
    {
        return Some(
            if item.item_type == MediaItemType::Show
                && item.show_status == Some(ShowStatus::Continuing)
            {
                MediaItemState::Ongoing
            } else {
                MediaItemState::Completed
            },
        );
    }

    if states
        .iter()
        .any(|state| matches!(state, MediaItemState::Ongoing | MediaItemState::Unreleased))
        || (item.item_type == MediaItemType::Show
            && item.show_status == Some(ShowStatus::Continuing))
    {
        return Some(MediaItemState::Ongoing);
    }

    if states.iter().any(|state| {
        matches!(
            state,
            MediaItemState::Completed | MediaItemState::PartiallyCompleted
        )
    }) {
        return Some(MediaItemState::PartiallyCompleted);
    }

    if states.iter().any(|state| *state == MediaItemState::Scraped) {
        return Some(MediaItemState::Scraped);
    }

    None
}

/// Compute and persist state without cascading to parents.
pub async fn refresh_state(pool: &PgPool, item: &MediaItem) -> Result<MediaItemState> {
    let state = compute_state(pool, item).await?;
    update_media_item_state(pool, item.id, state).await?;
    Ok(state)
}

/// Compute and persist state, then cascade up through parents.
pub async fn refresh_state_cascade(pool: &PgPool, item: &MediaItem) -> Result<()> {
    refresh_state(pool, item).await?;
    cascade_state_update(pool, item).await?;
    Ok(())
}

/// Cascade state updates from an item up through its parents.
pub async fn cascade_state_update(pool: &PgPool, item: &MediaItem) -> Result<()> {
    if item.item_type == MediaItemType::Episode {
        if let Some(season_id) = item.parent_id {
            if let Some(season) = get_media_item(pool, season_id).await? {
                refresh_state(pool, &season).await?;
                if let Some(show_id) = season.parent_id {
                    if let Some(show) = get_media_item(pool, show_id).await? {
                        refresh_state(pool, &show).await?;
                    }
                }
            }
        }
    } else if item.item_type == MediaItemType::Season {
        if let Some(show_id) = item.parent_id {
            if let Some(show) = get_media_item(pool, show_id).await? {
                refresh_state(pool, &show).await?;
            }
        }
    }
    Ok(())
}

/// Batch-set a list of items directly to `Completed` in one UPDATE.
///
/// Used by `persist_season` after successfully writing media entries for
/// multiple episodes — avoids N individual `refresh_state` calls (each of
/// which would SELECT + UPDATE) by computing state inline (we know the
/// episodes are completed because we just created their entries).
pub async fn batch_set_completed(pool: &PgPool, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE media_items SET state = 'completed', updated_at = NOW() WHERE id = ANY($1)",
    )
    .bind(ids)
    .execute(pool)
    .await?;
    Ok(())
}
