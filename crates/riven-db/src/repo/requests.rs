use anyhow::Result;
use riven_core::types::*;
use sqlx::PgPool;

use crate::entities::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemRequestUpsertAction {
    Created,
    Updated,
    Unchanged,
}

#[derive(Debug, Clone)]
pub struct UpsertedItemRequest {
    pub request: ItemRequest,
    pub action: ItemRequestUpsertAction,
}

fn normalize_request_seasons(seasons: Option<&[i32]>) -> Option<Vec<i32>> {
    seasons.map(|requested| {
        let mut values = requested.to_vec();
        values.sort_unstable();
        values.dedup();
        values
    })
}

fn parse_request_seasons(request: &ItemRequest) -> Option<Vec<i32>> {
    request
        .seasons
        .as_ref()
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

/// Collect the non-null `external_request_id`s for the item_requests linked to
/// the given media_item IDs.  Used to notify content services before deletion.
pub async fn get_external_request_ids_for_items(
    pool: &PgPool,
    media_item_ids: &[i64],
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT ir.external_request_id
           FROM media_items mi
           JOIN item_requests ir ON ir.id = mi.item_request_id
           WHERE mi.id = ANY($1)
             AND ir.external_request_id IS NOT NULL"#,
        media_item_ids
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .flatten()
    .collect())
}

pub async fn get_item_request_by_id(pool: &PgPool, id: i64) -> Result<Option<ItemRequest>> {
    Ok(
        sqlx::query_as::<_, ItemRequest>("SELECT * FROM item_requests WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

/// Fetch the top-level media item (movie or show) associated with an item request.
pub async fn get_request_root_item(pool: &PgPool, request_id: i64) -> Result<Option<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items
             WHERE item_request_id = $1
               AND item_type = ANY(ARRAY['movie'::media_item_type, 'show'::media_item_type])
             ORDER BY CASE item_type
                 WHEN 'movie'::media_item_type THEN 0
                 WHEN 'show'::media_item_type THEN 1
                 ELSE 2
             END
             LIMIT 1",
    )
    .bind(request_id)
    .fetch_optional(pool)
    .await?)
}

/// Find an existing item request by any matching external ID.
pub async fn find_existing_item_request(
    pool: &PgPool,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<Option<ItemRequest>> {
    Ok(sqlx::query_as::<_, ItemRequest>(
        "SELECT * FROM item_requests
         WHERE (imdb_id = $1 AND $1 IS NOT NULL)
            OR (tmdb_id = $2 AND $2 IS NOT NULL)
            OR (tvdb_id = $3 AND $3 IS NOT NULL)
         LIMIT 1",
    )
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(tvdb_id)
    .fetch_optional(pool)
    .await?)
}

pub async fn create_item_request(
    pool: &PgPool,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    request_type: ItemRequestType,
    requested_by: Option<&str>,
    external_request_id: Option<&str>,
    seasons: Option<&[i32]>,
) -> Result<UpsertedItemRequest> {
    if let Some(existing) = find_existing_item_request(pool, imdb_id, tmdb_id, tvdb_id).await? {
        if request_type == ItemRequestType::Show {
            let existing_seasons = parse_request_seasons(&existing);
            let desired_seasons = normalize_request_seasons(seasons);

            let is_conflict = match (existing_seasons.as_ref(), desired_seasons.as_ref()) {
                (Some(existing_values), Some(desired_values)) => desired_values
                    .iter()
                    .all(|season| existing_values.contains(season)),
                _ => false,
            };

            if is_conflict {
                return Ok(UpsertedItemRequest {
                    request: existing,
                    action: ItemRequestUpsertAction::Unchanged,
                });
            }

            if existing_seasons != desired_seasons {
                // If the existing request had already reached a terminal-ish
                // state (completed/ongoing/unreleased), flag it as having had
                // additional seasons appended so the indexer knows to
                // re-process. New requests (still `requested`/`failed`) or
                // requests already mid-extension are left untouched.
                let bump_state = matches!(
                    existing.state,
                    ItemRequestState::Completed
                        | ItemRequestState::Ongoing
                        | ItemRequestState::Unreleased
                );
                let updated = sqlx::query_as::<_, ItemRequest>(
                    "UPDATE item_requests
                        SET seasons = $1,
                            state = CASE WHEN $3 THEN 'requested_additional_seasons'::item_request_state ELSE state END
                      WHERE id = $2
                      RETURNING *",
                )
                .bind(
                    desired_seasons.map(|values| serde_json::to_value(values).unwrap_or_default()),
                )
                .bind(existing.id)
                .bind(bump_state)
                .fetch_one(pool)
                .await?;
                return Ok(UpsertedItemRequest {
                    request: updated,
                    action: ItemRequestUpsertAction::Updated,
                });
            }
        }

        return Ok(UpsertedItemRequest {
            request: existing,
            action: ItemRequestUpsertAction::Unchanged,
        });
    }

    let seasons_json = normalize_request_seasons(seasons)
        .map(|values| serde_json::to_value(values).unwrap_or_default());
    let request = sqlx::query_as::<_, ItemRequest>(
        "INSERT INTO item_requests (imdb_id, tmdb_id, tvdb_id, request_type, requested_by, external_request_id, seasons, state) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'requested') \
         RETURNING *",
    )
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(tvdb_id)
    .bind(request_type as ItemRequestType)
    .bind(requested_by)
    .bind(external_request_id)
    .bind(seasons_json)
    .fetch_one(pool)
    .await?;
    Ok(UpsertedItemRequest {
        request,
        action: ItemRequestUpsertAction::Created,
    })
}

/// Backfill `imdb_id`/`tvdb_id`/`tmdb_id` on an item_request from the values
/// the indexer resolved. Only fills in IDs that were previously NULL so we
/// never clobber what the user originally supplied. Prevents duplicate
/// requests where a later request comes in with a different ID set than the
/// original (e.g. only tmdb_id when the original only had imdb_id).
pub async fn backfill_item_request_external_ids(
    pool: &PgPool,
    request_id: i64,
    imdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    tmdb_id: Option<&str>,
) -> Result<()> {
    if imdb_id.is_none() && tvdb_id.is_none() && tmdb_id.is_none() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE item_requests
            SET imdb_id = COALESCE(imdb_id, $2),
                tvdb_id = COALESCE(tvdb_id, $3),
                tmdb_id = COALESCE(tmdb_id, $4)
          WHERE id = $1",
    )
    .bind(request_id)
    .bind(imdb_id)
    .bind(tvdb_id)
    .bind(tmdb_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Recompute `is_partial_request` after the indexer has populated the show's
/// seasons. A request is "partial" when the user requested a strict subset
/// of the non-special seasons that actually exist on the show. Special
/// seasons (`season_number = 0`, `is_special = true`) are excluded from the
/// denominator.
pub async fn recompute_is_partial_request(
    pool: &PgPool,
    request_id: i64,
    show_id: i64,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE item_requests
              SET is_partial_request = (
                  CASE
                      WHEN seasons IS NULL THEN false
                      WHEN jsonb_array_length(seasons) = 0 THEN false
                      ELSE jsonb_array_length(seasons) < (
                          SELECT COUNT(*) FROM media_items
                           WHERE parent_id = $2
                             AND item_type = 'season'
                             AND COALESCE(is_special, false) = false
                      )
                  END
              )
            WHERE id = $1"#,
    )
    .bind(request_id)
    .bind(show_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_item_request_state(
    pool: &PgPool,
    id: i64,
    state: ItemRequestState,
) -> Result<ItemRequest> {
    Ok(sqlx::query_as::<_, ItemRequest>(
        "UPDATE item_requests \
         SET state = $2, \
             completed_at = CASE
                 WHEN $2 = 'completed'::item_request_state THEN COALESCE(completed_at, NOW())
                 ELSE completed_at
             END \
         WHERE id = $1 \
         RETURNING *",
    )
    .bind(id)
    .bind(state)
    .fetch_one(pool)
    .await?)
}

pub async fn get_retryable_item_requests(pool: &PgPool) -> Result<Vec<ItemRequest>> {
    Ok(sqlx::query_as::<_, ItemRequest>(
        "SELECT * FROM item_requests
         WHERE state = ANY(ARRAY['requested'::item_request_state, 'failed'::item_request_state])
           AND (
             state = 'failed'
             OR NOT EXISTS (
               SELECT 1
               FROM media_items mi
               WHERE mi.item_request_id = item_requests.id
                 AND mi.item_type = ANY(ARRAY['movie'::media_item_type, 'show'::media_item_type])
             )
           )
         ORDER BY created_at ASC",
    )
    .fetch_all(pool)
    .await?)
}

pub async fn derive_item_request_state_for_request(
    pool: &PgPool,
    request: &ItemRequest,
) -> Result<ItemRequestState> {
    let root_item = get_request_root_item(pool, request.id).await?;

    match request.request_type {
        ItemRequestType::Movie => Ok(match root_item {
            Some(item) => match item.state {
                MediaItemState::Completed => ItemRequestState::Completed,
                MediaItemState::Unreleased => ItemRequestState::Unreleased,
                MediaItemState::Failed => ItemRequestState::Failed,
                _ => ItemRequestState::Requested,
            },
            None => ItemRequestState::Requested,
        }),
        ItemRequestType::Show => {
            let Some(show) = root_item else {
                return Ok(ItemRequestState::Requested);
            };

            if let Some(season_numbers) = parse_request_seasons(request).filter(|s| !s.is_empty()) {
                let season_states = sqlx::query_scalar::<_, MediaItemState>(
                    "SELECT state FROM media_items
                     WHERE parent_id = $1
                       AND item_type = 'season'
                       AND is_special = false
                       AND season_number = ANY($2)
                     ORDER BY season_number",
                )
                .bind(show.id)
                .bind(&season_numbers)
                .fetch_all(pool)
                .await?;

                if season_states.is_empty() {
                    return Ok(ItemRequestState::Requested);
                }

                if season_states
                    .iter()
                    .all(|state| *state == MediaItemState::Completed)
                {
                    return Ok(ItemRequestState::Completed);
                }

                let remaining_requested_episodes = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*)
                     FROM media_items episode
                     JOIN media_items season ON episode.parent_id = season.id
                     WHERE season.parent_id = $1
                       AND season.item_type = 'season'
                       AND season.is_special = false
                       AND season.season_number = ANY($2)
                       AND episode.item_type = 'episode'
                       AND episode.is_requested = true
                       AND episode.state <> 'completed'
                       AND episode.state <> 'unreleased'",
                )
                .bind(show.id)
                .bind(&season_numbers)
                .fetch_one(pool)
                .await?;

                if remaining_requested_episodes == 0 {
                    let requested_episode_count = sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*)
                         FROM media_items episode
                         JOIN media_items season ON episode.parent_id = season.id
                         WHERE season.parent_id = $1
                           AND season.item_type = 'season'
                           AND season.is_special = false
                           AND season.season_number = ANY($2)
                           AND episode.item_type = 'episode'
                           AND episode.is_requested = true",
                    )
                    .bind(show.id)
                    .bind(&season_numbers)
                    .fetch_one(pool)
                    .await?;

                    if requested_episode_count > 0 {
                        return Ok(ItemRequestState::Completed);
                    }
                }

                if season_states
                    .iter()
                    .all(|state| *state == MediaItemState::Unreleased)
                {
                    return Ok(ItemRequestState::Unreleased);
                }

                if season_states.contains(&MediaItemState::Failed) {
                    return Ok(ItemRequestState::Failed);
                }

                return Ok(ItemRequestState::Ongoing);
            }

            let remaining_aired_episodes = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)
                 FROM media_items episode
                 JOIN media_items season ON episode.parent_id = season.id
                 WHERE season.parent_id = $1
                   AND season.item_type = 'season'
                   AND season.is_special = false
                   AND episode.item_type = 'episode'
                   AND episode.is_requested = true
                   AND episode.state <> 'completed'
                   AND episode.state <> 'unreleased'",
            )
            .bind(show.id)
            .fetch_one(pool)
            .await?;

            if remaining_aired_episodes == 0 {
                let requested_episode_count = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*)
                     FROM media_items episode
                     JOIN media_items season ON episode.parent_id = season.id
                     WHERE season.parent_id = $1
                       AND season.item_type = 'season'
                       AND season.is_special = false
                       AND episode.item_type = 'episode'
                       AND episode.is_requested = true",
                )
                .bind(show.id)
                .fetch_one(pool)
                .await?;

                if requested_episode_count > 0 {
                    return Ok(ItemRequestState::Completed);
                }
            }

            if show.state == MediaItemState::Unreleased {
                Ok(ItemRequestState::Unreleased)
            } else if show.state == MediaItemState::Failed {
                Ok(ItemRequestState::Failed)
            } else if show.show_status == Some(ShowStatus::Continuing) || show.is_requested {
                Ok(ItemRequestState::Ongoing)
            } else {
                Ok(ItemRequestState::Requested)
            }
        }
    }
}
