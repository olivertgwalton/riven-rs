use anyhow::Result;
use riven_core::entities::{item_requests, media_items};
use riven_core::types::*;
use sea_orm::ActiveValue::{Set, Unchanged};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait,
    QueryFilter, QueryOrder, QuerySelect, Statement,
};

use crate::entities::*;
use crate::orm;

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
pub async fn get_external_request_ids_for_items(media_item_ids: &[i64]) -> Result<Vec<String>> {
    if media_item_ids.is_empty() {
        return Ok(Vec::new());
    }
    // The original JOIN-ed media_items → item_requests; SeaORM can't bind a PG
    // array here (no `postgres-array` feature), so do it in two builder steps:
    // collect the linked request ids, then their non-null external ids.
    let request_ids: Vec<i64> = media_items::Entity::find()
        .filter(media_items::Column::Id.is_in(media_item_ids.iter().copied()))
        .filter(media_items::Column::ItemRequestId.is_not_null())
        .select_only()
        .column(media_items::Column::ItemRequestId)
        .into_tuple::<Option<i64>>()
        .all(orm())
        .await?
        .into_iter()
        .flatten()
        .collect();
    if request_ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(item_requests::Entity::find()
        .filter(item_requests::Column::Id.is_in(request_ids))
        .filter(item_requests::Column::ExternalRequestId.is_not_null())
        .select_only()
        .column(item_requests::Column::ExternalRequestId)
        .into_tuple::<Option<String>>()
        .all(orm())
        .await?
        .into_iter()
        .flatten()
        .collect())
}

pub async fn get_item_request_by_id(id: i64) -> Result<Option<ItemRequest>> {
    Ok(item_requests::Entity::find_by_id(id)
        .into_model::<ItemRequest>()
        .one(orm())
        .await?)
}

/// Fetch the top-level media item (movie or show) associated with an item request.
pub async fn get_request_root_item(request_id: i64) -> Result<Option<MediaItem>> {
    // Prefer a movie over a show when both somehow exist for the same request,
    // matching the original `ORDER BY CASE item_type ...` ranking.
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ItemRequestId.eq(request_id))
        .filter(media_items::Column::ItemType.is_in([MediaItemType::Movie, MediaItemType::Show]))
        .order_by_asc(Expr::cust(
            "CASE item_type \
                 WHEN 'movie'::media_item_type THEN 0 \
                 WHEN 'show'::media_item_type THEN 1 \
                 ELSE 2 END",
        ))
        .into_model::<MediaItem>()
        .one(orm())
        .await?)
}

/// Find an existing item request by any matching external ID.
pub async fn find_existing_item_request(
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<Option<ItemRequest>> {
    // Match any supplied external id. With none supplied there is nothing to
    // match (the old SQL's `= $n AND $n IS NOT NULL` collapsed to false).
    let mut by_external_id = sea_orm::Condition::any();
    if let Some(imdb_id) = imdb_id {
        by_external_id = by_external_id.add(item_requests::Column::ImdbId.eq(imdb_id));
    }
    if let Some(tmdb_id) = tmdb_id {
        by_external_id = by_external_id.add(item_requests::Column::TmdbId.eq(tmdb_id));
    }
    if let Some(tvdb_id) = tvdb_id {
        by_external_id = by_external_id.add(item_requests::Column::TvdbId.eq(tvdb_id));
    }
    if by_external_id.is_empty() {
        return Ok(None);
    }
    Ok(item_requests::Entity::find()
        .filter(by_external_id)
        .into_model::<ItemRequest>()
        .one(orm())
        .await?)
}

pub async fn create_item_request(
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    request_type: ItemRequestType,
    requested_by: Option<&str>,
    external_request_id: Option<&str>,
    seasons: Option<&[i32]>,
) -> Result<UpsertedItemRequest> {
    if let Some(existing) = find_existing_item_request(imdb_id, tmdb_id, tvdb_id).await? {
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
                let bump_state = matches!(
                    existing.state,
                    ItemRequestState::Completed
                        | ItemRequestState::Ongoing
                        | ItemRequestState::Unreleased
                );
                let seasons_json =
                    desired_seasons.map(|values| serde_json::to_value(values).unwrap_or_default());
                // state = CASE WHEN bump THEN 'requested_additional_seasons' ELSE state END:
                // only touch the state column when we're actually bumping it.
                let mut active = item_requests::ActiveModel {
                    id: Unchanged(existing.id),
                    seasons: Set(seasons_json),
                    ..Default::default()
                };
                if bump_state {
                    active.state = Set(ItemRequestState::RequestedAdditionalSeasons);
                }
                active.update(orm()).await?;
                let updated = get_item_request_by_id(existing.id).await?.ok_or_else(|| {
                    anyhow::anyhow!("updated item request {} not found", existing.id)
                })?;
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
    let inserted = item_requests::ActiveModel {
        imdb_id: Set(imdb_id.map(str::to_owned)),
        tmdb_id: Set(tmdb_id.map(str::to_owned)),
        tvdb_id: Set(tvdb_id.map(str::to_owned)),
        request_type: Set(request_type),
        requested_by: Set(requested_by.map(str::to_owned)),
        external_request_id: Set(external_request_id.map(str::to_owned)),
        seasons: Set(seasons_json),
        state: Set(ItemRequestState::Requested),
        ..Default::default()
    }
    .insert(orm())
    .await?;
    let request = get_item_request_by_id(inserted.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("inserted item request {} not found", inserted.id))?;
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
    request_id: i64,
    imdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    tmdb_id: Option<&str>,
) -> Result<()> {
    if imdb_id.is_none() && tvdb_id.is_none() && tmdb_id.is_none() {
        return Ok(());
    }
    // `col = COALESCE(col, $n)` only fills the column when it is currently NULL,
    // never clobbering a user-supplied id. When the new value is None the
    // column is left untouched entirely (the old query bound NULL, and
    // COALESCE(col, NULL) is a no-op).
    let mut update = item_requests::Entity::update_many();
    if let Some(v) = imdb_id {
        update = update.col_expr(
            item_requests::Column::ImdbId,
            Expr::cust_with_values("COALESCE(imdb_id, $1)", [v.to_owned()]),
        );
    }
    if let Some(v) = tvdb_id {
        update = update.col_expr(
            item_requests::Column::TvdbId,
            Expr::cust_with_values("COALESCE(tvdb_id, $1)", [v.to_owned()]),
        );
    }
    if let Some(v) = tmdb_id {
        update = update.col_expr(
            item_requests::Column::TmdbId,
            Expr::cust_with_values("COALESCE(tmdb_id, $1)", [v.to_owned()]),
        );
    }
    update
        .filter(item_requests::Column::Id.eq(request_id))
        .exec(orm())
        .await?;
    Ok(())
}

/// Recompute `is_partial_request` after the indexer has populated the show's
/// seasons. A request is "partial" when the user requested a strict subset
/// of the non-special seasons that actually exist on the show. Special
/// seasons (`season_number = 0`, `is_special = true`) are excluded from the
/// denominator.
pub async fn recompute_is_partial_request(request_id: i64, show_id: i64) -> Result<()> {
    // The whole RHS is a correlated subquery/jsonb expression, so keep it raw
    // inside `col_expr`. `seasons` is JSONB (not an enum) and the inner count
    // queries plain columns, so no `::text` casts are required.
    item_requests::Entity::update_many()
        .col_expr(
            item_requests::Column::IsPartialRequest,
            Expr::cust_with_values(
                r#"(
                    CASE
                        WHEN seasons IS NULL THEN false
                        WHEN jsonb_array_length(seasons) = 0 THEN false
                        ELSE jsonb_array_length(seasons) < (
                            SELECT COUNT(*) FROM media_items
                             WHERE parent_id = $1
                               AND item_type = 'season'
                               AND COALESCE(is_special, false) = false
                        )
                    END
                )"#,
                [show_id],
            ),
        )
        .filter(item_requests::Column::Id.eq(request_id))
        .exec(orm())
        .await?;
    Ok(())
}

pub async fn update_item_request_state(id: i64, state: ItemRequestState) -> Result<ItemRequest> {
    // completed_at = CASE WHEN <new state> = 'completed'
    //                     THEN COALESCE(completed_at, NOW()) ELSE completed_at END.
    // The CASE only fires when we are transitioning to Completed, and COALESCE
    // preserves any earlier timestamp. Done in one statement so it stays atomic;
    // re-fetch afterwards to return the public struct (RETURNING * equivalent).
    let mut update = item_requests::Entity::update_many()
        .col_expr(item_requests::Column::State, state.as_enum());
    if state == ItemRequestState::Completed {
        update = update.col_expr(
            item_requests::Column::CompletedAt,
            Expr::cust("COALESCE(completed_at, NOW())"),
        );
    }
    update
        .filter(item_requests::Column::Id.eq(id))
        .exec(orm())
        .await?;
    get_item_request_by_id(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("updated item request {id} not found"))
}

pub async fn get_retryable_item_requests() -> Result<Vec<ItemRequest>> {
    // state IN ('requested','failed') AND (state='failed' OR no movie/show
    // media_item exists for this request). The NOT EXISTS correlated subquery
    // stays raw inside an `Expr::cust` filter.
    Ok(item_requests::Entity::find()
        .filter(
            item_requests::Column::State
                .is_in([ItemRequestState::Requested, ItemRequestState::Failed]),
        )
        .filter(Expr::cust(
            "(state = 'failed' OR NOT EXISTS ( \
                 SELECT 1 FROM media_items mi \
                 WHERE mi.item_request_id = item_requests.id \
                   AND mi.item_type = ANY(ARRAY['movie'::media_item_type, 'show'::media_item_type]) \
             ))",
        ))
        .order_by_asc(item_requests::Column::CreatedAt)
        .into_model::<ItemRequest>()
        .all(orm())
        .await?)
}

/// Run one of the COUNT(*) episode rollup queries. `season_numbers`, when set,
/// is inlined into the SQL (plain integers, no injection risk) because SeaORM
/// can't bind a PG array without the `postgres-array` feature.
async fn count_episodes_raw(
    sql: &str,
    show_id: i64,
    season_numbers: Option<&[i32]>,
) -> Result<i64> {
    let sql = match season_numbers {
        Some(nums) => {
            let list = nums
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            sql.replace("ANY($2)", &format!("ANY(ARRAY[{list}]::int[])"))
        }
        None => sql.to_owned(),
    };
    let row = orm()
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            &sql,
            [show_id.into()],
        ))
        .await?;
    match row {
        Some(row) => Ok(row.try_get::<i64>("", "count")?),
        None => Ok(0),
    }
}

pub async fn derive_item_request_state_for_request(
    request: &ItemRequest,
) -> Result<ItemRequestState> {
    let root_item = get_request_root_item(request.id).await?;

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
                let season_states: Vec<MediaItemState> = media_items::Entity::find()
                    .filter(media_items::Column::ParentId.eq(show.id))
                    .filter(media_items::Column::ItemType.eq(MediaItemType::Season))
                    .filter(media_items::Column::IsSpecial.eq(false))
                    .filter(media_items::Column::SeasonNumber.is_in(season_numbers.iter().copied()))
                    .order_by_asc(media_items::Column::SeasonNumber)
                    .select_only()
                    .column(media_items::Column::State)
                    .into_tuple()
                    .all(orm())
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

                let remaining_requested_episodes = count_episodes_raw(
                    "SELECT COUNT(*) AS count
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
                    show.id,
                    Some(&season_numbers),
                )
                .await?;

                if remaining_requested_episodes == 0 {
                    let requested_episode_count = count_episodes_raw(
                        "SELECT COUNT(*) AS count
                         FROM media_items episode
                         JOIN media_items season ON episode.parent_id = season.id
                         WHERE season.parent_id = $1
                           AND season.item_type = 'season'
                           AND season.is_special = false
                           AND season.season_number = ANY($2)
                           AND episode.item_type = 'episode'
                           AND episode.is_requested = true",
                        show.id,
                        Some(&season_numbers),
                    )
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

            let remaining_aired_episodes = count_episodes_raw(
                "SELECT COUNT(*) AS count
                 FROM media_items episode
                 JOIN media_items season ON episode.parent_id = season.id
                 WHERE season.parent_id = $1
                   AND season.item_type = 'season'
                   AND season.is_special = false
                   AND episode.item_type = 'episode'
                   AND episode.is_requested = true
                   AND episode.state <> 'completed'
                   AND episode.state <> 'unreleased'",
                show.id,
                None,
            )
            .await?;

            if remaining_aired_episodes == 0 {
                let requested_episode_count = count_episodes_raw(
                    "SELECT COUNT(*) AS count
                     FROM media_items episode
                     JOIN media_items season ON episode.parent_id = season.id
                     WHERE season.parent_id = $1
                       AND season.item_type = 'season'
                       AND season.is_special = false
                       AND episode.item_type = 'episode'
                       AND episode.is_requested = true",
                    show.id,
                    None,
                )
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
