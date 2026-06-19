use anyhow::Result;
use chrono::Utc;
use riven_core::entities::{item_requests, media_item_blacklisted_streams, media_items, streams};
use riven_core::types::*;
use sea_orm::ActiveValue::{NotSet, Set, Unchanged};
use sea_orm::sea_query::{Expr, NullOrdering, OnConflict};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, EntityTrait, Order, QueryFilter, QueryOrder,
    QuerySelect,
};

use crate::entities::*;
use crate::orm;

/// Shared INSERT … ON CONFLICT … RETURNING * implementation for top-level items
/// (movies and shows). `second_id_col` is either `"tmdb_id"` or `"tvdb_id"`;
/// `item_type` is either `"movie"` or `"show"`.
async fn upsert_top_level_item(
    title: &str,
    imdb_id: Option<&str>,
    second_id_col: &'static str,
    second_id_val: Option<&str>,
    item_type: &'static str,
    item_request_id: Option<i64>,
    is_requested: bool,
) -> Result<(MediaItem, bool)> {
    let (type_val, tmdb_id, tvdb_id) = match item_type {
        "movie" => (MediaItemType::Movie, second_id_val, None),
        _ => (MediaItemType::Show, None, second_id_val),
    };
    if let Some(existing) =
        find_existing_media_item(type_val, imdb_id, tmdb_id, tvdb_id).await?
    {
        let needs_update = is_requested
            && (!existing.is_requested
                || (item_request_id.is_some() && existing.item_request_id != item_request_id));
        if needs_update {
            media_items::ActiveModel {
                id: Unchanged(existing.id),
                is_requested: Set(true),
                // COALESCE($1, item_request_id): keep the existing id when None.
                item_request_id: item_request_id.map_or(NotSet, |v| Set(Some(v))),
                updated_at: Set(Some(Utc::now())),
                ..Default::default()
            }
            .update(orm())
            .await?;
            super::state::recompute(&[existing.id]).await?;
        }
        return Ok((existing, false));
    }
    let _ = second_id_col;
    let inserted = media_items::ActiveModel {
        title: Set(title.to_owned()),
        imdb_id: Set(imdb_id.map(str::to_owned)),
        tmdb_id: Set(tmdb_id.map(str::to_owned)),
        tvdb_id: Set(tvdb_id.map(str::to_owned)),
        item_type: Set(type_val),
        state: Set(MediaItemState::Indexed),
        is_requested: Set(is_requested),
        created_at: Set(Utc::now()),
        item_request_id: Set(item_request_id),
        ..Default::default()
    }
    .insert(orm())
    .await?;
    super::state::recompute(&[inserted.id]).await?;
    let item = get_media_item(inserted.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("inserted media item {} not found", inserted.id))?;
    Ok((item, true))
}

pub async fn get_media_item(id: i64) -> Result<Option<MediaItem>> {
    Ok(media_items::Entity::find_by_id(id)
        .into_model::<MediaItem>()
        .one(orm())
        .await?)
}

pub async fn list_media_items_by_ids(ids: &[i64]) -> Result<Vec<MediaItem>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(media_items::Entity::find()
        .filter(media_items::Column::Id.is_in(ids.iter().copied()))
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

async fn find_one_by(column: media_items::Column, value: &str) -> Result<Option<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(column.eq(value))
        .into_model::<MediaItem>()
        .one(orm())
        .await?)
}

pub async fn get_media_item_by_imdb(id: &str) -> Result<Option<MediaItem>> {
    find_one_by(media_items::Column::ImdbId, id).await
}

pub async fn get_media_item_by_tmdb(id: &str) -> Result<Option<MediaItem>> {
    find_one_by(media_items::Column::TmdbId, id).await
}

pub async fn get_media_item_by_tvdb(id: &str) -> Result<Option<MediaItem>> {
    find_one_by(media_items::Column::TvdbId, id).await
}

async fn list_by_type(item_type: MediaItemType) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ItemType.eq(item_type))
        .order_by_asc(media_items::Column::Title)
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

pub async fn list_movies() -> Result<Vec<MediaItem>> {
    list_by_type(MediaItemType::Movie).await
}

pub async fn list_shows() -> Result<Vec<MediaItem>> {
    list_by_type(MediaItemType::Show).await
}

pub async fn get_items_by_state(
    state: MediaItemState,
    item_type: MediaItemType,
) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::State.eq(state))
        .filter(media_items::Column::ItemType.eq(item_type))
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

/// Escalating cooldown applied to items with `failed_attempts > 0` so a
/// repeatedly-failing item doesn't get re-enqueued every retry cycle and
/// starve fresh content.
pub(crate) const FAILED_ATTEMPTS_COOLDOWN_SQL: &str = "(
    failed_attempts = 0
    OR last_scrape_attempt_at IS NULL
    OR last_scrape_attempt_at < NOW() - (CASE
        WHEN failed_attempts >= 10 THEN INTERVAL '24 hours'
        WHEN failed_attempts >= 5  THEN INTERVAL '6 hours'
        WHEN failed_attempts >= 2  THEN INTERVAL '2 hours'
        ELSE INTERVAL '30 minutes'
    END)
)";

/// Fetch all pending top-level items needing a retry: Indexed, Scraped, or PartiallyCompleted.
pub async fn get_pending_items_for_retry(item_type: MediaItemType) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::State.is_in([
            MediaItemState::Indexed,
            MediaItemState::Scraped,
            MediaItemState::PartiallyCompleted,
        ]))
        .filter(media_items::Column::ItemType.eq(item_type))
        .filter(media_items::Column::IsRequested.eq(true))
        .filter(Expr::cust(FAILED_ATTEMPTS_COOLDOWN_SQL))
        .order_by_asc(media_items::Column::FailedAttempts)
        .order_by_with_nulls(
            media_items::Column::LastScrapeAttemptAt,
            Order::Asc,
            NullOrdering::First,
        )
        .order_by_asc(media_items::Column::CreatedAt)
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

/// IDs of shows/seasons currently in `ongoing`. The retry scheduler re-derives
/// these each cycle: rollup-rule changes and crash-window drift don't rewrite
/// settled rows on their own, and a stale `ongoing` is invisible to
/// [`get_pending_items_for_retry`].
pub async fn get_ongoing_container_ids() -> Result<Vec<i64>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::State.eq(MediaItemState::Ongoing))
        .filter(
            media_items::Column::ItemType
                .is_in([MediaItemType::Show, MediaItemType::Season]),
        )
        .select_only()
        .column(media_items::Column::Id)
        .into_tuple::<i64>()
        .all(orm())
        .await?)
}

/// Return the earliest requested unreleased descendant air date for a show.
pub async fn get_next_unreleased_air_date_for_show(
    show_id: i64,
) -> Result<Option<chrono::NaiveDate>> {
    // Scalar MIN aggregate; only the `parent_id IN (SELECT season ...)` self-
    // referencing subquery (matching episodes under any of the show's seasons)
    // stays raw inside the filter.
    let min: Option<Option<chrono::NaiveDate>> = media_items::Entity::find()
        .select_only()
        .column_as(media_items::Column::AiredAt.min(), "min")
        .filter(media_items::Column::AiredAt.is_not_null())
        .filter(media_items::Column::State.eq(MediaItemState::Unreleased))
        .filter(media_items::Column::IsRequested.eq(true))
        .filter(Expr::cust_with_values(
            "(parent_id = $1 OR parent_id IN ( \
                 SELECT season.id FROM media_items season WHERE season.parent_id = $1 \
             ))",
            [show_id],
        ))
        .into_tuple()
        .one(orm())
        .await?;
    Ok(min.flatten())
}

/// Find an existing media item by type and any matching external ID.
pub async fn find_existing_media_item(
    item_type: MediaItemType,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<Option<MediaItem>> {
    // Match any supplied external id. With none supplied there is nothing to
    // match (the old SQL's `= $n AND $n IS NOT NULL` collapsed to false).
    let mut by_external_id = Condition::any();
    if let Some(imdb_id) = imdb_id {
        by_external_id = by_external_id.add(media_items::Column::ImdbId.eq(imdb_id));
    }
    if let Some(tmdb_id) = tmdb_id {
        by_external_id = by_external_id.add(media_items::Column::TmdbId.eq(tmdb_id));
    }
    if let Some(tvdb_id) = tvdb_id {
        by_external_id = by_external_id.add(media_items::Column::TvdbId.eq(tvdb_id));
    }
    if by_external_id.is_empty() {
        return Ok(None);
    }
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ItemType.eq(item_type))
        .filter(by_external_id)
        .into_model::<MediaItem>()
        .one(orm())
        .await?)
}

/// Returns `(item, was_created)`. `was_created` is false when an existing item was found.
pub async fn create_movie(
    title: &str,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    item_request_id: Option<i64>,
) -> Result<(MediaItem, bool)> {
    upsert_top_level_item(
        title,
        imdb_id,
        "tmdb_id",
        tmdb_id,
        "movie",
        item_request_id,
        true,
    )
    .await
}

/// Returns `(item, was_created)`. `was_created` is false when an existing item was found.
pub async fn create_show(
    title: &str,
    imdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    item_request_id: Option<i64>,
) -> Result<(MediaItem, bool)> {
    upsert_top_level_item(
        title,
        imdb_id,
        "tvdb_id",
        tvdb_id,
        "show",
        item_request_id,
        true,
    )
    .await
}

pub async fn create_movie_unrequested(
    title: &str,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
) -> Result<(MediaItem, bool)> {
    upsert_top_level_item(title, imdb_id, "tmdb_id", tmdb_id, "movie", None, false).await
}

pub async fn create_show_unrequested(
    title: &str,
    imdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<(MediaItem, bool)> {
    upsert_top_level_item(title, imdb_id, "tvdb_id", tvdb_id, "show", None, false).await
}

pub(crate) fn to_json<T: serde::Serialize>(v: &T) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or_default()
}

pub async fn update_media_item_index(
    id: i64,
    indexed: &riven_core::types::IndexedMediaItem,
) -> Result<()> {
    let now = Utc::now();
    // `COALESCE($n, col)` keeps the existing value when the new one is NULL —
    // which is exactly SeaORM's `Set` vs `NotSet`: a `None` field leaves the
    // column untouched.
    let opt_str =
        |o: Option<&str>| o.map_or(NotSet, |v| Set(Some(v.to_owned())));
    media_items::ActiveModel {
        id: Unchanged(id),
        title: indexed.title.clone().map_or(NotSet, Set),
        full_title: opt_str(indexed.full_title.as_deref()),
        imdb_id: opt_str(indexed.imdb_id.as_deref()),
        tvdb_id: opt_str(indexed.tvdb_id.as_deref()),
        tmdb_id: opt_str(indexed.tmdb_id.as_deref()),
        poster_path: opt_str(indexed.poster_path.as_deref()),
        year: indexed.year.map_or(NotSet, |v| Set(Some(v))),
        genres: indexed
            .genres
            .as_ref()
            .map_or(NotSet, |g| Set(Some(to_json(g)))),
        country: opt_str(indexed.country.as_deref()),
        language: opt_str(indexed.language.as_deref()),
        network: opt_str(indexed.network.as_deref()),
        content_rating: indexed.content_rating.map_or(NotSet, |v| Set(Some(v))),
        is_anime: Set(indexed.inferred_is_anime()),
        runtime: indexed.runtime.map_or(NotSet, |v| Set(Some(v))),
        aliases: indexed
            .aliases
            .as_ref()
            .map_or(NotSet, |a| Set(Some(to_json(a)))),
        aired_at: indexed.aired_at.map_or(NotSet, |v| Set(Some(v))),
        show_status: indexed.status.map_or(NotSet, |v| Set(Some(v))),
        rating: indexed.rating.map_or(NotSet, |v| Set(Some(v))),
        network_timezone: opt_str(indexed.network_timezone.as_deref()),
        indexed_at: Set(Some(now)),
        updated_at: Set(Some(now)),
        failed_attempts: Set(0),
        ..Default::default()
    }
    .update(orm())
    .await?;
    super::state::recompute(&[id]).await?;
    Ok(())
}

pub async fn set_active_stream(id: i64, stream_id: i64) -> Result<()> {
    media_items::ActiveModel {
        id: Unchanged(id),
        active_stream_id: Set(Some(stream_id)),
        updated_at: Set(Some(Utc::now())),
        ..Default::default()
    }
    .update(orm())
    .await?;
    Ok(())
}

pub async fn update_scraped(id: i64) -> Result<()> {
    media_items::Entity::update_many()
        .col_expr(media_items::Column::ScrapedAt, Expr::cust("NOW()"))
        .col_expr(
            media_items::Column::ScrapedTimes,
            Expr::col(media_items::Column::ScrapedTimes).add(1),
        )
        .col_expr(media_items::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(media_items::Column::Id.eq(id))
        .exec(orm())
        .await?;
    Ok(())
}

/// Feed unreleased items whose air date has passed through the state
/// derivation pipeline. Like the TS MediaItemStateSubscriber, this does NOT
/// manually set the state — it just identifies the candidates and lets
/// `recompute` derive the correct state from first principles (so an episode
/// with existing streams lands on `Scraped`, not just `Indexed`). The cascade
/// inside `recompute` then propagates the change up to the season and show.
pub async fn transition_unreleased_aired() -> Result<Vec<i64>> {
    let today = Utc::now().date_naive();
    let ids: Vec<i64> = media_items::Entity::find()
        .filter(media_items::Column::State.eq(MediaItemState::Unreleased))
        .filter(media_items::Column::AiredAt.is_not_null())
        .filter(media_items::Column::AiredAt.lte(today))
        .filter(media_items::Column::IsRequested.eq(true))
        .select_only()
        .column(media_items::Column::Id)
        .into_tuple()
        .all(orm())
        .await?;
    super::state::recompute(&ids).await?;
    Ok(ids)
}

pub async fn blacklist_stream_by_hash(media_item_id: i64, info_hash: &str) -> Result<()> {
    let stream_id: Option<i64> = streams::Entity::find()
        .filter(streams::Column::InfoHash.eq(info_hash))
        .select_only()
        .column(streams::Column::Id)
        .into_tuple()
        .one(orm())
        .await?;
    if let Some(stream_id) = stream_id {
        let insert = media_item_blacklisted_streams::Entity::insert(
            media_item_blacklisted_streams::ActiveModel {
                media_item_id: Set(media_item_id),
                stream_id: Set(stream_id),
                ..Default::default()
            },
        )
        .on_conflict(
            OnConflict::columns([
                media_item_blacklisted_streams::Column::MediaItemId,
                media_item_blacklisted_streams::Column::StreamId,
            ])
            .do_nothing()
            .to_owned(),
        );
        // `do_nothing` + an existing row surfaces as `RecordNotInserted`; that
        // is the ON CONFLICT DO NOTHING no-op, not an error.
        match insert.exec(orm()).await {
            Ok(_) | Err(sea_orm::DbErr::RecordNotInserted) => {}
            Err(error) => return Err(error.into()),
        }
        super::state::recompute(&[media_item_id]).await?;
    }
    Ok(())
}

pub async fn increment_failed_attempts(id: i64) -> Result<()> {
    media_items::Entity::update_many()
        .col_expr(
            media_items::Column::FailedAttempts,
            Expr::col(media_items::Column::FailedAttempts).add(1),
        )
        .col_expr(media_items::Column::LastScrapeAttemptAt, Expr::cust("NOW()"))
        .col_expr(media_items::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(media_items::Column::Id.eq(id))
        .exec(orm())
        .await?;
    super::state::recompute(&[id]).await?;
    Ok(())
}

pub async fn reset_failed_attempts(id: i64) -> Result<()> {
    media_items::ActiveModel {
        id: Unchanged(id),
        failed_attempts: Set(0),
        updated_at: Set(Some(Utc::now())),
        ..Default::default()
    }
    .update(orm())
    .await?;
    super::state::recompute(&[id]).await?;
    Ok(())
}

/// Delete top-level items (movies/shows) whose content-service request is no
/// longer active.  Seasons and episodes cascade via the DB foreign key.
/// Items that were added manually (no `item_request_id`) are never touched.
///
/// `active_external_ids` is the union of every `external_request_id` returned
/// by all content-service plugins in the current run.
pub async fn delete_items_removed_from_content_services(
    active_external_ids: &[String],
) -> Result<u64> {
    let db = orm();
    // Requests whose external id is no longer reported by any content service.
    // An empty active set means "nothing is active", so every externally-sourced
    // request is stale (matching the original `NOT (... = ANY('{}'))`).
    let mut stale = item_requests::Entity::find()
        .filter(item_requests::Column::ExternalRequestId.is_not_null());
    if !active_external_ids.is_empty() {
        stale = stale.filter(
            item_requests::Column::ExternalRequestId.is_not_in(active_external_ids.iter().cloned()),
        );
    }
    let stale_ids: Vec<i64> = stale
        .select_only()
        .column(item_requests::Column::Id)
        .into_tuple()
        .all(db)
        .await?;
    if stale_ids.is_empty() {
        return Ok(0);
    }

    // Seasons/episodes cascade via the FK; manually-added items (no request id)
    // are never matched.
    let deleted = media_items::Entity::delete_many()
        .filter(media_items::Column::ItemType.is_in([MediaItemType::Movie, MediaItemType::Show]))
        .filter(media_items::Column::ItemRequestId.is_in(stale_ids.iter().copied()))
        .exec(db)
        .await?;
    item_requests::Entity::delete_many()
        .filter(item_requests::Column::Id.is_in(stale_ids.iter().copied()))
        .exec(db)
        .await?;
    Ok(deleted.rows_affected)
}

pub async fn add_media_item_unrequested(
    item_type: MediaItemType,
    title: String,
    imdb_id: Option<String>,
    tmdb_id: Option<String>,
    tvdb_id: Option<String>,
) -> Result<MediaItem> {
    match item_type {
        MediaItemType::Movie => {
            create_movie_unrequested(&title, imdb_id.as_deref(), tmdb_id.as_deref())
                .await
                .map(|(item, _)| item)
        }
        MediaItemType::Show => {
            create_show_unrequested(&title, imdb_id.as_deref(), tvdb_id.as_deref())
                .await
                .map(|(item, _)| item)
        }
        _ => Err(anyhow::anyhow!(
            "Only Movie and Show types can be added directly"
        )),
    }
}
