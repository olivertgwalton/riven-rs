use anyhow::Result;
use chrono::Utc;
use riven_core::entities::media_items;
use riven_core::types::*;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, PaginatorTrait,
    QueryFilter, QueryOrder, Statement,
};

use crate::entities::*;
use crate::orm;

pub async fn list_seasons(show_id: i64) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ItemType.eq(MediaItemType::Season))
        .filter(media_items::Column::ParentId.eq(show_id))
        .order_by_asc(media_items::Column::SeasonNumber)
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

/// List seasons for a show, excluding season 0 (specials).
pub async fn list_seasons_excluding_specials(show_id: i64) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ItemType.eq(MediaItemType::Season))
        .filter(media_items::Column::ParentId.eq(show_id))
        .filter(
            media_items::Column::SeasonNumber
                .is_null()
                .or(media_items::Column::SeasonNumber.ne(0)),
        )
        .order_by_asc(media_items::Column::SeasonNumber)
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

/// Find an episode by the parent show's TVDB ID, episode number, and optional season number.
/// When `season_number` is `None`, looks up by absolute episode numbering.
pub async fn find_episode_by_show_tvdb(
    tvdb_id: &str,
    episode_number: i32,
    season_number: Option<i32>,
) -> Result<Option<MediaItem>> {
    // Two-level self-join (episode→season→show) plus a CASE that switches the
    // match between absolute and season/episode numbering — keep raw. Enum
    // columns cast to ::text for the MediaItem projection.
    let sql = r#"SELECT ep.*,
                  ep.content_rating::text AS content_rating,
                  ep.state::text          AS state,
                  ep.item_type::text      AS item_type,
                  ep.show_status::text    AS show_status
           FROM media_items ep
           INNER JOIN media_items season ON season.id = ep.parent_id AND season.item_type = 'season'
           INNER JOIN media_items show ON show.id = season.parent_id AND show.item_type = 'show'
           WHERE ep.item_type = 'episode'
             AND show.tvdb_id = $1
             AND (
               CASE WHEN $3::integer IS NULL
                    THEN ep.absolute_number = $2
                    ELSE ep.episode_number = $2 AND season.season_number = $3
               END
             )
           ORDER BY season.season_number, ep.episode_number
           LIMIT 1"#;
    Ok(MediaItem::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        [tvdb_id.into(), episode_number.into(), season_number.into()],
    ))
    .one(orm())
    .await?)
}

/// Count episodes in a season.
pub async fn count_episodes_in_season(season_id: i64) -> Result<i64> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ItemType.eq(MediaItemType::Episode))
        .filter(media_items::Column::ParentId.eq(season_id))
        .count(orm())
        .await
        .map(|c| i64::try_from(c).unwrap_or(i64::MAX))?)
}

/// Count the total expected downloadable episode files for a show.
///
/// Only processable seasons (not unreleased/ongoing, not specials, is_requested) are counted.
/// For continuing shows the last season is excluded (still airing).
///
/// Executes as a single SQL query.
pub async fn count_expected_files_for_show(show_id: i64) -> Result<i64> {
    let sql = r#"WITH qualifying_seasons AS (
               SELECT
                   id,
                   ROW_NUMBER() OVER (ORDER BY season_number ASC) AS rn
               FROM media_items
               WHERE parent_id = $1
                 AND item_type  = 'season'
                 AND is_requested = true
                 AND is_special   = false
                 AND state NOT IN ('unreleased', 'ongoing')
           ),
           show_info AS (
               SELECT COALESCE(show_status = 'continuing', false) AS is_continuing
               FROM media_items
               WHERE id = $1 AND item_type = 'show'
           ),
           season_cap AS (
               SELECT CASE
                   WHEN (SELECT is_continuing FROM show_info)
                   THEN GREATEST(1, COUNT(*) - 1)
                   ELSE COUNT(*)
               END AS cap
               FROM qualifying_seasons
           )
           SELECT COALESCE(COUNT(e.id), 0) AS count
           FROM qualifying_seasons qs
           JOIN media_items e ON e.parent_id = qs.id AND e.item_type = 'episode'
           WHERE qs.rn <= COALESCE((SELECT cap FROM season_cap), 0)"#;
    let row = orm()
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [show_id.into()],
        ))
        .await?;
    match row {
        Some(row) => Ok(row.try_get::<i64>("", "count")?),
        None => Ok(0),
    }
}

/// Return true if `item_id` is equal to `target_id` or is a descendant of it
/// (i.e. following parent_id links from `item_id` eventually reaches `target_id`).
///
/// Uses a recursive CTE — one query regardless of hierarchy depth.
pub async fn is_item_descendant_of(item_id: i64, target_id: i64) -> Result<bool> {
    let sql = r#"WITH RECURSIVE ancestors AS (
               SELECT id, parent_id FROM media_items WHERE id = $1
               UNION ALL
               SELECT m.id, m.parent_id
               FROM media_items m
               INNER JOIN ancestors a ON m.id = a.parent_id
           )
           SELECT EXISTS(SELECT 1 FROM ancestors WHERE id = $2) AS exists"#;
    let row = orm()
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [item_id.into(), target_id.into()],
        ))
        .await?;
    match row {
        Some(row) => Ok(row.try_get::<bool>("", "exists")?),
        None => Ok(false),
    }
}

pub async fn list_episodes(season_id: i64) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ItemType.eq(MediaItemType::Episode))
        .filter(media_items::Column::ParentId.eq(season_id))
        .order_by_asc(media_items::Column::EpisodeNumber)
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

pub async fn get_media_item_hierarchy(id: i64) -> Result<Option<MediaItemHierarchy>> {
    // `item.*` is followed by explicit `::text` aliases for every PG enum column
    // so the nested `MediaItem` projection (and the resolved_show enum) decode
    // through DeriveActiveEnum's text path. resolved_show_content_rating is the
    // only enum among the resolved_* columns.
    let sql = r#"SELECT
               item.*,
               item.content_rating::text AS content_rating,
               item.state::text          AS state,
               item.item_type::text      AS item_type,
               item.show_status::text    AS show_status,
               season.id AS resolved_season_id,
               season.season_number AS resolved_season_number,
               show_item.id AS resolved_show_id,
               show_item.title AS resolved_show_title,
               show_item.imdb_id AS resolved_show_imdb_id,
               show_item.tvdb_id AS resolved_show_tvdb_id,
               show_item.year AS resolved_show_year,
               show_item.aliases AS resolved_show_aliases,
               show_item.genres AS resolved_show_genres,
               show_item.network AS resolved_show_network,
               show_item.rating AS resolved_show_rating,
               show_item.content_rating::text AS resolved_show_content_rating,
               show_item.language AS resolved_show_language,
               show_item.country AS resolved_show_country,
               show_item.is_anime AS resolved_show_is_anime
           FROM media_items item
           LEFT JOIN media_items season
             ON (
                    (item.item_type = 'episode' AND item.parent_id = season.id AND season.item_type = 'season')
                 OR (item.item_type = 'season' AND item.id = season.id)
                )
           LEFT JOIN media_items show_item
             ON (
                    (item.item_type = 'show' AND item.id = show_item.id)
                 OR (item.item_type = 'season' AND item.parent_id = show_item.id AND show_item.item_type = 'show')
                 OR (item.item_type = 'episode' AND season.parent_id = show_item.id AND show_item.item_type = 'show')
                )
           WHERE item.id = $1"#;
    Ok(
        MediaItemHierarchy::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [id.into()],
        ))
        .one(orm())
        .await?,
    )
}

pub async fn create_season(
    show_id: i64,
    number: i32,
    title: Option<&str>,
    tvdb_id: Option<&str>,
    is_special: bool,
    item_request_id: Option<i64>,
    is_requested: bool,
) -> Result<MediaItem> {
    let now = Utc::now();
    let default_title = format!("Season {number:02}");
    let title_str = title.unwrap_or(&default_title);
    // INSERT … ON CONFLICT (parent_id, season_number) WHERE item_type = 'season'
    // DO UPDATE … RETURNING *. The partial-index conflict target and the
    // RETURNING-with-enums make this clearest as a raw statement (enum columns
    // cast to ::text for the MediaItem projection).
    let sql = r#"INSERT INTO media_items (title, tvdb_id, item_type, state, season_number, is_special, parent_id, is_requested, created_at, item_request_id)
           VALUES ($1, $2, 'season', 'indexed', $3, $4, $5, $6, $7, $8)
           ON CONFLICT (parent_id, season_number) WHERE item_type = 'season'
           DO UPDATE SET
               title        = EXCLUDED.title,
               tvdb_id      = COALESCE(EXCLUDED.tvdb_id, media_items.tvdb_id),
               is_requested = EXCLUDED.is_requested OR media_items.is_requested,
               updated_at   = NOW()
           RETURNING media_items.*,
               media_items.content_rating::text AS content_rating,
               media_items.state::text          AS state,
               media_items.item_type::text      AS item_type,
               media_items.show_status::text    AS show_status"#;
    let item = MediaItem::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        [
            title_str.into(),
            tvdb_id.into(),
            number.into(),
            is_special.into(),
            show_id.into(),
            is_requested.into(),
            now.into(),
            item_request_id.into(),
        ],
    ))
    .one(orm())
    .await?
    .ok_or_else(|| anyhow::anyhow!("create_season upsert returned no row"))?;
    super::state::recompute(&[item.id]).await?;
    Ok(item)
}

pub async fn create_episode(
    season_id: i64,
    number: i32,
    title: Option<&str>,
    tvdb_id: Option<&str>,
    aired_at: Option<chrono::NaiveDate>,
    aired_at_utc: Option<chrono::DateTime<Utc>>,
    runtime: Option<i32>,
    absolute_number: Option<i32>,
    item_request_id: Option<i64>,
    is_requested: bool,
    season_number: Option<i32>,
) -> Result<MediaItem> {
    let now = Utc::now();
    let default_title = format!("Episode {number:02}");
    let title_str = title.unwrap_or(&default_title);
    let state = match aired_at_utc
        .or_else(|| aired_at.map(|d| d.and_hms_opt(0, 0, 0).expect("midnight is valid").and_utc()))
    {
        Some(dt) if dt > now => MediaItemState::Unreleased,
        _ => MediaItemState::Indexed,
    };
    let state_text = match state {
        MediaItemState::Unreleased => "unreleased",
        _ => "indexed",
    };
    // INSERT … ON CONFLICT (parent_id, episode_number) WHERE item_type =
    // 'episode' DO UPDATE … RETURNING *. Raw statement for the partial-index
    // conflict target and the enum-bearing RETURNING. The bound state is passed
    // as text and cast to media_item_state in the VALUES clause.
    let sql = r#"INSERT INTO media_items (title, tvdb_id, item_type, state, episode_number, absolute_number, runtime, parent_id, aired_at, aired_at_utc, is_requested, season_number, created_at, item_request_id)
           VALUES ($1, $2, 'episode', $3::media_item_state, $4, $5, $6, $7, $8, $13, $9, $10, $11, $12)
           ON CONFLICT (parent_id, episode_number) WHERE item_type = 'episode'
           DO UPDATE SET
               title           = EXCLUDED.title,
               tvdb_id         = COALESCE(EXCLUDED.tvdb_id, media_items.tvdb_id),
               aired_at        = COALESCE(EXCLUDED.aired_at, media_items.aired_at),
               aired_at_utc    = COALESCE(EXCLUDED.aired_at_utc, media_items.aired_at_utc),
               runtime         = COALESCE(EXCLUDED.runtime, media_items.runtime),
               absolute_number = COALESCE(EXCLUDED.absolute_number, media_items.absolute_number),
               season_number   = COALESCE(EXCLUDED.season_number, media_items.season_number),
               is_requested    = EXCLUDED.is_requested OR media_items.is_requested,
               -- Transition unreleased episodes that have since aired, preferring precise UTC time
               state = CASE
                   WHEN media_items.state = 'unreleased'
                    AND COALESCE(EXCLUDED.aired_at_utc, EXCLUDED.aired_at::timestamptz) IS NOT NULL
                    AND COALESCE(EXCLUDED.aired_at_utc, EXCLUDED.aired_at::timestamptz) <= NOW()
                   THEN 'indexed'::media_item_state
                   ELSE media_items.state
               END,
               updated_at = NOW()
           RETURNING media_items.*,
               media_items.content_rating::text AS content_rating,
               media_items.state::text          AS state,
               media_items.item_type::text      AS item_type,
               media_items.show_status::text    AS show_status"#;
    let item = MediaItem::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        [
            title_str.into(),
            tvdb_id.into(),
            state_text.into(),
            number.into(),
            absolute_number.into(),
            runtime.into(),
            season_id.into(),
            aired_at.into(),
            is_requested.into(),
            season_number.into(),
            now.into(),
            item_request_id.into(),
            aired_at_utc.into(),
        ],
    ))
    .one(orm())
    .await?
    .ok_or_else(|| anyhow::anyhow!("create_episode upsert returned no row"))?;
    super::state::recompute(&[item.id]).await?;
    Ok(item)
}

/// Fetch incomplete (indexed/scraped/ongoing) episodes for a season.
pub async fn get_incomplete_episodes_for_season(season_id: i64) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ParentId.eq(season_id))
        .filter(media_items::Column::ItemType.eq(MediaItemType::Episode))
        .filter(media_items::Column::State.is_in([
            MediaItemState::Indexed,
            MediaItemState::Scraped,
            MediaItemState::Ongoing,
        ]))
        .order_by_asc(media_items::Column::EpisodeNumber)
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

/// Fetch all requested seasons for a show, with no state or special filtering.
pub async fn get_all_requested_seasons_for_show(show_id: i64) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::ParentId.eq(show_id))
        .filter(media_items::Column::ItemType.eq(MediaItemType::Season))
        .filter(media_items::Column::IsRequested.eq(true))
        .order_by_asc(media_items::Column::SeasonNumber)
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

/// Mark specific seasons of a show as requested and return their indexed episodes.
pub async fn mark_seasons_requested_and_get_episodes(
    show_id: i64,
    season_numbers: &[i32],
) -> Result<Vec<MediaItem>> {
    let db = orm();
    media_items::Entity::update_many()
        .col_expr(
            media_items::Column::IsRequested,
            sea_orm::sea_query::Expr::value(true),
        )
        .col_expr(
            media_items::Column::UpdatedAt,
            sea_orm::sea_query::Expr::cust("NOW()"),
        )
        .filter(media_items::Column::ParentId.eq(show_id))
        .filter(media_items::Column::ItemType.eq(MediaItemType::Season))
        .filter(media_items::Column::SeasonNumber.is_in(season_numbers.iter().copied()))
        .exec(db)
        .await?;

    // Correlated `parent_id IN (SELECT ... season_number = ANY($2))` subquery —
    // keep raw.
    db.execute(Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"UPDATE media_items
           SET is_requested = true, updated_at = NOW()
           WHERE parent_id IN (
               SELECT id FROM media_items
               WHERE parent_id = $1
                 AND item_type = 'season'
                 AND season_number = ANY($2)
           )
             AND item_type = 'episode'"#,
        [show_id.into(), season_numbers.to_vec().into()],
    ))
    .await?;

    // Only the show is recomputed: its rollup filters seasons by is_requested,
    // but episode is_requested feeds no rollup, so episodes need no recompute.
    super::state::recompute(&[show_id]).await?;

    // Correlated `parent_id IN (SELECT ... season_number = ANY($2))` subquery,
    // enum columns cast to ::text for the MediaItem projection — keep raw.
    let sql = r#"SELECT media_items.*,
                  media_items.content_rating::text AS content_rating,
                  media_items.state::text          AS state,
                  media_items.item_type::text      AS item_type,
                  media_items.show_status::text    AS show_status
           FROM media_items
           WHERE parent_id IN (
               SELECT id FROM media_items
               WHERE parent_id = $1
                 AND item_type = 'season'
                 AND season_number = ANY($2)
           )
             AND item_type = 'episode'
             AND state = 'indexed'"#;
    Ok(MediaItem::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        [show_id.into(), season_numbers.to_vec().into()],
    ))
    .all(db)
    .await?)
}

/// Render a Postgres enum array literal, e.g. `ARRAY['movie','show']::media_item_type[]`.
/// The labels come from the enum's own `Value` (never user input), so inlining
/// them is injection-safe and avoids needing sea-orm's `postgres-array` bind.
fn enum_array_literal<E: Into<sea_orm::Value> + Copy>(items: &[E], pg_type: &str) -> String {
    let labels: Vec<String> = items
        .iter()
        .map(|e| match (*e).into() {
            sea_orm::Value::String(Some(s)) => format!("'{s}'"),
            _ => "NULL".to_owned(),
        })
        .collect();
    format!("ARRAY[{}]::{pg_type}[]", labels.join(", "))
}

/// Build the dynamic WHERE fragment shared by the list/count queries. Enum
/// arrays are inlined; the only bound parameter is the search term, returned
/// separately so the caller can append it as `$1`.
fn item_filters(
    types: Option<&[MediaItemType]>,
    search: Option<&str>,
    states: Option<&[MediaItemState]>,
) -> (String, Option<String>) {
    let mut sql = String::new();
    let mut search_param = None;

    if let Some(t) = types
        && !t.is_empty()
    {
        sql.push_str(&format!(
            " AND media_items.item_type = ANY({})",
            enum_array_literal(t, "media_item_type")
        ));
    }
    if let Some(s) = states
        && !s.is_empty()
    {
        let arr = enum_array_literal(s, "media_item_state");
        sql.push_str(&format!(
            " AND (media_items.state = ANY({arr}) \
               OR (media_items.item_type = 'show' AND (EXISTS (\
                 SELECT 1 FROM media_items child_season \
                 WHERE child_season.item_type = 'season' \
                   AND child_season.parent_id = media_items.id \
                   AND child_season.is_requested = true \
                   AND child_season.state = ANY({arr})) \
               OR EXISTS (\
                 SELECT 1 FROM media_items child_episode \
                 JOIN media_items episode_season ON episode_season.id = child_episode.parent_id \
                 WHERE child_episode.item_type = 'episode' \
                   AND episode_season.item_type = 'season' \
                   AND episode_season.parent_id = media_items.id \
                   AND child_episode.is_requested = true \
                   AND child_episode.state = ANY({arr})))))"
        ));
    }
    if let Some(q) = search
        && !q.is_empty()
    {
        sql.push_str(" AND LOWER(media_items.title) LIKE $1");
        search_param = Some(format!("%{}%", q.to_lowercase()));
    }
    (sql, search_param)
}

pub async fn list_items_paginated(
    page: i64,
    limit: i64,
    sort: Option<String>,
    types: Option<Vec<MediaItemType>>,
    search: Option<String>,
    states: Option<Vec<MediaItemState>>,
) -> Result<Vec<MediaItemListRow>> {
    let page = page.max(1);
    let limit = limit.clamp(1, 200);
    let offset = (page - 1) * limit;
    let (filters, search_param) =
        item_filters(types.as_deref(), search.as_deref(), states.as_deref());

    let order = match sort.as_deref() {
        Some("date_asc") => "ORDER BY media_items.created_at ASC NULLS LAST",
        Some("title_asc") => "ORDER BY media_items.title ASC",
        Some("title_desc") => "ORDER BY media_items.title DESC",
        _ => "ORDER BY media_items.created_at DESC NULLS LAST",
    };
    // `limit`/`offset` are clamped integers (not user text); inlining them is safe.
    // Enum columns on `media_items.*` are re-aliased as ::text so the nested
    // MediaItem projection decodes under FromQueryResult.
    let sql = format!(
        "SELECT media_items.*, \
                media_items.content_rating::text AS content_rating, \
                media_items.state::text          AS state, \
                media_items.item_type::text      AS item_type, \
                media_items.show_status::text    AS show_status, \
                resolved_show.id AS show_id, \
                resolved_show.title AS show_title, \
                resolved_show.tmdb_id AS show_tmdb_id, \
                resolved_show.tvdb_id AS show_tvdb_id, \
                resolved_show.poster_path AS show_poster_path \
         FROM media_items \
         LEFT JOIN media_items parent_season \
           ON media_items.item_type = 'episode' \
          AND parent_season.id = media_items.parent_id \
          AND parent_season.item_type = 'season' \
         LEFT JOIN media_items resolved_show \
           ON (media_items.item_type = 'show' AND resolved_show.id = media_items.id) \
           OR (media_items.item_type = 'season' AND resolved_show.id = media_items.parent_id) \
           OR (media_items.item_type = 'episode' AND resolved_show.id = parent_season.parent_id) \
         WHERE 1=1{filters} {order} LIMIT {limit} OFFSET {offset}"
    );
    let values: Vec<sea_orm::Value> = search_param.into_iter().map(Into::into).collect();
    Ok(
        MediaItemListRow::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            values,
        ))
        .all(orm())
        .await?,
    )
}

pub async fn count_items_filtered(
    types: Option<Vec<MediaItemType>>,
    search: Option<String>,
    states: Option<Vec<MediaItemState>>,
) -> Result<i64> {
    let (filters, search_param) =
        item_filters(types.as_deref(), search.as_deref(), states.as_deref());
    let sql = format!("SELECT COUNT(*) AS count FROM media_items WHERE 1=1{filters}");
    let values: Vec<sea_orm::Value> = search_param.into_iter().map(Into::into).collect();
    let row = orm()
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            values,
        ))
        .await?;
    match row {
        Some(row) => Ok(row.try_get::<i64>("", "count")?),
        None => Ok(0),
    }
}
