use anyhow::Result;
use chrono::Utc;
use riven_core::types::*;
use sqlx::PgPool;

use crate::entities::*;

pub async fn list_seasons(pool: &PgPool, show_id: i64) -> Result<Vec<MediaItem>> {
    Ok(
        sqlx::query_as::<_, MediaItem>(
            "SELECT * FROM media_items WHERE item_type = 'season' AND parent_id = $1 ORDER BY season_number",
        )
        .bind(show_id)
        .fetch_all(pool)
        .await?,
    )
}

/// List seasons for a show, excluding season 0 (specials).
pub async fn list_seasons_excluding_specials(
    pool: &PgPool,
    show_id: i64,
) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items \
         WHERE item_type = 'season' AND parent_id = $1 \
           AND (season_number IS NULL OR season_number <> 0) \
         ORDER BY season_number",
    )
    .bind(show_id)
    .fetch_all(pool)
    .await?)
}

/// Find an episode by the parent show's TVDB ID, episode number, and optional season number.
/// When `season_number` is `None`, looks up by absolute episode numbering.
pub async fn find_episode_by_show_tvdb(
    pool: &PgPool,
    tvdb_id: &str,
    episode_number: i32,
    season_number: Option<i32>,
) -> Result<Option<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        r#"SELECT ep.*
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
           LIMIT 1"#,
    )
    .bind(tvdb_id)
    .bind(episode_number)
    .bind(season_number)
    .fetch_optional(pool)
    .await?)
}

/// Count episodes in a season.
pub async fn count_episodes_in_season(pool: &PgPool, season_id: i64) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*) FROM media_items WHERE item_type = 'episode' AND parent_id = $1",
    )
    .bind(season_id)
    .fetch_one(pool)
    .await?)
}

/// Count the total expected downloadable episode files for a show.
///
/// Only processable seasons (not unreleased/ongoing, not specials, is_requested) are counted.
/// For continuing shows the last season is excluded (still airing). Mirrors the TypeScript
/// `expectedFileCount` field resolver on `Show`.
///
/// Executes as a single SQL query.
pub async fn count_expected_files_for_show(pool: &PgPool, show_id: i64) -> Result<i64> {
    Ok(sqlx::query_scalar(
        r#"WITH qualifying_seasons AS (
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
           SELECT COALESCE(COUNT(e.id), 0)
           FROM qualifying_seasons qs
           JOIN media_items e ON e.parent_id = qs.id AND e.item_type = 'episode'
           WHERE qs.rn <= COALESCE((SELECT cap FROM season_cap), 0)"#,
    )
    .bind(show_id)
    .fetch_one(pool)
    .await?)
}

/// Return true if `item_id` is equal to `target_id` or is a descendant of it
/// (i.e. following parent_id links from `item_id` eventually reaches `target_id`).
///
/// Uses a recursive CTE — one query regardless of hierarchy depth.
pub async fn is_item_descendant_of(pool: &PgPool, item_id: i64, target_id: i64) -> Result<bool> {
    Ok(sqlx::query_scalar(
        r#"WITH RECURSIVE ancestors AS (
               SELECT id, parent_id FROM media_items WHERE id = $1
               UNION ALL
               SELECT m.id, m.parent_id
               FROM media_items m
               INNER JOIN ancestors a ON m.id = a.parent_id
           )
           SELECT EXISTS(SELECT 1 FROM ancestors WHERE id = $2)"#,
    )
    .bind(item_id)
    .bind(target_id)
    .fetch_one(pool)
    .await?)
}

pub async fn list_episodes(pool: &PgPool, season_id: i64) -> Result<Vec<MediaItem>> {
    Ok(
        sqlx::query_as::<_, MediaItem>(
            "SELECT * FROM media_items WHERE item_type = 'episode' AND parent_id = $1 ORDER BY episode_number",
        )
        .bind(season_id)
        .fetch_all(pool)
        .await?,
    )
}

pub async fn get_media_item_hierarchy(
    pool: &PgPool,
    id: i64,
) -> Result<Option<MediaItemHierarchy>> {
    Ok(sqlx::query_as::<_, MediaItemHierarchy>(
        r#"SELECT
               item.*,
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
               show_item.content_rating AS resolved_show_content_rating,
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
           WHERE item.id = $1"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?)
}

pub async fn create_season(
    pool: &PgPool,
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
    let item = sqlx::query_as::<_, MediaItem>(
        r#"INSERT INTO media_items (title, tvdb_id, item_type, state, season_number, is_special, parent_id, is_requested, created_at, item_request_id)
           VALUES ($1, $2, 'season', 'indexed', $3, $4, $5, $6, $7, $8)
           ON CONFLICT (parent_id, season_number) WHERE item_type = 'season'
           DO UPDATE SET
               title        = EXCLUDED.title,
               tvdb_id      = COALESCE(EXCLUDED.tvdb_id, media_items.tvdb_id),
               is_requested = EXCLUDED.is_requested OR media_items.is_requested,
               updated_at   = NOW()
           RETURNING *"#,
    )
    .bind(title_str)
    .bind(tvdb_id)
    .bind(number)
    .bind(is_special)
    .bind(show_id)
    .bind(is_requested)
    .bind(now)
    .bind(item_request_id)
    .fetch_one(pool)
    .await?;
    Ok(item)
}

pub async fn create_episode(
    pool: &PgPool,
    season_id: i64,
    number: i32,
    title: Option<&str>,
    tvdb_id: Option<&str>,
    aired_at: Option<chrono::NaiveDate>,
    runtime: Option<i32>,
    absolute_number: Option<i32>,
    item_request_id: Option<i64>,
    is_requested: bool,
    season_number: Option<i32>,
) -> Result<MediaItem> {
    let now = Utc::now();
    let default_title = format!("Episode {number:02}");
    let title_str = title.unwrap_or(&default_title);
    let state = match aired_at {
        Some(date) if date > Utc::now().date_naive() => MediaItemState::Unreleased,
        _ => MediaItemState::Indexed,
    };
    let item = sqlx::query_as::<_, MediaItem>(
        r#"INSERT INTO media_items (title, tvdb_id, item_type, state, episode_number, absolute_number, runtime, parent_id, aired_at, is_requested, season_number, created_at, item_request_id)
           VALUES ($1, $2, 'episode', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
           ON CONFLICT (parent_id, episode_number) WHERE item_type = 'episode'
           DO UPDATE SET
               title           = EXCLUDED.title,
               tvdb_id         = COALESCE(EXCLUDED.tvdb_id, media_items.tvdb_id),
               aired_at        = COALESCE(EXCLUDED.aired_at, media_items.aired_at),
               runtime         = COALESCE(EXCLUDED.runtime, media_items.runtime),
               absolute_number = COALESCE(EXCLUDED.absolute_number, media_items.absolute_number),
               season_number   = COALESCE(EXCLUDED.season_number, media_items.season_number),
               is_requested    = EXCLUDED.is_requested OR media_items.is_requested,
               -- Transition unreleased episodes that have since aired
               state = CASE
                   WHEN media_items.state = 'unreleased'
                    AND EXCLUDED.aired_at IS NOT NULL
                    AND EXCLUDED.aired_at <= CURRENT_DATE
                   THEN 'indexed'::media_item_state
                   ELSE media_items.state
               END,
               updated_at = NOW()
           RETURNING *"#,
    )
    .bind(title_str)
    .bind(tvdb_id)
    .bind(state as MediaItemState)
    .bind(number)
    .bind(absolute_number)
    .bind(runtime)
    .bind(season_id)
    .bind(aired_at)
    .bind(is_requested)
    .bind(season_number)
    .bind(now)
    .bind(item_request_id)
    .fetch_one(pool)
    .await?;
    Ok(item)
}

/// Base season query. Excludes specials (Season 0).
async fn seasons_for_show(
    pool: &PgPool,
    show_id: i64,
    state_filter: &str,
) -> Result<Vec<MediaItem>> {
    let sql = format!(
        "SELECT * FROM media_items \
         WHERE parent_id = $1 AND item_type = 'season' AND is_requested = true \
           AND is_special = false AND {state_filter} \
         ORDER BY season_number ASC"
    );
    Ok(sqlx::query_as::<_, MediaItem>(&sql)
        .bind(show_id)
        .fetch_all(pool)
        .await?)
}

/// Fetch all requested, non-completed seasons for a show.
pub async fn get_requested_seasons_for_show(pool: &PgPool, show_id: i64) -> Result<Vec<MediaItem>> {
    seasons_for_show(
        pool,
        show_id,
        "state NOT IN ('completed', 'unreleased', 'paused', 'failed')",
    )
    .await
}

/// Fetch requested seasons in scraped state for a show.
pub async fn get_scraped_seasons_for_show(pool: &PgPool, show_id: i64) -> Result<Vec<MediaItem>> {
    seasons_for_show(pool, show_id, "state = 'scraped'").await
}

/// Fetch incomplete (indexed/scraped/ongoing) requested episodes for a season.
pub async fn get_incomplete_episodes_for_season(
    pool: &PgPool,
    season_id: i64,
) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items
         WHERE parent_id = $1
           AND item_type = 'episode'
           AND is_requested = true
           AND state = ANY(ARRAY['indexed'::media_item_state, 'scraped'::media_item_state, 'ongoing'::media_item_state])
         ORDER BY episode_number ASC",
    )
    .bind(season_id)
    .fetch_all(pool)
    .await?)
}

/// Fetch indexed, requested episodes ready for scraping, with the parent show's
/// imdb_id filled in via JOIN.
pub async fn get_episodes_ready_for_scraping(pool: &PgPool, limit: i64) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as!(
        MediaItem,
        r#"SELECT
               e.id, e.title, e.full_title,
               COALESCE(e.imdb_id, show_item.imdb_id) AS imdb_id,
               e.tvdb_id, e.tmdb_id, e.poster_path,
               e.created_at, e.updated_at, e.indexed_at, e.scraped_at, e.scraped_times,
               e.aliases, e.network, e.country, e.language, e.is_anime, e.aired_at, e.year, e.genres,
               e.rating, e.content_rating AS "content_rating: _", e.state AS "state: _",
               e.failed_attempts, e.item_type AS "item_type: _",
               e.is_requested, e.show_status AS "show_status: _", e.season_number, e.is_special, e.parent_id,
               e.episode_number, e.absolute_number, e.runtime, e.item_request_id, e.active_stream_id
           FROM media_items e
           LEFT JOIN media_items season_item ON e.parent_id = season_item.id AND season_item.item_type = 'season'
           LEFT JOIN media_items show_item ON season_item.parent_id = show_item.id AND show_item.item_type = 'show'
           WHERE e.state = 'indexed'
             AND e.item_type = 'episode'
             AND e.is_requested = true
             AND (e.failed_attempts = 0 OR e.updated_at IS NULL OR e.updated_at < NOW() - INTERVAL '1 hour')
           ORDER BY e.failed_attempts ASC, e.created_at ASC
           LIMIT $1"#,
        limit
    )
    .fetch_all(pool)
    .await?)
}

/// Fetch all indexed, requested episodes that belong to a given show.
pub async fn get_indexed_episodes_for_show(pool: &PgPool, show_id: i64) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT e.* FROM media_items e
         JOIN media_items s ON e.parent_id = s.id
         WHERE s.parent_id = $1
           AND s.item_type = 'season'
           AND e.item_type = 'episode'
           AND e.state = 'indexed'
           AND e.is_requested = true",
    )
    .bind(show_id)
    .fetch_all(pool)
    .await?)
}

/// Mark specific seasons of a show as requested and return their indexed episodes.
pub async fn mark_seasons_requested_and_get_episodes(
    pool: &PgPool,
    show_id: i64,
    season_numbers: &[i32],
) -> Result<Vec<MediaItem>> {
    // Mark the seasons themselves as requested.
    sqlx::query!(
        r#"UPDATE media_items
           SET is_requested = true, updated_at = NOW()
           WHERE parent_id = $1
             AND item_type = 'season'
             AND season_number = ANY($2)"#,
        show_id,
        season_numbers
    )
    .execute(pool)
    .await?;

    // Mark all episodes as requested, then return the indexed ones (those needing a scrape).
    sqlx::query!(
        r#"UPDATE media_items
           SET is_requested = true, updated_at = NOW()
           WHERE parent_id IN (
               SELECT id FROM media_items
               WHERE parent_id = $1
                 AND item_type = 'season'
                 AND season_number = ANY($2)
           )
             AND item_type = 'episode'"#,
        show_id,
        season_numbers
    )
    .execute(pool)
    .await?;

    Ok(sqlx::query_as::<_, MediaItem>(
        r#"SELECT * FROM media_items
           WHERE parent_id IN (
               SELECT id FROM media_items
               WHERE parent_id = $1
                 AND item_type = 'season'
                 AND season_number = ANY($2)
           )
             AND item_type = 'episode'
             AND state = 'indexed'"#,
    )
    .bind(show_id)
    .bind(season_numbers)
    .fetch_all(pool)
    .await?)
}

/// Clear `is_requested` on all non-completed seasons of a show that are NOT in
/// `requested_season_numbers`, and on their child episodes. This prevents the
/// retry scheduler from processing seasons the user did not select.
pub async fn unmark_unrequested_seasons(
    pool: &PgPool,
    show_id: i64,
    requested_season_numbers: &[i32],
) -> Result<()> {
    // Un-mark seasons not in the requested list (skip already-completed ones so
    // they remain visible in the show's state computation).
    sqlx::query(
        "UPDATE media_items \
         SET is_requested = false, updated_at = NOW() \
         WHERE parent_id = $1 \
           AND item_type = 'season' \
           AND season_number IS NOT NULL \
           AND NOT (season_number = ANY($2)) \
           AND state NOT IN ('completed', 'partially_completed', 'ongoing')",
    )
    .bind(show_id)
    .bind(requested_season_numbers)
    .execute(pool)
    .await?;

    // Un-mark episodes inside those seasons too.
    sqlx::query(
        "UPDATE media_items \
         SET is_requested = false, updated_at = NOW() \
         WHERE parent_id IN ( \
             SELECT id FROM media_items \
             WHERE parent_id = $1 \
               AND item_type = 'season' \
               AND season_number IS NOT NULL \
               AND NOT (season_number = ANY($2)) \
               AND state NOT IN ('completed', 'partially_completed', 'ongoing') \
         ) \
           AND item_type = 'episode'",
    )
    .bind(show_id)
    .bind(requested_season_numbers)
    .execute(pool)
    .await?;

    Ok(())
}

fn apply_item_filters(
    qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>,
    types: Option<&[MediaItemType]>,
    search: Option<&str>,
    states: Option<&[MediaItemState]>,
) {
    if let Some(t) = types
        && !t.is_empty()
    {
        qb.push(" AND item_type = ANY(");
        qb.push_bind(t.to_vec());
        qb.push(")");
    }
    if let Some(s) = states
        && !s.is_empty()
    {
        qb.push(" AND state = ANY(");
        qb.push_bind(s.to_vec());
        qb.push(")");
    }
    if let Some(q) = search
        && !q.is_empty()
    {
        qb.push(" AND LOWER(title) LIKE ");
        qb.push_bind(format!("%{}%", q.to_lowercase()));
    }
}

pub async fn list_items_paginated(
    pool: &PgPool,
    page: i64,
    limit: i64,
    sort: Option<String>,
    types: Option<Vec<MediaItemType>>,
    search: Option<String>,
    states: Option<Vec<MediaItemState>>,
) -> Result<Vec<MediaItem>> {
    let page = page.max(1);
    let limit = limit.clamp(1, 200);
    let offset = (page - 1) * limit;

    let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT * FROM media_items WHERE 1=1");
    apply_item_filters(
        &mut qb,
        types.as_deref(),
        search.as_deref(),
        states.as_deref(),
    );

    let order = match sort.as_deref() {
        Some("date_asc") => " ORDER BY created_at ASC NULLS LAST",
        Some("title_asc") => " ORDER BY title ASC",
        Some("title_desc") => " ORDER BY title DESC",
        _ => " ORDER BY created_at DESC NULLS LAST",
    };
    qb.push(order);
    qb.push(" LIMIT ");
    qb.push_bind(limit);
    qb.push(" OFFSET ");
    qb.push_bind(offset);
    Ok(qb.build_query_as::<MediaItem>().fetch_all(pool).await?)
}

pub async fn count_items_filtered(
    pool: &PgPool,
    types: Option<Vec<MediaItemType>>,
    search: Option<String>,
    states: Option<Vec<MediaItemState>>,
) -> Result<i64> {
    let mut qb =
        sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT COUNT(*) FROM media_items WHERE 1=1");
    apply_item_filters(
        &mut qb,
        types.as_deref(),
        search.as_deref(),
        states.as_deref(),
    );
    Ok(qb.build_query_scalar::<i64>().fetch_one(pool).await?)
}
