use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use riven_core::entities::{media_items, ranking_profiles, settings};
use riven_core::types::MediaItemState;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, Statement,
};

use crate::entities::*;
use crate::orm;

#[derive(Debug, serde::Serialize, serde::Deserialize, FromQueryResult)]
pub struct RankingProfile {
    pub id: i32,
    pub name: String,
    pub settings: serde_json::Value,
    pub is_builtin: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, FromQueryResult)]
pub struct MediaStats {
    pub total_movies: i64,
    pub total_shows: i64,
    pub total_seasons: i64,
    pub total_episodes: i64,
    pub completed: i64,
    pub scraped: i64,
    pub indexed: i64,
    pub failed: i64,
    pub paused: i64,
    pub ongoing: i64,
    pub partially_completed: i64,
    pub unreleased: i64,
}

pub async fn get_stats() -> Result<MediaStats> {
    // Single-pass FILTER aggregate over a CTE — no builder form. Only COUNT
    // columns are projected (no enum columns selected), so no ::text casts are
    // needed; the enum comparisons live in the WHERE/FILTER predicates.
    MediaStats::find_by_statement(Statement::from_string(
        DbBackend::Postgres,
        r#"WITH ongoing_season_ids AS (
               SELECT id FROM media_items
               WHERE item_type = 'season'
               AND parent_id IN (
                   SELECT id FROM media_items
                   WHERE item_type = 'show' AND state = 'ongoing'
               )
           )
           SELECT
             COUNT(*) FILTER (WHERE item_type = 'movie' AND is_requested = true) AS total_movies,
             COUNT(*) FILTER (WHERE item_type = 'show' AND is_requested = true) AS total_shows,
             COUNT(*) FILTER (WHERE item_type = 'season' AND is_requested = true) AS total_seasons,
             COUNT(*) FILTER (WHERE item_type = 'episode' AND is_requested = true) AS total_episodes,
             -- State counts are strictly leaf-level (movies + episodes) so a single
             -- completed show doesn't inflate a bucket by also counting its show and
             -- season rows. 'ongoing'/'partially_completed' only ever exist on
             -- shows/seasons, so the leaf-level filters below resolve them to upcoming
             -- episodes / zero respectively.
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'completed'
                 AND item_type IN ('movie', 'episode')) AS completed,
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'scraped'
                 AND item_type IN ('movie', 'episode')) AS scraped,
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'indexed'
                 AND item_type IN ('movie', 'episode')) AS indexed,
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'failed'
                 AND item_type IN ('movie', 'episode')) AS failed,
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'paused'
                 AND item_type IN ('movie', 'episode')) AS paused,
             -- Leaf-level "ongoing" = upcoming episodes under an ongoing show.
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'unreleased'
                 AND item_type = 'episode'
                 AND parent_id IN (SELECT id FROM ongoing_season_ids)) AS ongoing,
             -- No leaf item is ever 'partially_completed'; always 0, kept for API stability.
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'partially_completed'
                 AND item_type IN ('movie', 'episode')) AS partially_completed,
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'unreleased'
                 AND item_type IN ('movie', 'episode')
                 AND NOT (item_type = 'episode'
                     AND parent_id IN (SELECT id FROM ongoing_season_ids))
             ) AS unreleased
           FROM media_items"#,
    ))
    .one(orm())
    .await?
    .ok_or_else(|| anyhow::anyhow!("stats aggregate returned no row"))
}

/// Returns a map of ISO date strings (YYYY-MM-DD) to count of items that
/// transitioned to `completed` on that date, covering the past year.
pub async fn get_activity() -> Result<std::collections::HashMap<String, i64>> {
    #[derive(FromQueryResult)]
    struct ActivityRow {
        date: NaiveDate,
        count: i64,
    }
    // GROUP BY a computed date expression; `state = 'completed'` is a predicate,
    // not a projected enum column, so no ::text cast is needed.
    let rows = ActivityRow::find_by_statement(Statement::from_string(
        DbBackend::Postgres,
        r#"SELECT
             DATE(updated_at AT TIME ZONE 'UTC') AS date,
             COUNT(*)::bigint AS count
           FROM media_items
           WHERE state = 'completed'
             AND updated_at IS NOT NULL
             AND updated_at >= NOW() - INTERVAL '1 year'
           GROUP BY DATE(updated_at AT TIME ZONE 'UTC')"#,
    ))
    .all(orm())
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| (row.date.to_string(), row.count))
        .collect())
}

/// Count media items grouped by release year.
pub async fn get_year_releases() -> Result<Vec<(i32, i64)>> {
    #[derive(FromQueryResult)]
    struct YearRow {
        year: i32,
        count: i64,
    }
    // COALESCE over a computed year + COUNT; `item_type IN (...)` is a predicate
    // only, so no ::text cast is needed.
    let rows = YearRow::find_by_statement(Statement::from_string(
        DbBackend::Postgres,
        r#"SELECT
               COALESCE(year, EXTRACT(YEAR FROM aired_at)::integer)::integer AS year,
               COUNT(*)::bigint AS count
           FROM media_items
           WHERE item_type IN ('movie', 'episode')
             AND COALESCE(year, EXTRACT(YEAR FROM aired_at)::integer) IS NOT NULL
           GROUP BY 1
           ORDER BY 1 ASC"#,
    ))
    .all(orm())
    .await?;
    Ok(rows.into_iter().map(|r| (r.year, r.count)).collect())
}

/// Fetch upcoming unreleased items with the show title resolved in a single JOIN.
pub async fn get_calendar_entries(limit: i64) -> Result<Vec<crate::entities::CalendarRow>> {
    // Self-joins to resolve the ancestor show title — keep the raw statement.
    // `item_type`/`state` are PG enums: cast to ::text so FromQueryResult decodes
    // them via DeriveActiveEnum's string_value rather than the native enum OID.
    let rows = crate::entities::CalendarRow::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"SELECT
               mi.id,
               mi.item_type::text AS item_type,
               mi.state::text AS state,
               mi.title,
               COALESCE(
                   CASE
                       WHEN mi.item_type = 'episode' THEN grandparent.title
                       WHEN mi.item_type = 'season'  THEN parent.title
                   END,
                   mi.title
               ) AS show_title,
               mi.aired_at,
               mi.season_number,
               mi.episode_number,
               COALESCE(
                   CASE
                       WHEN mi.item_type = 'episode' THEN grandparent.tmdb_id
                       WHEN mi.item_type = 'season'  THEN parent.tmdb_id
                   END,
                   mi.tmdb_id
               ) AS tmdb_id,
               COALESCE(
                   CASE
                       WHEN mi.item_type = 'episode' THEN grandparent.tvdb_id
                       WHEN mi.item_type = 'season'  THEN parent.tvdb_id
                   END,
                   mi.tvdb_id
               ) AS tvdb_id
           FROM media_items mi
           LEFT JOIN media_items parent      ON parent.id = mi.parent_id
           LEFT JOIN media_items grandparent ON grandparent.id = parent.parent_id
           WHERE mi.aired_at > CURRENT_DATE
             AND mi.state = 'unreleased'
           ORDER BY mi.aired_at ASC
           LIMIT $1"#,
        [limit.into()],
    ))
    .all(orm())
    .await?;
    Ok(rows)
}

/// Fetch all requested items with a future air date as full MediaItem rows.
pub async fn get_upcoming_unreleased(limit: i64) -> Result<Vec<MediaItem>> {
    Ok(media_items::Entity::find()
        .filter(media_items::Column::AiredAt.gt(Utc::now().date_naive()))
        .filter(media_items::Column::State.eq(MediaItemState::Unreleased))
        .order_by_asc(media_items::Column::AiredAt)
        .limit(u64::try_from(limit).unwrap_or(0))
        .into_model::<MediaItem>()
        .all(orm())
        .await?)
}

pub async fn get_setting(key: &str) -> Result<Option<serde_json::Value>> {
    Ok(settings::Entity::find()
        .filter(settings::Column::Key.eq(key))
        .select_only()
        .column(settings::Column::Value)
        .into_tuple()
        .one(orm())
        .await?)
}

pub async fn set_setting(key: &str, value: serde_json::Value) -> Result<()> {
    settings::Entity::insert(settings::ActiveModel {
        key: Set(key.to_owned()),
        value: Set(value),
        updated_at: Set(Utc::now().fixed_offset()),
    })
    .on_conflict(
        OnConflict::column(settings::Column::Key)
            .update_columns([settings::Column::Value, settings::Column::UpdatedAt])
            .to_owned(),
    )
    .exec(orm())
    .await?;
    Ok(())
}

pub async fn get_plugin_enabled(name: &str) -> Result<bool> {
    Ok(get_plugin_enabled_setting(name).await?.unwrap_or(false))
}

pub async fn get_plugin_enabled_setting(name: &str) -> Result<Option<bool>> {
    let key = format!("plugin_enabled.{name}");
    Ok(match get_setting(&key).await? {
        Some(serde_json::Value::Bool(enabled)) => Some(enabled),
        _ => None,
    })
}

pub async fn set_plugin_enabled(name: &str, enabled: bool) -> Result<()> {
    let key = format!("plugin_enabled.{name}");
    set_setting(&key, serde_json::Value::Bool(enabled)).await
}

pub async fn list_ranking_profiles() -> Result<Vec<RankingProfile>> {
    Ok(ranking_profiles::Entity::find()
        .order_by_asc(ranking_profiles::Column::Name)
        .into_model::<RankingProfile>()
        .all(orm())
        .await?)
}

/// Return only profiles whose `enabled` flag is true.
pub async fn get_enabled_profiles() -> Result<Vec<RankingProfile>> {
    Ok(ranking_profiles::Entity::find()
        .filter(ranking_profiles::Column::Enabled.eq(true))
        .order_by_asc(ranking_profiles::Column::Name)
        .into_model::<RankingProfile>()
        .all(orm())
        .await?)
}

/// Toggle `enabled` on any profile (built-in or custom) by name.
pub async fn set_profile_enabled(name: &str, enabled: bool) -> Result<bool> {
    let result = ranking_profiles::Entity::update_many()
        .col_expr(ranking_profiles::Column::Enabled, Expr::value(enabled))
        .col_expr(ranking_profiles::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(ranking_profiles::Column::Name.eq(name))
        .exec(orm())
        .await?;
    Ok(result.rows_affected > 0)
}

pub async fn upsert_ranking_profile(
    id: Option<i32>,
    name: &str,
    settings: serde_json::Value,
    enabled: bool,
) -> Result<RankingProfile> {
    if let Some(existing_id) = id {
        // UPDATE ... WHERE id = $1 AND is_builtin = false RETURNING *. SeaORM's
        // ActiveModel::update keys on the primary key only, so the
        // `is_builtin = false` guard is applied as an explicit filter first; the
        // original `fetch_one` errored when nothing matched, so do the same.
        ranking_profiles::Entity::update_many()
            .col_expr(ranking_profiles::Column::Name, Expr::value(name))
            .col_expr(ranking_profiles::Column::Settings, Expr::value(settings))
            .col_expr(ranking_profiles::Column::Enabled, Expr::value(enabled))
            .col_expr(ranking_profiles::Column::UpdatedAt, Expr::cust("NOW()"))
            .filter(ranking_profiles::Column::Id.eq(existing_id))
            .filter(ranking_profiles::Column::IsBuiltin.eq(false))
            .exec(orm())
            .await?;
        let model = ranking_profiles::Entity::find_by_id(existing_id)
            .filter(ranking_profiles::Column::IsBuiltin.eq(false))
            .one(orm())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("ranking profile {existing_id} not found or is built-in")
            })?;
        Ok(profile_from_model(model))
    } else {
        let model = ranking_profiles::ActiveModel {
            name: Set(name.to_owned()),
            settings: Set(settings),
            enabled: Set(enabled),
            ..Default::default()
        }
        .insert(orm())
        .await?;
        Ok(profile_from_model(model))
    }
}

/// Build the local `RankingProfile` projection from an entity model returned by
/// an insert/update (which yields a `ranking_profiles::Model`, not the local
/// struct).
fn profile_from_model(m: ranking_profiles::Model) -> RankingProfile {
    RankingProfile {
        id: m.id,
        name: m.name,
        settings: m.settings,
        is_builtin: m.is_builtin,
        enabled: m.enabled,
        created_at: m.created_at.into(),
        updated_at: m.updated_at.into(),
    }
}

/// Update the `settings` JSON on a profile (built-in or custom) by name.
/// For built-in profiles this stores user overrides that are merged on top of
/// the Rust defaults at load time.
pub async fn update_profile_settings(name: &str, settings: serde_json::Value) -> Result<bool> {
    let result = ranking_profiles::Entity::update_many()
        .col_expr(ranking_profiles::Column::Settings, Expr::value(settings))
        .col_expr(ranking_profiles::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(ranking_profiles::Column::Name.eq(name))
        .exec(orm())
        .await?;
    Ok(result.rows_affected > 0)
}

/// Delete a custom ranking profile. Built-in profiles cannot be deleted.
pub async fn delete_ranking_profile(id: i32) -> Result<bool> {
    let result = ranking_profiles::Entity::delete_many()
        .filter(ranking_profiles::Column::Id.eq(id))
        .filter(ranking_profiles::Column::IsBuiltin.eq(false))
        .exec(orm())
        .await?;
    Ok(result.rows_affected > 0)
}

pub async fn get_all_settings() -> Result<serde_json::Value> {
    let rows: Vec<(String, serde_json::Value)> = settings::Entity::find()
        .select_only()
        .column(settings::Column::Key)
        .column(settings::Column::Value)
        .into_tuple()
        .all(orm())
        .await?;
    Ok(serde_json::Value::Object(rows.into_iter().collect()))
}

pub async fn set_all_settings(settings: serde_json::Value) -> Result<serde_json::Value> {
    if let serde_json::Value::Object(ref map) = settings {
        for (key, value) in map {
            set_setting(key, value.clone()).await?;
        }
        Ok(settings)
    } else {
        Err(anyhow::anyhow!("Settings must be a JSON object"))
    }
}
