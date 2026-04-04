use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgPool;

use crate::entities::*;

// ── Ranking profiles ──

#[derive(Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct RankingProfile {
    pub id: i32,
    pub name: String,
    pub settings: serde_json::Value,
    pub is_builtin: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
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

pub async fn get_stats(pool: &PgPool) -> Result<MediaStats> {
    Ok(sqlx::query_as!(
        MediaStats,
        r#"WITH ongoing_season_ids AS (
               SELECT id FROM media_items
               WHERE item_type = 'season'
               AND parent_id IN (
                   SELECT id FROM media_items
                   WHERE item_type = 'show' AND state = 'ongoing'
               )
           )
           SELECT
             COUNT(*) FILTER (WHERE item_type = 'movie' AND is_requested = true) AS "total_movies!: i64",
             COUNT(*) FILTER (WHERE item_type = 'show' AND is_requested = true) AS "total_shows!: i64",
             COUNT(*) FILTER (WHERE item_type = 'season' AND is_requested = true) AS "total_seasons!: i64",
             COUNT(*) FILTER (WHERE item_type = 'episode' AND is_requested = true) AS "total_episodes!: i64",
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'completed') AS "completed!: i64",
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'scraped') AS "scraped!: i64",
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'indexed') AS "indexed!: i64",
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'failed') AS "failed!: i64",
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'paused') AS "paused!: i64",
             COUNT(*) FILTER (WHERE is_requested = true AND (
                 state = 'ongoing'
                 OR (state = 'unreleased' AND item_type = 'episode'
                     AND parent_id IN (SELECT id FROM ongoing_season_ids))
             )) AS "ongoing!: i64",
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'partially_completed') AS "partially_completed!: i64",
             COUNT(*) FILTER (WHERE is_requested = true AND state = 'unreleased'
                 AND NOT (item_type = 'episode'
                     AND parent_id IN (SELECT id FROM ongoing_season_ids))
             ) AS "unreleased!: i64"
           FROM media_items"#
    )
    .fetch_one(pool)
    .await?)
}

/// Returns a map of ISO date strings (YYYY-MM-DD) to count of items that
/// transitioned to `completed` on that date, covering the past year.
pub async fn get_activity(pool: &PgPool) -> Result<std::collections::HashMap<String, i64>> {
    let rows: Vec<(NaiveDate, i64)> = sqlx::query_as(
        r#"SELECT
             DATE(updated_at AT TIME ZONE 'UTC') AS date,
             COUNT(*)::bigint AS count
           FROM media_items
           WHERE state = 'completed'
             AND updated_at IS NOT NULL
             AND updated_at >= NOW() - INTERVAL '1 year'
           GROUP BY DATE(updated_at AT TIME ZONE 'UTC')"#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(date, count)| (date.to_string(), count))
        .collect())
}

/// Count media items grouped by release year.
pub async fn get_year_releases(pool: &PgPool) -> Result<Vec<(i32, i64)>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(
        r#"SELECT
               COALESCE(year, EXTRACT(YEAR FROM aired_at)::integer)::integer AS year,
               COUNT(*)::bigint AS count
           FROM media_items
           WHERE item_type IN ('movie', 'episode')
             AND COALESCE(year, EXTRACT(YEAR FROM aired_at)::integer) IS NOT NULL
           GROUP BY 1
           ORDER BY 1 ASC"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Fetch upcoming unreleased items with the show title resolved in a single JOIN.
pub async fn get_calendar_entries(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<crate::entities::CalendarRow>> {
    let rows = sqlx::query_as!(
        crate::entities::CalendarRow,
        r#"SELECT
               mi.id,
               mi.item_type AS "item_type: _",
               mi.state AS "state: _",
               mi.title,
               COALESCE(
                   CASE
                       WHEN mi.item_type = 'episode' THEN grandparent.title
                       WHEN mi.item_type = 'season'  THEN parent.title
                   END,
                   mi.title
               ) AS "show_title!: String",
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
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Fetch all requested items with a future air date as full MediaItem rows.
pub async fn get_upcoming_unreleased(pool: &PgPool, limit: i64) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items
         WHERE aired_at > CURRENT_DATE AND state = 'unreleased'
         ORDER BY aired_at ASC
         LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

pub async fn get_setting(pool: &PgPool, key: &str) -> Result<Option<serde_json::Value>> {
    Ok(
        sqlx::query_scalar!("SELECT value FROM settings WHERE key = $1", key)
            .fetch_optional(pool)
            .await?,
    )
}

pub async fn set_setting(pool: &PgPool, key: &str, value: serde_json::Value) -> Result<()> {
    sqlx::query!(
        r#"INSERT INTO settings (key, value, updated_at)
           VALUES ($1, $2, NOW())
           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"#,
        key,
        value
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_plugin_enabled(pool: &PgPool, name: &str) -> Result<bool> {
    let key = format!("plugin_enabled.{name}");
    Ok(match get_setting(pool, &key).await? {
        Some(serde_json::Value::Bool(enabled)) => enabled,
        _ => true,
    })
}

pub async fn set_plugin_enabled(pool: &PgPool, name: &str, enabled: bool) -> Result<()> {
    let key = format!("plugin_enabled.{name}");
    set_setting(pool, &key, serde_json::Value::Bool(enabled)).await
}

pub async fn list_ranking_profiles(pool: &PgPool) -> Result<Vec<RankingProfile>> {
    Ok(sqlx::query_as::<_, RankingProfile>(
        "SELECT id, name, settings, is_builtin, enabled, created_at, updated_at \
         FROM ranking_profiles ORDER BY name",
    )
    .fetch_all(pool)
    .await?)
}

/// Return only profiles whose `enabled` flag is true.
pub async fn get_enabled_profiles(pool: &PgPool) -> Result<Vec<RankingProfile>> {
    Ok(sqlx::query_as::<_, RankingProfile>(
        "SELECT id, name, settings, is_builtin, enabled, created_at, updated_at \
         FROM ranking_profiles WHERE enabled = true ORDER BY name",
    )
    .fetch_all(pool)
    .await?)
}

/// Toggle `enabled` on any profile (built-in or custom) by name.
pub async fn set_profile_enabled(pool: &PgPool, name: &str, enabled: bool) -> Result<bool> {
    let result =
        sqlx::query("UPDATE ranking_profiles SET enabled = $1, updated_at = NOW() WHERE name = $2")
            .bind(enabled)
            .bind(name)
            .execute(pool)
            .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn upsert_ranking_profile(
    pool: &PgPool,
    id: Option<i32>,
    name: &str,
    settings: serde_json::Value,
    enabled: bool,
) -> Result<RankingProfile> {
    if let Some(existing_id) = id {
        Ok(sqlx::query_as::<_, RankingProfile>(
            "UPDATE ranking_profiles \
             SET name = $2, settings = $3, enabled = $4, updated_at = NOW() \
             WHERE id = $1 AND is_builtin = false \
             RETURNING id, name, settings, is_builtin, enabled, created_at, updated_at",
        )
        .bind(existing_id)
        .bind(name)
        .bind(&settings)
        .bind(enabled)
        .fetch_one(pool)
        .await?)
    } else {
        Ok(sqlx::query_as::<_, RankingProfile>(
            "INSERT INTO ranking_profiles (name, settings, enabled) \
             VALUES ($1, $2, $3) \
             RETURNING id, name, settings, is_builtin, enabled, created_at, updated_at",
        )
        .bind(name)
        .bind(&settings)
        .bind(enabled)
        .fetch_one(pool)
        .await?)
    }
}

/// Update the `settings` JSON on a profile (built-in or custom) by name.
/// For built-in profiles this stores user overrides that are merged on top of
/// the Rust defaults at load time.
pub async fn update_profile_settings(
    pool: &PgPool,
    name: &str,
    settings: serde_json::Value,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE ranking_profiles SET settings = $1, updated_at = NOW() WHERE name = $2",
    )
    .bind(&settings)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Delete a custom ranking profile. Built-in profiles cannot be deleted.
pub async fn delete_ranking_profile(pool: &PgPool, id: i32) -> Result<bool> {
    let result = sqlx::query("DELETE FROM ranking_profiles WHERE id = $1 AND is_builtin = false")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn get_all_settings(pool: &PgPool) -> Result<serde_json::Value> {
    let rows: Vec<(String, serde_json::Value)> = sqlx::query_as("SELECT key, value FROM settings")
        .fetch_all(pool)
        .await?;
    Ok(serde_json::Value::Object(rows.into_iter().collect()))
}

pub async fn set_all_settings(
    pool: &PgPool,
    settings: serde_json::Value,
) -> Result<serde_json::Value> {
    if let serde_json::Value::Object(ref map) = settings {
        for (key, value) in map {
            set_setting(pool, key, value.clone()).await?;
        }
        Ok(settings)
    } else {
        Err(anyhow::anyhow!("Settings must be a JSON object"))
    }
}
