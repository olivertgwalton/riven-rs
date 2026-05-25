//! Per-title usenet health storage. Populated by the background availability
//! scanner; read by the API's usenet-health view.

use anyhow::Result;
use sqlx::PgPool;

/// A usenet-backed media file the scanner may check, with its last check time
/// (via the join) implicitly driving ordering.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UsenetFileToCheck {
    pub info_hash: String,
    pub file_index: i32,
    pub media_item_id: Option<i64>,
}

/// Distinct usenet files ordered least-recently-checked first (never-checked
/// lead). The scanner pulls a small batch each tick.
pub async fn usenet_files_due_for_check(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<UsenetFileToCheck>> {
    Ok(sqlx::query_as::<_, UsenetFileToCheck>(
        r#"
        SELECT u.info_hash, u.file_index, u.media_item_id
        FROM (
            SELECT DISTINCT ON (usenet_info_hash, usenet_file_index)
                   usenet_info_hash AS info_hash,
                   usenet_file_index AS file_index,
                   media_item_id
            FROM filesystem_entries
            WHERE usenet_info_hash IS NOT NULL
              AND usenet_file_index IS NOT NULL
            ORDER BY usenet_info_hash, usenet_file_index, media_item_id
        ) u
        LEFT JOIN usenet_file_health h
               ON h.info_hash = u.info_hash AND h.file_index = u.file_index
        ORDER BY h.checked_at ASC NULLS FIRST
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

/// One health update for [`upsert_usenet_file_health`].
pub struct UsenetHealthUpdate<'a> {
    pub info_hash: &'a str,
    pub file_index: i32,
    pub media_item_id: Option<i64>,
    pub status: &'a str,
    pub total_segments: i32,
    pub sampled_segments: i32,
    pub missing_segments: i32,
    pub error_segments: i32,
}

/// Insert or update the health record for one usenet file. Stamps `checked_at`.
pub async fn upsert_usenet_file_health(pool: &PgPool, u: UsenetHealthUpdate<'_>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO usenet_file_health
            (info_hash, file_index, media_item_id, status,
             total_segments, sampled_segments, missing_segments, error_segments,
             checked_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
        ON CONFLICT (info_hash, file_index) DO UPDATE SET
            media_item_id    = EXCLUDED.media_item_id,
            status           = EXCLUDED.status,
            total_segments   = EXCLUDED.total_segments,
            sampled_segments = EXCLUDED.sampled_segments,
            missing_segments = EXCLUDED.missing_segments,
            error_segments   = EXCLUDED.error_segments,
            checked_at       = now(),
            updated_at       = now()
        "#,
    )
    .bind(u.info_hash)
    .bind(u.file_index)
    .bind(u.media_item_id)
    .bind(u.status)
    .bind(u.total_segments)
    .bind(u.sampled_segments)
    .bind(u.missing_segments)
    .bind(u.error_segments)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete health rows whose usenet file no longer exists in the library — e.g.
/// the title was re-grabbed onto a different (or non-usenet) release. Keeps the
/// health view in sync so stale "not ingested"/"missing data" rows don't linger.
pub async fn prune_orphaned_usenet_health(pool: &PgPool) -> Result<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM usenet_file_health h
        WHERE NOT EXISTS (
            SELECT 1 FROM filesystem_entries fe
            WHERE fe.usenet_info_hash = h.info_hash
              AND fe.usenet_file_index = h.file_index
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Auto-repair: if a file is eligible for another automatic re-grab (under the
/// retry cap and past its backoff window), returns its current attempt count.
/// `None` means not due / exhausted.
pub async fn usenet_repair_due(
    pool: &PgPool,
    info_hash: &str,
    file_index: i32,
    max_retries: i32,
) -> Result<Option<i32>> {
    Ok(sqlx::query_scalar::<_, i32>(
        r#"
        SELECT repair_attempts
        FROM usenet_file_health
        WHERE info_hash = $1 AND file_index = $2
          AND repair_attempts < $3
          AND (next_repair_at IS NULL OR next_repair_at <= now())
        "#,
    )
    .bind(info_hash)
    .bind(file_index)
    .bind(max_retries)
    .fetch_optional(pool)
    .await?)
}

/// Record an auto-repair attempt: bump the counter and schedule the next
/// attempt `backoff_secs` from now.
pub async fn record_usenet_repair_attempt(
    pool: &PgPool,
    info_hash: &str,
    file_index: i32,
    backoff_secs: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE usenet_file_health
        SET repair_attempts = repair_attempts + 1,
            last_repair_at = now(),
            next_repair_at = now() + ($3 * interval '1 second')
        WHERE info_hash = $1 AND file_index = $2
        "#,
    )
    .bind(info_hash)
    .bind(file_index)
    .bind(backoff_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// Clear repair bookkeeping once a file is healthy again (altmount's
/// "resolve on import"). No-op when there's nothing to clear.
pub async fn clear_usenet_repair_state(
    pool: &PgPool,
    info_hash: &str,
    file_index: i32,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE usenet_file_health
        SET repair_attempts = 0, last_repair_at = NULL, next_repair_at = NULL
        WHERE info_hash = $1 AND file_index = $2
          AND (repair_attempts > 0 OR next_repair_at IS NOT NULL)
        "#,
    )
    .bind(info_hash)
    .bind(file_index)
    .execute(pool)
    .await?;
    Ok(())
}
