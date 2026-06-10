//! Postgres-backed durable store for `NzbMeta`.
//!
//! Schema and rationale: see migration `028_usenet_meta.sql`. The streamer's
//! only persistence concern is "given an info_hash, can I rebuild the segment
//! map." Postgres holds that record for as long as it's relevant; the
//! in-memory LRU above absorbs hot reads. No TTL, no Redis hop.

use sqlx::PgPool;

use super::{NzbMeta, StreamerError};

pub(super) async fn load(db: &PgPool, info_hash: &str) -> Result<Option<NzbMeta>, StreamerError> {
    let row: Option<(sqlx::types::Json<NzbMeta>,)> =
        sqlx::query_as("SELECT meta FROM usenet_meta WHERE info_hash = $1")
            .bind(info_hash)
            .fetch_optional(db)
            .await?;
    Ok(row.map(|(j,)| j.0))
}

pub(super) async fn store(
    db: &PgPool,
    info_hash: &str,
    meta: &NzbMeta,
) -> Result<(), StreamerError> {
    sqlx::query(
        "INSERT INTO usenet_meta (info_hash, meta, created_at, updated_at) \
         VALUES ($1, $2, NOW(), NOW()) \
         ON CONFLICT (info_hash) DO UPDATE \
            SET meta = EXCLUDED.meta, updated_at = NOW()",
    )
    .bind(info_hash)
    .bind(sqlx::types::Json(meta))
    .execute(db)
    .await?;
    Ok(())
}
