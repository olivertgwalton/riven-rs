pub mod flow_artifacts;
pub mod hierarchy;
pub mod media;
pub mod requests;
pub mod state;
pub mod stats;
pub mod streams;

pub use flow_artifacts::*;
pub use hierarchy::*;
pub use media::*;
pub use requests::*;
pub use state::*;
pub use stats::*;
pub use streams::*;

use anyhow::Result;
use sqlx::PgPool;

// ── Bulk state mutations ──

async fn bulk_update(pool: &PgPool, ids: &[i64], set_clause: &'static str) -> Result<u64> {
    let sql = format!("UPDATE media_items SET {set_clause}, updated_at = NOW() WHERE id = ANY($1)");
    let result = sqlx::query(&sql).bind(ids).execute(pool).await?;
    Ok(result.rows_affected())
}

pub async fn reset_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    bulk_update(pool, &ids, "state = 'indexed', failed_attempts = 0").await
}

pub async fn retry_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    bulk_update(pool, &ids, "failed_attempts = 0").await
}

pub async fn pause_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    bulk_update(pool, &ids, "state = 'paused'").await
}

pub async fn unpause_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    let result = sqlx::query!(
        "UPDATE media_items SET state = 'indexed', updated_at = NOW() WHERE id = ANY($1) AND state = 'paused'",
        &ids[..]
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn delete_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    let result = sqlx::query!("DELETE FROM media_items WHERE id = ANY($1)", &ids[..])
        .execute(pool)
        .await?;

    // Clean up item_requests that are no longer referenced by any media_item.
    // Deleting a show cascades its seasons/episodes, leaving the item_request
    // orphaned. Without this, re-requesting the same show finds the old request
    // and merges all previously-requested seasons back in.
    let _ = sqlx::query(
        "DELETE FROM item_requests \
         WHERE id NOT IN ( \
             SELECT DISTINCT item_request_id FROM media_items \
             WHERE item_request_id IS NOT NULL \
         )",
    )
    .execute(pool)
    .await;

    Ok(result.rows_affected())
}
