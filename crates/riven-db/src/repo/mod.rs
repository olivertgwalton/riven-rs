pub mod hierarchy;
pub mod media;
pub mod requests;
pub mod state;
pub mod stats;
pub mod streams;
pub mod usenet_health;
pub mod usenet_traffic;

pub use hierarchy::*;
pub use media::*;
pub use requests::*;
pub use state::*;
pub use stats::*;
pub use streams::*;
pub use usenet_health::*;
pub use usenet_traffic::*;

use anyhow::Result;
use sqlx::PgPool;

pub async fn reset_library(pool: &PgPool) -> Result<u64> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_items")
        .fetch_one(pool)
        .await?;

    sqlx::query(
        "TRUNCATE TABLE media_items, streams, item_requests, usenet_meta \
         RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await?;

    Ok(count.cast_unsigned())
}

pub async fn reset_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query!(
        "UPDATE media_items SET state = 'indexed', failed_attempts = 0, updated_at = NOW() \
         WHERE id = ANY($1)",
        &ids[..]
    )
    .execute(pool)
    .await?;
    state::recompute(pool, &ids).await?;
    Ok(result.rows_affected())
}

pub async fn retry_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query!(
        "UPDATE media_items SET failed_attempts = 0, updated_at = NOW() WHERE id = ANY($1)",
        &ids[..]
    )
    .execute(pool)
    .await?;
    state::recompute(pool, &ids).await?;
    Ok(result.rows_affected())
}

pub async fn pause_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query!(
        "UPDATE media_items SET state = 'paused', updated_at = NOW() WHERE id = ANY($1)",
        &ids[..]
    )
    .execute(pool)
    .await?;
    state::recompute(pool, &ids).await?;
    Ok(result.rows_affected())
}

pub async fn unpause_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    state::unpause_items(pool, &ids).await?;
    Ok(ids.len() as u64)
}

pub async fn delete_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    // Capture parents before the DELETE: deleting children fires no recompute
    // on the parent, so they must be recomputed explicitly afterward.
    let parent_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT parent_id FROM media_items WHERE id = ANY($1) AND parent_id IS NOT NULL",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let result = sqlx::query!("DELETE FROM media_items WHERE id = ANY($1)", &ids[..])
        .execute(pool)
        .await?;

    if let Err(e) = sqlx::query(
        "DELETE FROM item_requests \
         WHERE id NOT IN ( \
             SELECT DISTINCT item_request_id FROM media_items \
             WHERE item_request_id IS NOT NULL \
         )",
    )
    .execute(pool)
    .await
    {
        tracing::warn!(error = %e, "failed to prune orphaned item_requests");
    }

    state::force_recompute(pool, &parent_ids).await?;

    Ok(result.rows_affected())
}
