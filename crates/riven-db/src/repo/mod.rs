pub mod hierarchy;
pub mod media;
pub mod requests;
pub mod state;
pub mod stats;
pub mod streams;

pub use hierarchy::*;
pub use media::*;
pub use requests::*;
pub use state::*;
pub use stats::*;
pub use streams::*;

use anyhow::Result;
use sqlx::PgPool;

pub async fn reset_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    // Stomp the column to a non-sticky placeholder, zero the attempts ceiling,
    // then re-derive the real state from current facts.
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
    // `Paused` is the only sticky state the application authors. The recompute
    // call below propagates this up to parents (their rollup logic recognises
    // the new sticky state on the child).
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
    // Capture parents before the DELETE so we can recompute them after.
    // (Children deletion fires no recompute on the parent — there's no
    // remaining row that emitted the change.)
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

    // Drop any item_request that no media_item still references — otherwise
    // re-requesting the same show merges the old request's prior seasons back in.
    let _ = sqlx::query(
        "DELETE FROM item_requests \
         WHERE id NOT IN ( \
             SELECT DISTINCT item_request_id FROM media_items \
             WHERE item_request_id IS NOT NULL \
         )",
    )
    .execute(pool)
    .await;

    state::force_recompute(pool, &parent_ids).await?;

    Ok(result.rows_affected())
}
