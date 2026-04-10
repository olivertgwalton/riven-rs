use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;

pub async fn clear_flow_artifacts(pool: &PgPool, flow_name: &str, item_id: i64) -> Result<u64> {
    let result = sqlx::query("DELETE FROM flow_artifacts WHERE flow_name = $1 AND item_id = $2")
        .bind(flow_name)
        .bind(item_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

pub async fn upsert_flow_artifact(
    pool: &PgPool,
    flow_name: &str,
    item_id: i64,
    plugin_name: &str,
    payload: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO flow_artifacts (flow_name, item_id, plugin_name, payload, created_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (flow_name, item_id, plugin_name)
         DO UPDATE SET payload = EXCLUDED.payload, created_at = EXCLUDED.created_at",
    )
    .bind(flow_name)
    .bind(item_id)
    .bind(plugin_name)
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn load_flow_artifacts(
    pool: &PgPool,
    flow_name: &str,
    item_id: i64,
) -> Result<Vec<Value>> {
    sqlx::query_scalar::<_, Value>(
        "SELECT payload
         FROM flow_artifacts
         WHERE flow_name = $1 AND item_id = $2
         ORDER BY created_at ASC, plugin_name ASC",
    )
    .bind(flow_name)
    .bind(item_id)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

pub async fn count_flow_artifacts(pool: &PgPool, flow_name: &str, item_id: i64) -> Result<i64> {
    let count: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)
         FROM flow_artifacts
         WHERE flow_name = $1 AND item_id = $2",
    )
    .bind(flow_name)
    .bind(item_id)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

pub async fn delete_stale_flow_artifacts(pool: &PgPool, older_than_hours: i64) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM flow_artifacts
         WHERE created_at < NOW() - ($1 * INTERVAL '1 hour')",
    )
    .bind(older_than_hours)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
