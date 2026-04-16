use redis::AsyncCommands;
use riven_core::plugin::PluginContext;

use crate::types::{PreflightResult, UsenetPayload};
use crate::{CACHE_TTL_SECS, PREFLIGHT_TTL_SECS};

fn payload_key(hash: &str) -> String {
    format!("plugin:usenet:payload:{hash}")
}

fn preflight_key(hash: &str) -> String {
    format!("plugin:usenet:preflight:{hash}")
}

pub(crate) async fn load_payload(ctx: &PluginContext, hash: &str) -> Option<UsenetPayload> {
    let mut conn = ctx.redis.clone();
    let key = payload_key(hash);

    match conn.get::<_, Option<String>>(&key).await {
        Ok(Some(value)) => match serde_json::from_str(&value) {
            Ok(payload) => return Some(payload),
            Err(error) => tracing::debug!(hash, error = %error, "invalid usenet payload cache"),
        },
        Ok(None) => {}
        Err(error) => tracing::debug!(hash, error = %error, "failed to load usenet payload cache"),
    }

    match sqlx::query_scalar::<_, String>("SELECT magnet FROM streams WHERE info_hash = $1")
        .bind(hash)
        .fetch_optional(&ctx.db_pool)
        .await
    {
        Ok(Some(magnet)) => UsenetPayload::from_magnet(&magnet),
        Ok(None) => None,
        Err(error) => {
            tracing::debug!(hash, error = %error, "failed to load usenet payload from database");
            None
        }
    }
}

pub(crate) async fn store_payload(ctx: &PluginContext, hash: &str, payload: &UsenetPayload) {
    let Ok(value) = serde_json::to_string(payload) else {
        return;
    };

    let mut conn = ctx.redis.clone();
    let _: redis::RedisResult<()> = conn.set_ex(payload_key(hash), value, CACHE_TTL_SECS).await;
}

pub(crate) async fn load_preflight(ctx: &PluginContext, hash: &str) -> Option<PreflightResult> {
    let mut conn = ctx.redis.clone();

    match conn.get::<_, Option<String>>(preflight_key(hash)).await {
        Ok(Some(value)) => serde_json::from_str(&value).ok(),
        Ok(None) => None,
        Err(error) => {
            tracing::debug!(hash, error = %error, "failed to load usenet preflight cache");
            None
        }
    }
}

pub(crate) async fn store_preflight(ctx: &PluginContext, hash: &str, preflight: &PreflightResult) {
    let Ok(value) = serde_json::to_string(preflight) else {
        return;
    };

    let mut conn = ctx.redis.clone();
    let _: redis::RedisResult<()> = conn
        .set_ex(preflight_key(hash), value, PREFLIGHT_TTL_SECS)
        .await;
}
