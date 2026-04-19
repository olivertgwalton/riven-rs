use async_graphql::{Context, Error, Json, Object, Result};
use riven_core::http::HttpClient;
use riven_core::http::profiles::{TMDB, TVDB};
use riven_core::plugin::PluginRegistry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::schema::metadata::{TMDB_API_BASE, get_tmdb_api_key, get_tvdb_api_key};

const TVDB_API_BASE: &str = "https://api4.thetvdb.com/v4";
const TVDB_TOKEN_EXPIRY: Duration = Duration::from_secs(25 * 24 * 60 * 60);

static TVDB_TOKEN_CACHE: OnceLock<Mutex<Option<(String, Instant)>>> = OnceLock::new();

#[derive(Default)]
pub struct CoreTvdbQuery;

#[Object]
impl CoreTvdbQuery {
    async fn tvdb_series(&self, ctx: &Context<'_>, id: i64) -> Result<Json<serde_json::Value>> {
        let token = get_tvdb_token(ctx).await?;
        tvdb_get_json(ctx, &token, &format!("/series/{id}"), None).await
    }

    async fn tvdb_series_extended(
        &self,
        ctx: &Context<'_>,
        id: i64,
        meta: Option<String>,
    ) -> Result<Json<serde_json::Value>> {
        let token = get_tvdb_token(ctx).await?;
        let query = meta.map(|value| HashMap::from([("meta".to_string(), value)]));
        tvdb_get_json(
            ctx,
            &token,
            &format!("/series/{id}/extended"),
            query.as_ref(),
        )
        .await
    }

    async fn tvdb_search_remote_id(
        &self,
        ctx: &Context<'_>,
        remote_id: String,
    ) -> Result<Json<serde_json::Value>> {
        let token = get_tvdb_token(ctx).await?;
        tvdb_get_json(ctx, &token, &format!("/search/remoteid/{remote_id}"), None).await
    }

    async fn tvdb_episodes(
        &self,
        ctx: &Context<'_>,
        id: i64,
        season_type: String,
        lang: String,
        page: Option<i64>,
    ) -> Result<Json<serde_json::Value>> {
        let token = get_tvdb_token(ctx).await?;
        let query = HashMap::from([("page".to_string(), page.unwrap_or(0).to_string())]);
        tvdb_get_json(
            ctx,
            &token,
            &format!("/series/{id}/episodes/{season_type}/{lang}"),
            Some(&query),
        )
        .await
    }

    async fn resolve_tmdb_to_tvdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<i64>> {
        resolve_tmdb_to_tvdb_id(ctx, &tmdb_id).await
    }
}

pub async fn resolve_tmdb_to_tvdb_id(ctx: &Context<'_>, tmdb_id: &str) -> Result<Option<i64>> {
    if let Some(tvdb_id) = fetch_tmdb_external_tvdb_id(ctx, tmdb_id).await? {
        return Ok(Some(tvdb_id));
    }

    let token = get_tvdb_token(ctx).await?;
    let remote_lookup =
        tvdb_get_value(ctx, &token, &format!("/search/remoteid/{tmdb_id}"), None).await?;
    if let Some(series_id) = remote_lookup
        .get("data")
        .and_then(|value| value.as_array())
        .and_then(|items| {
            items.iter().find_map(|item| {
                item.get("series")
                    .and_then(|series| series.get("id"))
                    .and_then(|id| id.as_i64())
            })
        })
    {
        return Ok(Some(series_id));
    }

    let direct_series = tvdb_get_value(ctx, &token, &format!("/series/{tmdb_id}"), None).await;
    match direct_series {
        Ok(value) => Ok(value
            .get("data")
            .and_then(|item| item.get("id"))
            .and_then(|id| id.as_i64())),
        Err(_) => Ok(None),
    }
}

async fn fetch_tmdb_external_tvdb_id(ctx: &Context<'_>, tmdb_id: &str) -> Result<Option<i64>> {
    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    let http = ctx.data::<HttpClient>()?;
    let api_key = get_tmdb_api_key(registry).await?;

    let value: serde_json::Value = match http
        .get_json(TMDB, format!("tmdb:external_ids:tv:{tmdb_id}"), |client| {
            client
                .get(format!("{TMDB_API_BASE}/3/tv/{tmdb_id}/external_ids"))
                .bearer_auth(&api_key)
        })
        .await
    {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    Ok(value.get("tvdb_id").and_then(|id| id.as_i64()))
}

async fn get_tvdb_token(ctx: &Context<'_>) -> Result<String> {
    let cache = TVDB_TOKEN_CACHE.get_or_init(|| Mutex::new(None));
    if let Some((token, created_at)) = cache.lock().expect("tvdb token cache poisoned").clone()
        && created_at.elapsed() < TVDB_TOKEN_EXPIRY
    {
        return Ok(token);
    }

    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    let http = ctx.data::<HttpClient>()?;
    let api_key = get_tvdb_api_key(registry).await?;

    let value: serde_json::Value = http
        .get_json(TVDB, "tvdb:login".to_string(), |client| {
            client
                .post(format!("{TVDB_API_BASE}/login"))
                .json(&serde_json::json!({ "apikey": api_key }))
        })
        .await
        .map_err(|e| Error::new(format!("TVDB login request failed: {e}")))?;

    let token = value
        .get("data")
        .and_then(|data| data.get("token"))
        .and_then(|token| token.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::new("TVDB login response missing token"))?;

    *cache.lock().expect("tvdb token cache poisoned") = Some((token.clone(), Instant::now()));
    Ok(token)
}

async fn tvdb_get_json(
    ctx: &Context<'_>,
    token: &str,
    path: &str,
    query: Option<&HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    tvdb_get_value(ctx, token, path, query).await.map(Json)
}

async fn tvdb_get_value(
    ctx: &Context<'_>,
    token: &str,
    path: &str,
    query: Option<&HashMap<String, String>>,
) -> Result<serde_json::Value> {
    let http = ctx.data::<HttpClient>()?;
    let dedupe_key = format!("tvdb:{path}:{query:?}");

    http.get_json(TVDB, dedupe_key, |client| {
        let mut request = client
            .get(format!("{TVDB_API_BASE}{path}"))
            .bearer_auth(token);

        if let Some(query) = query {
            request = request.query(query);
        }

        request
    })
    .await
    .map_err(|e| Error::new(format!("TVDB request failed: {e}")))
}
