use std::sync::Arc;

use redis::AsyncCommands;

use riven_core::events::ScrapeRequest;
use riven_core::http::{HttpClient, HttpResponseData};
use riven_core::types::{
    CacheCheckFile, CacheCheckResult, DownloadFile, DownloadResult, MediaItemType, build_magnet_uri,
};

use std::time::Duration;

use crate::models::{
    StremthruCacheCheck, StremthruErrorResponse, StremthruFile, StremthruLink, StremthruNewz,
    StremthruNewzAdd, StremthruResponse, StremthruTorz, StremthruTorznabResponse, StremthruUser,
    parse_torrent_status,
};
use crate::{PROFILE, debrid_service};

mod account;
mod downloads;
mod links;
mod torznab;

pub use account::fetch_user_info;
use downloads::parse_quota_interval;
pub use downloads::{
    AddTorrentOutcome, add_newz, add_torrent, check_cache, download_result_from_newz,
    download_result_from_torz,
};
#[cfg(test)]
use downloads::{cache_check_key, classify_add_torrent_rejection};
#[cfg(test)]
use links::describe_empty_link_response;
pub use links::{GeneratedLink, generate_link};
pub use torznab::scrape_torznab;

/// Attach StremThru's per-store routing headers (`x-stremthru-store-name` and
/// the `Bearer` authorization) to a request builder.
trait StoreHeaders {
    fn store_headers(self, store: &str, api_key: &str) -> Self;
}

impl StoreHeaders for reqwest::RequestBuilder {
    fn store_headers(self, store: &str, api_key: &str) -> Self {
        self.header("x-stremthru-store-name", store).header(
            "x-stremthru-store-authorization",
            format!("Bearer {api_key}"),
        )
    }
}
/// Default per-store cooldown when a 429 carries no parseable quota message.
/// Longer than the HTTP layer's 10 s service pause because store 429s here are
/// quota-style (e.g. TorBox's 60 adds/hour), not burst throttles.
const DEFAULT_STORE_COOLDOWN_SECS: u64 = 60;

fn store_cooldown_key(store: &str) -> String {
    format!("plugin:stremthru:store-cooldown:{store}")
}

/// Pause a single store after it rate-limited us: only the throttled store
/// sits out; the rest keep serving requests.
async fn set_store_cooldown(
    redis: &redis::aio::ConnectionManager,
    store: &str,
    duration: Duration,
) {
    let mut conn = redis.clone();
    let _result: Result<(), _> = AsyncCommands::set_ex(
        &mut conn,
        store_cooldown_key(store),
        1u8,
        duration.as_secs().max(1),
    )
    .await;
}

/// Remaining cooldown for a store, if one is active.
pub async fn store_cooldown_remaining(
    redis: &redis::aio::ConnectionManager,
    store: &str,
) -> Option<u64> {
    let mut conn = redis.clone();
    let ttl: i64 = redis::cmd("TTL")
        .arg(store_cooldown_key(store))
        .query_async(&mut conn)
        .await
        .ok()?;
    u64::try_from(ttl).ok().filter(|t| *t > 0)
}

/// A store-scoped request was withheld or rejected because the store is
/// rate-limiting us. Not a store failure: callers should leave the store's
/// health score alone and move on.
#[derive(Debug)]
pub struct StoreRateLimited {
    pub retry_after: Duration,
    /// True when this request triggered the cooldown (fresh 429); false when
    /// an already-active cooldown short-circuited the request before sending.
    pub fresh: bool,
}

impl std::fmt::Display for StoreRateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "store rate limited; retry in {}s",
            self.retry_after.as_secs()
        )
    }
}

impl std::error::Error for StoreRateLimited {}

/// A store-scoped response that cleared the rate-limit gate.
enum StoreSend {
    /// 2xx — body not yet consumed.
    Ok(reqwest::Response),
    /// Non-2xx, non-rate-limit; body already read for diagnostics.
    Rejected {
        status: reqwest::StatusCode,
        body: String,
    },
}

/// Detect a rate-limit rejection and compute its cooldown. Matches both a
/// 429 status and the proxied `TOO_MANY_REQUESTS` error code, since StremThru
/// relays the upstream store's envelope:
/// `{"error":{"code":"TOO_MANY_REQUESTS","message":"60 per 1 hour"}}`.
fn rate_limit_cooldown(status: reqwest::StatusCode, body: &str) -> Option<Duration> {
    let error = StremthruErrorResponse::parse(body).error;
    if status != reqwest::StatusCode::TOO_MANY_REQUESTS && error.code != "TOO_MANY_REQUESTS" {
        return None;
    }
    Some(
        parse_quota_interval(&error.message)
            .unwrap_or(Duration::from_secs(DEFAULT_STORE_COOLDOWN_SECS)),
    )
}

/// Gate every store-scoped request behind the per-store rate-limit cooldown:
/// skip the call while the store is cooling down, and start a cooldown when
/// the store answers with a quota rejection.
async fn send_store<F>(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    store: &str,
    make_request: F,
) -> anyhow::Result<StoreSend>
where
    F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
{
    if let Some(remaining) = store_cooldown_remaining(redis, store).await {
        return Err(StoreRateLimited {
            retry_after: Duration::from_secs(remaining),
            fresh: false,
        }
        .into());
    }

    let response = http.send(PROFILE, make_request).await?;
    if response.status().is_success() {
        return Ok(StoreSend::Ok(response));
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if let Some(retry_after) = rate_limit_cooldown(status, &body) {
        set_store_cooldown(redis, store, retry_after).await;
        return Err(StoreRateLimited {
            retry_after,
            fresh: true,
        }
        .into());
    }

    Ok(StoreSend::Rejected { status, body })
}

/// Dedupe-keyed variant of [`send_store`] for cacheable GETs. Non-2xx,
/// non-rate-limit responses are returned for the caller to interpret, same
/// as before the gate existed.
async fn send_store_data<F>(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    store: &str,
    dedupe_key: String,
    make_request: F,
) -> anyhow::Result<Arc<HttpResponseData>>
where
    F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
{
    if let Some(remaining) = store_cooldown_remaining(redis, store).await {
        return Err(StoreRateLimited {
            retry_after: Duration::from_secs(remaining),
            fresh: false,
        }
        .into());
    }

    let response = http
        .send_data(PROFILE, Some(dedupe_key), make_request)
        .await?;
    if !response.status().is_success()
        && let Some(retry_after) =
            rate_limit_cooldown(response.status(), &response.text().unwrap_or_default())
    {
        set_store_cooldown(redis, store, retry_after).await;
        return Err(StoreRateLimited {
            retry_after,
            fresh: true,
        }
        .into());
    }

    Ok(response)
}

#[cfg(test)]
mod tests;
