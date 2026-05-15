//! AvailNZB (snzb.stream) crowdsourced NZB availability pre-filter.
//!
//! The cache-check hook would otherwise mark every NZB candidate as
//! `Cached` without probing — downloading the NZB XML plus STAT-ing
//! segments would exhaust indexer rate limits well before any download
//! starts. AvailNZB offers a cheap URL-keyed pre-filter: clients query
//! whether the crowdsourced dataset has marked an NZB URL available based
//! on prior playback outcomes, and optionally report their own outcomes
//! back to feed the dataset.
//!
//! Response shape from `GET /api/v1/status/url?url=<nzb>`:
//! ```json
//! {"url":"...","available":bool,"summary":{...},"release_name":"","download_link":"","size":0}
//! ```
//! For an unreported URL `available` is `false` and `summary` is `{}`. We
//! treat that case as `Unknown` (no signal) rather than `Unavailable` so
//! the screen only filters releases AvailNZB actually has data on.

use std::time::Duration;

use redis::AsyncCommands;
use riven_core::http::{HttpClient, HttpServiceProfile};
use serde::Deserialize;
use sha1::{Digest, Sha1};

pub const DEFAULT_BASE_URL: &str = "https://snzb.stream";

pub const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("availnzb").with_rate_limit(60, Duration::from_secs(60));

const AVAILABLE_TTL_SECS: u64 = 6 * 60 * 60;
const UNAVAILABLE_TTL_SECS: u64 = 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Availability {
    Available,
    Unavailable,
    Unknown,
}

#[derive(Debug, Deserialize)]
struct StatusResponse {
    #[serde(default)]
    available: bool,
    #[serde(default)]
    summary: serde_json::Value,
}

fn result_key(nzb_url: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(nzb_url.as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("riven:availnzb:url:{hex}")
}

pub async fn check_url(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    nzb_url: &str,
) -> Availability {
    if nzb_url.is_empty() {
        return Availability::Unknown;
    }
    let mut conn = redis.clone();
    let key = result_key(nzb_url);
    if let Ok(Some(cached)) = AsyncCommands::get::<_, Option<String>>(&mut conn, &key).await {
        return decode_cached(&cached);
    }

    let endpoint = format!("{}/api/v1/status/url", base_url.trim_end_matches('/'));
    let resp = match http
        .send_data(PROFILE, Some(format!("availnzb:{nzb_url}")), |client| {
            client.get(&endpoint).query(&[("url", nzb_url)])
        })
        .await
    {
        Ok(r) => r,
        Err(error) => {
            tracing::debug!(%error, "availnzb status request failed");
            return Availability::Unknown;
        }
    };
    if !resp.status().is_success() {
        tracing::debug!(status = %resp.status(), "availnzb status returned non-success");
        return Availability::Unknown;
    }
    let parsed: StatusResponse = match resp.json() {
        Ok(v) => v,
        Err(error) => {
            tracing::debug!(%error, "availnzb response not JSON-parseable");
            return Availability::Unknown;
        }
    };

    let has_reports = matches!(&parsed.summary, serde_json::Value::Object(map) if !map.is_empty());
    let (outcome, ttl) = if parsed.available {
        (Availability::Available, AVAILABLE_TTL_SECS)
    } else if has_reports {
        (Availability::Unavailable, UNAVAILABLE_TTL_SECS)
    } else {
        // No crowdsourced data — don't cache an authoritative answer; a
        // later query may find a report has been filed in the meantime.
        return Availability::Unknown;
    };

    let _: redis::RedisResult<()> =
        AsyncCommands::set_ex(&mut conn, &key, encode_cached(outcome), ttl).await;
    outcome
}

fn encode_cached(o: Availability) -> &'static str {
    match o {
        Availability::Available => "a",
        Availability::Unavailable => "u",
        Availability::Unknown => "?",
    }
}

fn decode_cached(s: &str) -> Availability {
    match s {
        "a" => Availability::Available,
        "u" => Availability::Unavailable,
        _ => Availability::Unknown,
    }
}

/// Report a playback outcome to AvailNZB. `status = true` means the NZB
/// streamed successfully; `false` means a STAT sample (or ingest) found
/// the articles missing. Silently no-ops when `api_key` is empty so
/// callers can hand the configured key in unconditionally.
pub async fn report(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    nzb_url: &str,
    status: bool,
    release_name: Option<&str>,
) {
    if api_key.is_empty() || nzb_url.is_empty() {
        return;
    }
    let endpoint = format!("{}/api/v1/report", base_url.trim_end_matches('/'));
    let mut body = serde_json::json!({
        "url": nzb_url,
        "status": status,
    });
    if let Some(name) = release_name
        && !name.is_empty()
        && let Some(obj) = body.as_object_mut()
    {
        obj.insert("release_name".to_string(), serde_json::Value::String(name.to_string()));
    }

    let result = http
        .send(PROFILE, |client| {
            client
                .post(&endpoint)
                .header("X-API-Key", api_key)
                .json(&body)
        })
        .await;

    match result {
        Ok(resp) if resp.status().is_success() => {
            tracing::debug!(nzb_url, status, "availnzb report submitted");
        }
        Ok(resp) => {
            tracing::debug!(
                nzb_url,
                status,
                http_status = %resp.status(),
                "availnzb report rejected"
            );
        }
        Err(error) => {
            tracing::debug!(nzb_url, status, %error, "availnzb report failed");
        }
    }
}

/// Convenience wrapper: read the API key + base URL from plugin settings
/// and spawn an unawaited report. Spawns nothing when AvailNZB is
/// disabled or no API key is configured.
pub fn spawn_report_if_configured(
    http: HttpClient,
    settings: &riven_core::settings::PluginSettings,
    nzb_url: String,
    status: bool,
    release_name: Option<String>,
) {
    if settings.get_or("availnzbenabled", "false") == "false" {
        return;
    }
    let Some(api_key) = settings.get("availnzbapikey").filter(|k| !k.is_empty()) else {
        return;
    };
    let api_key = api_key.to_string();
    let base_url = settings
        .get_or("availnzburl", DEFAULT_BASE_URL)
        .to_string();
    tokio::spawn(async move {
        report(
            &http,
            &base_url,
            &api_key,
            &nzb_url,
            status,
            release_name.as_deref(),
        )
        .await;
    });
}
