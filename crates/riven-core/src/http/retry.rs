use std::time::Duration;

use reqwest::header::HeaderMap;
use tokio::time::sleep;

use super::rate_limit::ServiceState;

pub(super) const BACKOFF_BASE_SECS: u64 = 5;
const JITTER: f64 = 0.5;
/// Cap on how long a 429 `Retry-After` pause is registered on the service state.
pub(super) const MAX_RETRY_AFTER_SECS: u64 = 60;

/// Returns `true` for transient network errors that warrant a retry (connection
/// failures, timeouts, stale keep-alive races producing `IncompleteMessage`).
fn is_transient(e: &reqwest::Error) -> bool {
    if e.is_connect() || e.is_timeout() {
        return true;
    }
    if e.is_request() {
        use std::error::Error as StdError;
        let mut src: Option<&dyn StdError> = Some(e);
        while let Some(err) = src {
            if let Some(hyper_err) = err.downcast_ref::<hyper::Error>() {
                return hyper_err.is_incomplete_message();
            }
            src = err.source();
        }
        return true;
    }
    false
}

fn with_jitter(d: Duration) -> Duration {
    let secs = d.as_secs_f64();
    let jitter = secs * JITTER * (rand() * 2.0 - 1.0);
    Duration::from_secs_f64((secs + jitter).max(0.0))
}

/// Minimal xorshift RNG - avoids pulling in the `rand` crate.
fn rand() -> f64 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let x = seed ^ (seed << 13) ^ (seed >> 7) ^ (seed << 17);
    (x & 0xFFFFFF) as f64 / 0x1000000 as f64
}

/// Parse the `Retry-After` header as a duration. Supports both delay-seconds
/// ("120") and HTTP-date ("Wed, 21 Oct 2015 07:28:00 GMT") forms.
pub(super) fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();

    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(value) {
        let wait = dt.with_timezone(&chrono::Utc) - chrono::Utc::now();
        if wait > chrono::TimeDelta::zero() {
            return Some(Duration::from_millis(wait.num_milliseconds() as u64));
        }
    }

    None
}

pub(super) async fn execute_with_retry<F>(
    client: &reqwest::Client,
    service: Option<&ServiceState>,
    attempts: u32,
    make_request: F,
) -> reqwest::Result<reqwest::Response>
where
    F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
{
    debug_assert!(attempts >= 1, "attempts must be at least 1");

    let mut attempt = 0;
    loop {
        let is_last = attempt + 1 >= attempts;

        if let Some(service) = service {
            service.acquire_slot().await;
        }

        match make_request(client).send().await {
            Ok(resp) => return Ok(resp),
            Err(e) if !is_last && is_transient(&e) => {
                let delay = with_jitter(Duration::from_secs(BACKOFF_BASE_SECS * (1 << attempt)));
                tracing::debug!(
                    service = service.map(|s| s.profile.name),
                    attempt = attempt + 1,
                    delay_secs = delay.as_secs(),
                    error = %e,
                    "http request failed, retrying"
                );
                sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}
