use std::time::Duration;

use tokio::time::sleep;

/// Number of attempts matching riven-ts `BaseDataSource.requestAttempts`.
pub const DEFAULT_ATTEMPTS: u32 = 3;

/// Base backoff delay in seconds, matching riven-ts (5 s base, exponential).
const BACKOFF_BASE_SECS: u64 = 5;

/// Returns `true` for transient network errors that warrant a retry.
/// Covers TCP connection failures and timeouts, but not application-level
/// errors (bad status codes, decode failures, invalid URLs).
fn is_transient(e: &reqwest::Error) -> bool {
    e.is_connect() || e.is_timeout()
}

/// Send an HTTP request with automatic retry on transient network errors.
///
/// `make_request` is called once per attempt and must return a fresh
/// `RequestBuilder` each time (the builder is consumed by `.send()`).
///
/// Retry schedule (matching riven-ts exponential backoff, 5 s base):
/// - attempt 1 fails → wait  5 s → attempt 2
/// - attempt 2 fails → wait 10 s → attempt 3
/// - attempt 3 fails → propagate error
///
/// Non-transient errors (HTTP status, body decode, redirect loops) are
/// returned immediately without retrying.
pub async fn send_with_retry<F>(
    attempts: u32,
    make_request: F,
) -> reqwest::Result<reqwest::Response>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    debug_assert!(attempts >= 1, "attempts must be at least 1");

    let mut attempt = 0;
    loop {
        let is_last = attempt + 1 >= attempts;
        match make_request().send().await {
            Ok(resp) => return Ok(resp),
            Err(e) if !is_last && is_transient(&e) => {
                let delay = Duration::from_secs(BACKOFF_BASE_SECS * (1 << attempt));
                tracing::debug!(
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

/// Convenience wrapper that uses [`DEFAULT_ATTEMPTS`].
pub async fn send<F>(make_request: F) -> reqwest::Result<reqwest::Response>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    send_with_retry(DEFAULT_ATTEMPTS, make_request).await
}
