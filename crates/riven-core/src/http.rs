use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tokio::time::sleep;

pub const DEFAULT_ATTEMPTS: u32 = 3;
const BACKOFF_BASE_SECS: u64 = 5;
const JITTER: f64 = 0.5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimit {
    pub max: u32,
    pub per: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpServiceProfile {
    pub name: &'static str,
    pub attempts: u32,
    pub rate_limit: Option<RateLimit>,
}

impl HttpServiceProfile {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            attempts: DEFAULT_ATTEMPTS,
            rate_limit: None,
        }
    }

    pub const fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    pub const fn with_rate_limit(mut self, max: u32, per: Duration) -> Self {
        self.rate_limit = Some(RateLimit { max, per });
        self
    }
}

#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    services: Arc<DashMap<&'static str, Arc<ServiceState>>>,
    inflight: Arc<DashMap<String, Arc<InFlightRequest>>>,
}

impl HttpClient {
    pub fn new(inner: reqwest::Client) -> Self {
        Self {
            inner,
            services: Arc::new(DashMap::new()),
            inflight: Arc::new(DashMap::new()),
        }
    }

    pub fn raw(&self) -> &reqwest::Client {
        &self.inner
    }

    pub async fn send<F>(
        &self,
        profile: HttpServiceProfile,
        make_request: F,
    ) -> reqwest::Result<reqwest::Response>
    where
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        let state = self.service_state(profile);
        execute_with_retry(&self.inner, Some(&state), profile.attempts, make_request).await
    }

    pub async fn send_data<F>(
        &self,
        profile: HttpServiceProfile,
        dedupe_key: Option<String>,
        make_request: F,
    ) -> anyhow::Result<Arc<HttpResponseData>>
    where
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        let Some(dedupe_key) = dedupe_key else {
            let response = self.send(profile, make_request).await?;
            return Ok(Arc::new(HttpResponseData::from_response(response).await?));
        };

        let (state, is_leader) = if let Some(existing) = self.inflight.get(&dedupe_key) {
            (existing.clone(), false)
        } else {
            let candidate = Arc::new(InFlightRequest::new());
            match self.inflight.entry(dedupe_key.clone()) {
                dashmap::mapref::entry::Entry::Occupied(entry) => (entry.get().clone(), false),
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    entry.insert(candidate.clone());
                    (candidate, true)
                }
            }
        };

        if is_leader {
            let result = self
                .send(profile, make_request)
                .await
                .map_err(|error| error.to_string())
                .and_then(|response| Ok(response));
            let result = match result {
                Ok(response) => HttpResponseData::from_response(response)
                    .await
                    .map(Arc::new)
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error),
            };
            state.finish(result.clone());
            self.inflight.remove(&dedupe_key);
            return result.map_err(anyhow::Error::msg);
        }

        state.wait().await.map_err(anyhow::Error::msg)
    }

    pub async fn get_json<T, F>(
        &self,
        profile: HttpServiceProfile,
        dedupe_key: String,
        make_request: F,
    ) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        let response = self
            .send_data(profile, Some(dedupe_key), make_request)
            .await?;
        response.error_for_status_ref()?;
        response.json()
    }

    fn service_state(&self, profile: HttpServiceProfile) -> Arc<ServiceState> {
        self.services
            .entry(profile.name)
            .or_insert_with(|| Arc::new(ServiceState::new(profile)))
            .clone()
    }
}

/// Shared state for a deduplicated in-flight HTTP request.
///
/// Uses `watch` rather than `Mutex` + `Notify` to avoid a race where
/// `notify_waiters()` fires between the condition check and `notified().await`
/// registration, leaving waiters blocked forever.  `watch::Receiver::wait_for`
/// always inspects the current value before suspending, so late subscribers
/// see the result immediately regardless of when `finish` was called.
#[derive(Debug)]
struct InFlightRequest {
    tx: watch::Sender<Option<Result<Arc<HttpResponseData>, String>>>,
}

impl InFlightRequest {
    fn new() -> Self {
        let (tx, _) = watch::channel(None);
        Self { tx }
    }

    fn finish(&self, result: Result<Arc<HttpResponseData>, String>) {
        // Errors only occur if all receivers were dropped (i.e. all waiters
        // cancelled), which is harmless.
        let _ = self.tx.send(Some(result));
    }

    async fn wait(&self) -> Result<Arc<HttpResponseData>, String> {
        let mut rx = self.tx.subscribe();
        // wait_for checks the current value first, then waits for changes —
        // no missed-notification race possible.
        //
        // RecvError means the Sender was dropped before finish() was called,
        // which can happen if the leader future was cancelled (e.g. by a
        // select! timeout).  Treat it as a transient failure so the caller
        // retries the request independently rather than panicking.
        rx.wait_for(|v| v.is_some())
            .await
            .map_err(|_| "inflight leader cancelled before completing request".to_string())?
            .clone()
            .unwrap() // safe: predicate guarantees Some(_)
    }
}

#[derive(Clone, Debug)]
pub struct HttpResponseData {
    status: StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Bytes,
}

impl HttpResponseData {
    async fn from_response(response: reqwest::Response) -> anyhow::Result<Self> {
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.bytes().await?;
        Ok(Self {
            status,
            headers,
            body,
        })
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        &self.headers
    }

    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    pub fn text(&self) -> anyhow::Result<String> {
        Ok(String::from_utf8(self.body.to_vec())?)
    }

    pub fn json<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(serde_json::from_slice(&self.body)?)
    }

    pub fn error_for_status_ref(&self) -> anyhow::Result<()> {
        if self.is_success() {
            return Ok(());
        }
        let body = self.text().unwrap_or_default();
        anyhow::bail!(
            "http request failed with status {}: {}",
            self.status,
            body.chars().take(200).collect::<String>()
        )
    }
}

#[derive(Debug)]
struct ServiceState {
    profile: HttpServiceProfile,
    limiter: Mutex<LimiterState>,
}

impl ServiceState {
    fn new(profile: HttpServiceProfile) -> Self {
        Self {
            profile,
            limiter: Mutex::new(LimiterState::default()),
        }
    }

    async fn acquire_slot(&self) {
        loop {
            let wait = {
                let mut limiter = self.limiter.lock();
                limiter.next_wait(self.profile)
            };

            if let Some(wait) = wait {
                sleep(wait).await;
            } else {
                return;
            }
        }
    }

    fn register_retry_after(&self, delay: Duration) {
        let mut limiter = self.limiter.lock();
        limiter.pause_for(delay);
    }
}

#[derive(Debug)]
struct LimiterState {
    window_started: Option<Instant>,
    used_in_window: u32,
    paused_until: Option<Instant>,
}

impl Default for LimiterState {
    fn default() -> Self {
        Self {
            window_started: None,
            used_in_window: 0,
            paused_until: None,
        }
    }
}

impl LimiterState {
    fn next_wait(&mut self, profile: HttpServiceProfile) -> Option<Duration> {
        let now = Instant::now();

        if let Some(paused_until) = self.paused_until {
            if paused_until > now {
                return Some(paused_until - now);
            }
            self.paused_until = None;
        }

        let Some(rate_limit) = profile.rate_limit else {
            return None;
        };

        let window_started = self.window_started.get_or_insert(now);
        if now.duration_since(*window_started) >= rate_limit.per {
            *window_started = now;
            self.used_in_window = 0;
        }

        if self.used_in_window < rate_limit.max {
            self.used_in_window += 1;
            return None;
        }

        Some(
            rate_limit
                .per
                .saturating_sub(now.duration_since(*window_started)),
        )
    }

    fn pause_for(&mut self, delay: Duration) {
        let until = Instant::now() + delay;
        self.paused_until = Some(
            self.paused_until
                .map_or(until, |current| current.max(until)),
        );
    }
}

/// Returns `true` for transient network errors that warrant a retry.
///
/// Covers TCP connection failures, timeouts, and the stale keep-alive race
/// where the server closes a pooled connection just before we reuse it,
/// producing a hyper `IncompleteMessage` before any response bytes arrive.
///
/// Strategy: if `is_request()` is true (transport-level error during the HTTP
/// exchange) we walk the source chain for a `hyper::Error` to confirm it is
/// specifically `IncompleteMessage`.  If reqwest's error chain is opaque and we
/// cannot reach `hyper::Error`, we fall back to treating all request-level
/// transport errors as transient — they share the same retry-safe profile.
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
        // Could not reach hyper::Error through the chain (reqwest may wrap via
        // hyper_util); treat all request-level transport errors as transient.
        return true;
    }

    false
}

fn with_jitter(d: Duration) -> Duration {
    let secs = d.as_secs_f64();
    let jitter = secs * JITTER * (rand() * 2.0 - 1.0);
    Duration::from_secs_f64((secs + jitter).max(0.0))
}

/// Minimal xorshift RNG — avoids pulling in the `rand` crate.
fn rand() -> f64 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let x = seed ^ (seed << 13) ^ (seed >> 7) ^ (seed << 17);
    (x & 0xFFFFFF) as f64 / 0x1000000 as f64
}

/// Parse the `Retry-After` header as a delay duration.
/// Supports both the delay-seconds form ("120") and the HTTP-date form
/// ("Wed, 21 Oct 2015 07:28:00 GMT").
/// Falls back to `None` if the header is absent or unparseable.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();

    // Try delay-seconds first.
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    // Try HTTP-date (RFC 7231 / RFC 1123): "Thu, 01 Jan 1970 00:00:00 GMT".
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(value) {
        let now = chrono::Utc::now();
        let wait = dt.with_timezone(&chrono::Utc) - now;
        if wait > chrono::TimeDelta::zero() {
            return Some(Duration::from_millis(wait.num_milliseconds() as u64));
        }
    }

    None
}

/// Send an HTTP request with automatic retry on transient network errors and
/// 429 rate-limit responses, honouring the `Retry-After` header.
///
/// `make_request` is called once per attempt and must return a fresh
/// `RequestBuilder` each time (the builder is consumed by `.send()`).
///
/// Retry schedule
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
    let client = reqwest::Client::new();
    execute_with_retry(&client, None, attempts, |client| {
        let _ = client;
        make_request()
    })
    .await
}

/// Convenience wrapper that uses [`DEFAULT_ATTEMPTS`].
pub async fn send<F>(make_request: F) -> reqwest::Result<reqwest::Response>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    send_with_retry(DEFAULT_ATTEMPTS, make_request).await
}

async fn execute_with_retry<F>(
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
            Ok(resp) => {
                if !is_last && resp.status() == StatusCode::TOO_MANY_REQUESTS {
                    let retry_after = parse_retry_after(resp.headers())
                        .unwrap_or_else(|| Duration::from_secs(BACKOFF_BASE_SECS));
                    if let Some(service) = service {
                        service.register_retry_after(retry_after);
                    }
                    let delay = with_jitter(retry_after);
                    tracing::debug!(
                        service = service.map(|service| service.profile.name),
                        attempt = attempt + 1,
                        delay_secs = delay.as_secs(),
                        "request rate-limited (429), retrying"
                    );
                    sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                return Ok(resp);
            }
            Err(e) if !is_last && is_transient(&e) => {
                let delay = with_jitter(Duration::from_secs(BACKOFF_BASE_SECS * (1 << attempt)));
                tracing::debug!(
                    service = service.map(|service| service.profile.name),
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

pub mod profiles {
    use std::time::Duration;

    use super::HttpServiceProfile;

    pub const COMET: HttpServiceProfile =
        HttpServiceProfile::new("comet").with_rate_limit(150, Duration::from_secs(60));
    pub const DISCORD_WEBHOOK: HttpServiceProfile = HttpServiceProfile::new("discord_webhook");
    pub const EMBY: HttpServiceProfile = HttpServiceProfile::new("emby");
    pub const JELLYFIN: HttpServiceProfile = HttpServiceProfile::new("jellyfin");
    pub const LISTRR: HttpServiceProfile =
        HttpServiceProfile::new("listrr").with_rate_limit(50, Duration::from_secs(1));
    pub const MDBLIST: HttpServiceProfile =
        HttpServiceProfile::new("mdblist").with_rate_limit(50, Duration::from_secs(1));
    pub const PLEX: HttpServiceProfile = HttpServiceProfile::new("plex");
    pub const SEERR: HttpServiceProfile =
        HttpServiceProfile::new("seerr").with_rate_limit(20, Duration::from_secs(1));
    pub const STREMTHRU: HttpServiceProfile = HttpServiceProfile::new("stremthru");
    pub const TMDB: HttpServiceProfile =
        HttpServiceProfile::new("tmdb").with_rate_limit(40, Duration::from_secs(1));
    pub const TORRENTIO: HttpServiceProfile =
        HttpServiceProfile::new("torrentio").with_rate_limit(150, Duration::from_secs(60));
    pub const TRAKT: HttpServiceProfile = HttpServiceProfile::new("trakt");
    pub const TVDB: HttpServiceProfile =
        HttpServiceProfile::new("tvdb").with_rate_limit(25, Duration::from_secs(1));
    pub const TVMAZE: HttpServiceProfile =
        HttpServiceProfile::new("tvmaze").with_rate_limit(20, Duration::from_secs(10));
    pub const WEBHOOK_JSON: HttpServiceProfile = HttpServiceProfile::new("json_webhook");

    pub const REALDEBRID: HttpServiceProfile = HttpServiceProfile::new("realdebrid");
    pub const TORBOX: HttpServiceProfile = HttpServiceProfile::new("torbox");
    pub const ALLDEBRID: HttpServiceProfile = HttpServiceProfile::new("alldebrid");
    pub const DEBRIDLINK: HttpServiceProfile = HttpServiceProfile::new("debridlink");
    pub const PREMIUMIZE: HttpServiceProfile = HttpServiceProfile::new("premiumize");

    pub fn media_server(plugin: &'static str) -> HttpServiceProfile {
        match plugin {
            "emby" => EMBY,
            "jellyfin" => JELLYFIN,
            _ => HttpServiceProfile::new(plugin),
        }
    }

    pub fn debrid_service(store: &str) -> HttpServiceProfile {
        match store {
            "realdebrid" => REALDEBRID,
            "torbox" => TORBOX,
            "alldebrid" => ALLDEBRID,
            "debridlink" => DEBRIDLINK,
            "premiumize" => PREMIUMIZE,
            _ => HttpServiceProfile::new("debrid"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use axum::http::{HeaderValue, StatusCode};
    use axum::response::IntoResponse;
    use axum::{Json, Router, routing::get};
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::{HttpClient, HttpServiceProfile};

    async fn spawn_json_server(
        counter: Arc<AtomicUsize>,
    ) -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let app = Router::new().route(
            "/value",
            get({
                let counter = Arc::clone(&counter);
                move || {
                    let counter = Arc::clone(&counter);
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Json(json!({ "ok": true }))
                    }
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });
        Ok((addr, handle))
    }

    async fn spawn_retry_after_server(
        counter: Arc<AtomicUsize>,
    ) -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let app = Router::new().route(
            "/retry",
            get({
                let counter = Arc::clone(&counter);
                move || {
                    let counter = Arc::clone(&counter);
                    async move {
                        let attempt = counter.fetch_add(1, Ordering::SeqCst);
                        if attempt == 0 {
                            let mut response =
                                (StatusCode::TOO_MANY_REQUESTS, "retry later").into_response();
                            response.headers_mut().insert(
                                reqwest::header::RETRY_AFTER,
                                HeaderValue::from_static("1"),
                            );
                            return response;
                        }
                        (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
                    }
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });
        Ok((addr, handle))
    }

    #[tokio::test]
    async fn dedupes_concurrent_get_requests() -> anyhow::Result<()> {
        let counter = Arc::new(AtomicUsize::new(0));
        let (addr, handle) = spawn_json_server(Arc::clone(&counter)).await?;
        let http = HttpClient::new(reqwest::Client::new());
        let url = format!("http://{addr}/value");
        let profile = HttpServiceProfile::new("test-dedupe");

        let (first, second) = tokio::join!(
            http.get_json::<serde_json::Value, _>(profile, url.clone(), |client| client.get(&url)),
            http.get_json::<serde_json::Value, _>(profile, url.clone(), |client| client.get(&url))
        );

        assert_eq!(first?, json!({ "ok": true }));
        assert_eq!(second?, json!({ "ok": true }));
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn retries_429_with_retry_after() -> anyhow::Result<()> {
        let counter = Arc::new(AtomicUsize::new(0));
        let (addr, handle) = spawn_retry_after_server(Arc::clone(&counter)).await?;
        let http = HttpClient::new(reqwest::Client::new());
        let url = format!("http://{addr}/retry");
        let profile = HttpServiceProfile::new("test-retry-after");

        let started = Instant::now();
        let body = http
            .get_json::<serde_json::Value, _>(profile, url.clone(), |client| client.get(&url))
            .await?;

        assert_eq!(body, json!({ "ok": true }));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert!(
            started.elapsed() >= Duration::from_millis(400),
            "retry-after delay should have been honored"
        );

        handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn enforces_rate_limit_windows() -> anyhow::Result<()> {
        let counter = Arc::new(AtomicUsize::new(0));
        let (addr, handle) = spawn_json_server(Arc::clone(&counter)).await?;
        let http = HttpClient::new(reqwest::Client::new());
        let url = format!("http://{addr}/value");
        let profile = HttpServiceProfile::new("test-rate-limit")
            .with_rate_limit(1, Duration::from_millis(150));

        let started = Instant::now();
        let first = http
            .get_json::<serde_json::Value, _>(profile, format!("{url}?a=1"), |client| {
                client.get(format!("{url}?a=1"))
            })
            .await?;
        let second = http
            .get_json::<serde_json::Value, _>(profile, format!("{url}?a=2"), |client| {
                client.get(format!("{url}?a=2"))
            })
            .await?;

        assert_eq!(first, json!({ "ok": true }));
        assert_eq!(second, json!({ "ok": true }));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert!(
            started.elapsed() >= Duration::from_millis(240),
            "second request should have waited for the next rate-limit window"
        );

        handle.abort();
        Ok(())
    }
}
