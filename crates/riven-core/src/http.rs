mod client;
mod inflight;
pub mod profiles;
mod rate_limit;
mod response;
mod retry;

pub use client::HttpClient;
pub use profiles::HttpServiceProfile;
pub use rate_limit::RateLimit;
pub use response::HttpResponseData;

/// Returned by [`HttpClient::get_json`] on HTTP 429. The worker slot is freed
/// immediately; callers should re-queue with backoff rather than retrying inline.
#[derive(Debug)]
pub struct RateLimitedError;

impl std::fmt::Display for RateLimitedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rate limited (429)")
    }
}

impl std::error::Error for RateLimitedError {}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

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
