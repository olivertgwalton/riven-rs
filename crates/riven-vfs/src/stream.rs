use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use reqwest::{Client, Response, StatusCode};

use riven_core::config::vfs::{
    ACTIVITY_TIMEOUT_SECS, STREAM_RETRY_BASE_DELAY_MS, STREAM_RETRY_MAX_ATTEMPTS,
};

/// A failed stream request, tagged with whether retrying the *same* URL is
/// worthwhile. Only a permanent HTTP status (a 4xx other than 408/425/429)
/// proves the URL itself is bad — every other failure (no response at all, a
/// 5xx, a truncated or mis-ranged body) is treated as transient and retried
/// in-place before the caller escalates to refreshing the stream URL.
struct StreamError {
    transient: bool,
    source: anyhow::Error,
}

impl StreamError {
    fn transient(source: anyhow::Error) -> Self {
        Self {
            transient: true,
            source,
        }
    }

    fn permanent(source: anyhow::Error) -> Self {
        Self {
            transient: false,
            source,
        }
    }
}

fn status_is_transient(status: StatusCode) -> bool {
    status.is_server_error()
        || matches!(
            status,
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS
        )
}

/// Runs `attempt` up to `STREAM_RETRY_MAX_ATTEMPTS` times, backing off
/// exponentially between transient failures. A permanent failure returns
/// immediately; an exhausted transient failure returns its last error. Either
/// way the caller then refreshes the stream URL and retries once more.
async fn with_retry<T, F, Fut>(url: &str, what: &str, mut attempt: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StreamError>>,
{
    let mut tries = 0u32;
    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                tries += 1;
                if !error.transient || tries >= STREAM_RETRY_MAX_ATTEMPTS {
                    return Err(error.source);
                }
                let delay = STREAM_RETRY_BASE_DELAY_MS << (tries - 1);
                tracing::debug!(
                    target: "streaming",
                    url,
                    what,
                    attempt = tries,
                    delay_ms = delay,
                    error = %error.source,
                    "transient stream failure, retrying in-place"
                );
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
    }
}

pub async fn open_stream(client: &Client, url: &str, start: u64) -> Result<Response> {
    with_retry(url, "open", || open_stream_once(client, url, start)).await
}

async fn open_stream_once(client: &Client, url: &str, start: u64) -> Result<Response, StreamError> {
    let range = format!("bytes={start}-");
    let response = client
        .get(url)
        .header("range", &range)
        .header("accept-encoding", "identity")
        .header("connection", "keep-alive")
        .timeout(Duration::from_secs(ACTIVITY_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| StreamError::transient(e.into()))?;

    match response.status() {
        StatusCode::OK if start == 0 => Ok(response),
        StatusCode::PARTIAL_CONTENT => {
            validate_content_range(&response, start, None).map_err(StreamError::transient)?;
            Ok(response)
        }
        status => {
            let error = anyhow::anyhow!(
                "stream request {range} failed with status {status} for {url}"
            );
            Err(if status_is_transient(status) {
                StreamError::transient(error)
            } else {
                StreamError::permanent(error)
            })
        }
    }
}

/// Extract the inclusive end byte of the response body from its
/// `Content-Range` header (`bytes start-end/total`). Returns `None` for
/// a plain `200 OK` (no Content-Range) or an unparseable header — callers
/// then fall back to assuming the body extends to the file's last byte.
pub fn response_body_end(response: &Response) -> Option<u64> {
    let header = response.headers().get(reqwest::header::CONTENT_RANGE)?;
    let s = header.to_str().ok()?;
    let rest = s.strip_prefix("bytes ")?;
    let (range_part, _total) = rest.split_once('/')?;
    let (_start, end) = range_part.split_once('-')?;
    end.parse::<u64>().ok()
}

pub async fn fetch_range(client: &Client, url: &str, start: u64, end: u64) -> Result<Bytes> {
    with_retry(url, "range", || fetch_range_once(client, url, start, end)).await
}

async fn fetch_range_once(
    client: &Client,
    url: &str,
    start: u64,
    end: u64,
) -> Result<Bytes, StreamError> {
    let range = format!("bytes={start}-{end}");
    let expected_len = (end - start + 1) as usize;

    let response = client
        .get(url)
        .header("range", &range)
        .header("accept-encoding", "identity")
        .header("connection", "keep-alive")
        .timeout(Duration::from_secs(ACTIVITY_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| StreamError::transient(e.into()))?;

    let status = response.status();
    if status != StatusCode::PARTIAL_CONTENT {
        let error =
            anyhow::anyhow!("stream range request {range} failed with status {status} for {url}");
        return Err(if status_is_transient(status) {
            StreamError::transient(error)
        } else {
            StreamError::permanent(error)
        });
    }

    validate_content_range(&response, start, Some(end)).map_err(StreamError::transient)?;

    let bytes = response
        .bytes()
        .await
        .map_err(|e| StreamError::transient(e.into()))?;
    if bytes.len() != expected_len {
        return Err(StreamError::transient(anyhow::anyhow!(
            "stream range request {range} returned {} bytes, expected {expected_len} for {url}",
            bytes.len(),
        )));
    }

    Ok(bytes)
}

fn validate_content_range(response: &Response, start: u64, end: Option<u64>) -> Result<()> {
    let Some(content_range) = response.headers().get(reqwest::header::CONTENT_RANGE) else {
        anyhow::bail!("missing content-range header");
    };

    let content_range = content_range.to_str().unwrap_or_default();
    let expected_prefix = match end {
        Some(end) => format!("bytes {start}-{end}/"),
        None => format!("bytes {start}-"),
    };

    if !content_range.starts_with(&expected_prefix) {
        anyhow::bail!("mismatched content-range '{content_range}'");
    }

    Ok(())
}
