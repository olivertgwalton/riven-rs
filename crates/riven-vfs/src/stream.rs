use anyhow::Result;
use bytes::Bytes;
use reqwest::{Client, Response, StatusCode};
use std::time::Duration;

pub async fn open_stream(client: &Client, url: &str, start: u64) -> Result<Response> {
    let range = format!("bytes={start}-");
    let response = client
        .get(url)
        .header("range", &range)
        .header("accept-encoding", "identity")
        .header("connection", "keep-alive")
        .timeout(Duration::from_secs(
            riven_core::config::vfs::ACTIVITY_TIMEOUT_SECS,
        ))
        .send()
        .await?;

    match response.status() {
        StatusCode::OK if start == 0 => Ok(response),
        StatusCode::PARTIAL_CONTENT => {
            validate_content_range(&response, start, None)?;
            Ok(response)
        }
        status => anyhow::bail!(
            "stream request {} failed with status {} for {url}",
            range,
            status
        ),
    }
}

pub async fn fetch_range(client: &Client, url: &str, start: u64, end: u64) -> Result<Bytes> {
    let range = format!("bytes={start}-{end}");
    let expected_len = (end - start + 1) as usize;

    let response = client
        .get(url)
        .header("range", &range)
        .header("accept-encoding", "identity")
        .header("connection", "keep-alive")
        .timeout(Duration::from_secs(
            riven_core::config::vfs::ACTIVITY_TIMEOUT_SECS,
        ))
        .send()
        .await?;

    if response.status() != StatusCode::PARTIAL_CONTENT {
        anyhow::bail!(
            "stream range request {} failed with status {} for {url}",
            range,
            response.status()
        );
    }

    validate_content_range(&response, start, Some(end))?;

    let bytes = response.bytes().await?;
    if bytes.len() != expected_len {
        anyhow::bail!(
            "stream range request {} returned {} bytes, expected {} for {url}",
            range,
            bytes.len(),
            expected_len
        );
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
