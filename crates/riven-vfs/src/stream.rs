use anyhow::Result;
use reqwest::Client;
use std::time::Duration;

/// Create a streaming HTTP request for a byte range.
pub async fn create_stream_request(
    client: &Client,
    url: &str,
    start: u64,
    end: Option<u64>,
) -> Result<reqwest::Response> {
    let range = match end {
        Some(e) => format!("bytes={start}-{e}"),
        None => format!("bytes={start}-"),
    };

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

    if !response.status().is_success() && response.status() != reqwest::StatusCode::PARTIAL_CONTENT
    {
        anyhow::bail!(
            "stream request failed with status {} for {url}",
            response.status()
        );
    }

    Ok(response)
}

/// Read exactly `size` bytes from a response body.
pub async fn read_exact_from_stream(
    response: &mut reqwest::Response,
    size: usize,
) -> Result<Vec<u8>> {
    #[allow(unused_imports)]
    use futures::StreamExt;

    let mut buf = Vec::with_capacity(size);
    let timeout = Duration::from_secs(riven_core::config::vfs::CHUNK_TIMEOUT_SECS);

    while buf.len() < size {
        let chunk = tokio::time::timeout(timeout, response.chunk()).await??;
        match chunk {
            Some(bytes) => buf.extend_from_slice(&bytes),
            None => break,
        }
    }

    buf.truncate(size);
    Ok(buf)
}
