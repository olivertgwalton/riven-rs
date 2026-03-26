use anyhow::Result;
use bytes::Bytes;
use reqwest::Client;
use std::time::Duration;

pub async fn fetch_range(client: &Client, url: &str, start: u64, end: u64) -> Result<Bytes> {
    let range = format!("bytes={start}-{end}");

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

    Ok(response.bytes().await?)
}
