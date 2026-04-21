use std::time::Duration;

const STREAM_LINK_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Request to resolve a stream link for a file.
#[derive(Debug)]
pub struct LinkRequest {
    pub download_url: String,
    /// The store/provider that originally created this download link (e.g. "torbox", "realdebrid").
    /// When set, only that store should be used to regenerate the stream URL.
    pub provider: Option<String>,
    pub response_tx: tokio::sync::oneshot::Sender<Option<String>>,
}

/// Fetch a fresh stream URL from the debrid service.
pub async fn request_stream_url(
    download_url: Option<&str>,
    provider: Option<&str>,
    link_request_tx: &tokio::sync::mpsc::Sender<LinkRequest>,
) -> Option<String> {
    let dl_url = download_url?;
    let (tx, rx) = tokio::sync::oneshot::channel();

    let request = LinkRequest {
        download_url: dl_url.to_string(),
        provider: provider.map(str::to_owned),
        response_tx: tx,
    };

    tokio::time::timeout(STREAM_LINK_REQUEST_TIMEOUT, link_request_tx.send(request))
        .await
        .ok()?
        .ok()?;

    tokio::time::timeout(STREAM_LINK_REQUEST_TIMEOUT, rx)
        .await
        .ok()?
        .ok()
        .flatten()
}

/// Fetch a fresh stream URL from the debrid service from a synchronous caller.
pub fn request_stream_url_blocking(
    download_url: Option<&str>,
    provider: Option<&str>,
    link_request_tx: &tokio::sync::mpsc::Sender<LinkRequest>,
    runtime: &tokio::runtime::Handle,
) -> Option<String> {
    tokio::task::block_in_place(|| runtime.block_on(request_stream_url(download_url, provider, link_request_tx)))
}
