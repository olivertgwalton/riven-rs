use std::time::Duration;

const STREAM_LINK_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// HTTP statuses from a debrid link-generation request that mean the torrent
/// is permanently gone on the service — not a transient blip. Seeing one of
/// these is the signal to blacklist the stream and re-download from another
/// candidate, rather than retrying the same dead link forever.
pub fn is_fatal_status_code(status: u16) -> bool {
    matches!(status, 404 | 410 | 451)
}

/// Request to resolve a stream link for a file.
#[derive(Debug)]
pub struct LinkRequest {
    pub download_url: String,
    /// The store/provider that originally created this download link (e.g. "torbox", "realdebrid").
    /// When set, only that store should be used to regenerate the stream URL.
    pub provider: Option<String>,
    /// The `filesystem_entries` row this request is resolving for, when known.
    /// Lets the link-request consumer blacklist the entry's stream and trigger
    /// a re-download if the debrid service reports the torrent is dead.
    pub entry_id: Option<i64>,
    /// The stream URL the caller already knows is unusable, when refreshing
    /// after a failure. If the debrid service hands back this exact URL it has
    /// nothing fresher to offer — treated as a dead link, same as a 404.
    pub current_url: Option<String>,
    pub response_tx: tokio::sync::oneshot::Sender<Option<String>>,
}

/// Fetch a fresh stream URL from the debrid service.
pub async fn request_stream_url(
    download_url: Option<&str>,
    provider: Option<&str>,
    entry_id: Option<i64>,
    current_url: Option<&str>,
    link_request_tx: &tokio::sync::mpsc::Sender<LinkRequest>,
) -> Option<String> {
    let dl_url = download_url?;
    let (tx, rx) = tokio::sync::oneshot::channel();

    let request = LinkRequest {
        download_url: dl_url.to_string(),
        provider: provider.map(str::to_owned),
        entry_id,
        current_url: current_url.map(str::to_owned),
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
    entry_id: Option<i64>,
    current_url: Option<&str>,
    link_request_tx: &tokio::sync::mpsc::Sender<LinkRequest>,
    runtime: &tokio::runtime::Handle,
) -> Option<String> {
    tokio::task::block_in_place(|| {
        runtime.block_on(request_stream_url(
            download_url,
            provider,
            entry_id,
            current_url,
            link_request_tx,
        ))
    })
}
