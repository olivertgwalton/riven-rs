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
pub fn request_stream_url(
    download_url: Option<&str>,
    provider: Option<&str>,
    link_request_tx: &tokio::sync::mpsc::Sender<LinkRequest>,
    runtime: &tokio::runtime::Handle,
) -> Option<String> {
    let dl_url = download_url?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    if link_request_tx
        .blocking_send(LinkRequest {
            download_url: dl_url.to_string(),
            provider: provider.map(str::to_owned),
            response_tx: tx,
        })
        .is_err()
    {
        return None;
    }

    runtime.block_on(rx).ok().flatten()
}
