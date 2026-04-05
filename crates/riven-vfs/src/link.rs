/// Fetch a fresh stream URL from the debrid service for the given entry.
/// Updates the DB with the resolved URL and returns it.
pub fn resolve_stream_url(
    download_url: Option<&str>,
    link_request_tx: &tokio::sync::mpsc::Sender<crate::LinkRequest>,
    pool: &sqlx::PgPool,
    entry_id: i64,
    runtime: &tokio::runtime::Handle,
) -> Option<String> {
    let dl_url = download_url?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    if link_request_tx
        .blocking_send(crate::LinkRequest {
            download_url: dl_url.to_string(),
            response_tx: tx,
        })
        .is_err()
    {
        return None;
    }

    match runtime.block_on(rx) {
        Ok(Some(url)) => {
            let _ = runtime.block_on(riven_db::repo::update_stream_url(pool, entry_id, &url));
            Some(url)
        }
        _ => None,
    }
}
