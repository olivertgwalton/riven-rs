use bytes::Bytes;

use crate::cache::{cache_get, cache_put, RangeCache};
use crate::detect::ReadType;
use crate::prefetch::Prefetch;
use crate::stream::fetch_range;

pub enum ReadOutcome {
    Data(Bytes),
    Error(i32),
}

pub fn serve_read(
    read_type: ReadType,
    ino: u64,
    start: u64,
    end: u64,
    stream_url: &str,
    cache: &RangeCache,
    client: &reqwest::Client,
    runtime: &tokio::runtime::Handle,
    prefetch: &mut Option<Prefetch>,
    debug_logging: bool,
) -> ReadOutcome {
    match read_type {
        ReadType::RangeFetch => {
            let key = (ino, start, end);
            if let Some(data) = cache_get(cache, key) {
                return ReadOutcome::Data(data);
            }
            match runtime.block_on(fetch_range(client, stream_url, start, end)) {
                Ok(data) => {
                    cache_put(cache, key, data.clone());
                    ReadOutcome::Data(data)
                }
                Err(e) => {
                    tracing::error!(error = %e, "range fetch failed");
                    ReadOutcome::Error(libc::EIO)
                }
            }
        }

        ReadType::Sequential => {
            let bytes_needed = (end - start + 1) as usize;
            let need_restart = prefetch.as_ref().map(|p| !p.is_valid_for(start)).unwrap_or(true);

            if need_restart {
                if debug_logging {
                    tracing::debug!(position = start, "starting stream reader");
                }
                *prefetch = Prefetch::start(client.clone(), stream_url.to_string(), start, runtime);
            }

            match prefetch.as_mut() {
                Some(pf) => match pf.read(start, bytes_needed, runtime) {
                    Ok(data) => ReadOutcome::Data(data),
                    Err(e) => {
                        tracing::error!(error = %e, "stream read failed");
                        *prefetch = None;
                        ReadOutcome::Error(libc::EIO)
                    }
                },
                None => {
                    tracing::error!("failed to start stream reader");
                    ReadOutcome::Error(libc::EIO)
                }
            }
        }
    }
}

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
    if link_request_tx.blocking_send(crate::LinkRequest { download_url: dl_url.to_string(), response_tx: tx }).is_err() {
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
