use crate::cache::{
    cache_get, cache_put, read_from_cache, store_fetched_chunks, ChunkCache,
};
use crate::chunks::Chunk;
use crate::detect::ReadType;
use crate::prefetch::Prefetch;
use crate::stream::create_stream_request;

/// Fetch a byte range from `url` and return the raw bytes.
pub async fn fetch_range(
    client: &reqwest::Client,
    url: &str,
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<u8>> {
    let resp = create_stream_request(client, url, start, Some(end)).await?;
    let bytes = resp.bytes().await?;
    Ok(bytes.to_vec())
}

/// Parameters for a single `read()` dispatch.
pub struct ReadParams<'a> {
    pub read_type: ReadType,
    pub start: u64,
    pub end: u64,
    pub size: u32,
    pub ino: u64,
    pub stream_url: &'a str,
    pub needed_chunks: &'a [Chunk],
}

/// Outcome of a `serve_read` call.
pub enum ReadOutcome {
    Data(Vec<u8>),
    Empty,
    /// FUSE error code (libc constant).
    Error(i32),
}

/// Serve a read request, dispatching on read type.
///
/// Handles CacheHit, HeaderScan, FooterScan, GeneralScan, BodyRead, and FooterRead.
pub fn serve_read(
    params: &ReadParams<'_>,
    chunk_cache: &ChunkCache,
    client: &reqwest::Client,
    runtime: &tokio::runtime::Handle,
    prefetch: &mut Option<Prefetch>,
    debug_logging: bool,
) -> ReadOutcome {
    match params.read_type {
        ReadType::CacheHit => {
            let buf = read_from_cache(chunk_cache, params.needed_chunks, params.ino, params.start, params.end);
            ReadOutcome::Data(buf)
        }

        ReadType::HeaderScan => {
            let fetch_start = params.needed_chunks.first().map(|c| c.start).unwrap_or(params.start);
            let fetch_end = params.needed_chunks.last().map(|c| c.end).unwrap_or(params.end);

            match runtime.block_on(fetch_range(client, params.stream_url, fetch_start, fetch_end)) {
                Ok(data) => {
                    store_fetched_chunks(chunk_cache, params.ino, params.needed_chunks, &data, fetch_start);
                    let ret_start = (params.start - fetch_start) as usize;
                    let ret_end = ret_start + (params.end - params.start + 1) as usize;
                    ReadOutcome::Data(data[ret_start..ret_end.min(data.len())].to_vec())
                }
                Err(e) => {
                    tracing::error!(error = %e, "stream read failed");
                    ReadOutcome::Error(libc::EIO)
                }
            }
        }

        ReadType::FooterScan | ReadType::GeneralScan => {
            *prefetch = None;
            let exact_key = (params.ino, params.start, params.end);
            if let Some(data) = cache_get(chunk_cache, exact_key) {
                ReadOutcome::Data(data)
            } else {
                match runtime.block_on(fetch_range(client, params.stream_url, params.start, params.end)) {
                    Ok(data) => {
                        cache_put(chunk_cache, exact_key, data.clone());
                        ReadOutcome::Data(data)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "stream read failed");
                        ReadOutcome::Error(libc::EIO)
                    }
                }
            }
        }

        ReadType::BodyRead | ReadType::FooterRead => {
            let bytes_needed = (params.end - params.start + 1) as usize;
            let need_restart = prefetch
                .as_ref()
                .map(|p| !p.is_valid_for(params.start))
                .unwrap_or(true);

            if need_restart {
                if debug_logging {
                    tracing::debug!(position = params.start, "starting prefetch task");
                }
                *prefetch = Some(Prefetch::start(client.clone(), params.stream_url.to_string(), params.start, runtime));
            }

            let pf = prefetch.as_mut().unwrap();
            match pf.read(params.start, bytes_needed, runtime) {
                Ok(data) => ReadOutcome::Data(data),
                Err(e) => {
                    tracing::error!(error = %e, "prefetch read failed");
                    *prefetch = None;
                    ReadOutcome::Error(libc::EIO)
                }
            }
        }
    }
}

/// Resolve a stream URL for an open file, either via the link-resolver channel
/// or falling back to the cached stream URL.
///
/// Returns `Some(url)` on success, or `None` if resolution fails (the caller
/// should send an EIO or ENOENT reply).
pub fn resolve_stream_url(
    download_url: Option<&str>,
    stream_url: Option<&str>,
    link_request_tx: &tokio::sync::mpsc::Sender<crate::LinkRequest>,
    pool: &sqlx::PgPool,
    entry_id: i64,
    runtime: &tokio::runtime::Handle,
) -> Option<String> {
    if let Some(dl_url) = download_url {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let link_req = crate::LinkRequest {
            download_url: dl_url.to_string(),
            response_tx: tx,
        };
        if link_request_tx.blocking_send(link_req).is_err() {
            return None;
        }
        match runtime.block_on(rx) {
            Ok(Some(url)) => {
                let _ = runtime.block_on(riven_db::repo::update_stream_url(pool, entry_id, &url));
                Some(url)
            }
            _ => None,
        }
    } else {
        stream_url.map(|s| s.to_string())
    }
}
