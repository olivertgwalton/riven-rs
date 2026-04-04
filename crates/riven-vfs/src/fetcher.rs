use bytes::{Bytes, BytesMut};

use crate::cache::{cache_get, cache_put, RangeCache};
use crate::chunks::ChunkRange;
use crate::detect::ReadType;
use crate::prefetch::Prefetch;
use crate::stream::fetch_range;

pub enum ReadOutcome {
    Data(Bytes),
    Error(i32),
}

fn fetch_and_cache_range(
    ino: u64,
    chunk: ChunkRange,
    stream_url: &str,
    cache: &RangeCache,
    client: &reqwest::Client,
    runtime: &tokio::runtime::Handle,
) -> Result<Bytes, ()> {
    let key = (ino, chunk.start, chunk.end);
    if let Some(data) = cache_get(cache, key) {
        return Ok(data);
    }

    match runtime.block_on(fetch_range(client, stream_url, chunk.start, chunk.end)) {
        Ok(data) => {
            cache_put(cache, key, data.clone());
            Ok(data)
        }
        Err(e) => {
            tracing::error!(error = %e, "range fetch failed");
            Err(())
        }
    }
}

fn read_cached_chunks(
    ino: u64,
    start: u64,
    end: u64,
    chunks: &[ChunkRange],
    cache: &RangeCache,
) -> ReadOutcome {
    let total_len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
    let mut full = BytesMut::with_capacity(total_len);

    for chunk in chunks {
        let Some(data) = cache_get(cache, (ino, chunk.start, chunk.end)) else {
            return ReadOutcome::Error(libc::EIO);
        };
        full.extend_from_slice(&data);
    }

    let offset = (start - chunks[0].start) as usize;
    let slice_len = (end - start + 1) as usize;
    let full = full.freeze();
    ReadOutcome::Data(full.slice(offset..(offset + slice_len).min(full.len())))
}

fn read_scan_range(
    ino: u64,
    start: u64,
    end: u64,
    chunk: ChunkRange,
    should_cache: bool,
    stream_url: &str,
    cache: &RangeCache,
    client: &reqwest::Client,
    runtime: &tokio::runtime::Handle,
) -> ReadOutcome {
    let full = if should_cache {
        match fetch_and_cache_range(ino, chunk, stream_url, cache, client, runtime) {
            Ok(data) => data,
            Err(()) => return ReadOutcome::Error(libc::EIO),
        }
    } else {
        match runtime.block_on(fetch_range(client, stream_url, start, end)) {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(error = %e, "range fetch failed");
                return ReadOutcome::Error(libc::EIO);
            }
        }
    };

    if !should_cache {
        return ReadOutcome::Data(full);
    }

    let slice_start = (start - chunk.start) as usize;
    let slice_end = slice_start + (end - start + 1) as usize;
    ReadOutcome::Data(full.slice(slice_start..slice_end.min(full.len())))
}

fn ensure_prefetch(
    prefetch: &mut Option<Prefetch>,
    start: u64,
    client: &reqwest::Client,
    stream_url: &str,
    runtime: &tokio::runtime::Handle,
    debug_logging: bool,
) -> bool {
    let need_restart = prefetch
        .as_ref()
        .map(|p| !p.is_valid_for(start))
        .unwrap_or(true);
    if need_restart {
        if debug_logging {
            tracing::debug!(position = start, "starting stream reader");
        }
        *prefetch = Prefetch::start(client.clone(), stream_url.to_string(), start, runtime);
    }
    prefetch.is_some()
}

fn read_body(
    ino: u64,
    chunks: &[ChunkRange],
    start: u64,
    end: u64,
    client: &reqwest::Client,
    stream_url: &str,
    runtime: &tokio::runtime::Handle,
    cache: &RangeCache,
    prefetch: &mut Option<Prefetch>,
    debug_logging: bool,
) -> ReadOutcome {
    let Some(first_missing) = chunks
        .iter()
        .find(|chunk| cache_get(cache, (ino, chunk.start, chunk.end)).is_none())
        .copied()
    else {
        return read_cached_chunks(ino, start, end, chunks, cache);
    };

    for attempt in 0..2 {
        if !ensure_prefetch(
            prefetch,
            first_missing.start,
            client,
            stream_url,
            runtime,
            debug_logging,
        ) {
            tracing::error!("failed to start stream reader");
            return ReadOutcome::Error(libc::EIO);
        }

        let mut full = BytesMut::with_capacity(chunks.iter().map(|chunk| chunk.len()).sum());
        let mut failed = false;

        for chunk in chunks {
            let key = (ino, chunk.start, chunk.end);
            let data = if let Some(cached) = cache_get(cache, key) {
                cached
            } else {
                match prefetch.as_mut().unwrap().read(chunk.start, chunk.len()) {
                    Ok(data) => {
                        cache_put(cache, key, data.clone());
                        data
                    }
                    Err(e) => {
                        if attempt == 0 {
                            tracing::warn!(error = %e, "stream read failed, retrying once");
                            *prefetch = None;
                            failed = true;
                            break;
                        }
                        tracing::error!(error = %e, "stream read failed after retry");
                        *prefetch = None;
                        return ReadOutcome::Error(libc::EIO);
                    }
                }
            };

            full.extend_from_slice(&data);
        }

        if failed {
            continue;
        }

        let offset = (start - chunks[0].start) as usize;
        let slice_len = (end - start + 1) as usize;
        let full = full.freeze();
        return ReadOutcome::Data(full.slice(offset..(offset + slice_len).min(full.len())));
    }

    ReadOutcome::Error(libc::EIO)
}

pub fn serve_read(
    read_type: ReadType,
    ino: u64,
    start: u64,
    end: u64,
    chunks: &[ChunkRange],
    stream_url: &str,
    cache: &RangeCache,
    client: &reqwest::Client,
    runtime: &tokio::runtime::Handle,
    prefetch: &mut Option<Prefetch>,
    debug_logging: bool,
) -> ReadOutcome {
    match read_type {
        ReadType::HeaderScan => read_scan_range(
            ino, start, end, chunks[0], true, stream_url, cache, client, runtime,
        ),
        ReadType::FooterScan | ReadType::FooterRead => {
            let chunk = *chunks.last().unwrap_or(&chunks[0]);
            read_scan_range(
                ino, start, end, chunk, true, stream_url, cache, client, runtime,
            )
        }
        ReadType::GeneralScan => read_scan_range(
            ino,
            start,
            end,
            ChunkRange { start, end },
            false,
            stream_url,
            cache,
            client,
            runtime,
        ),
        ReadType::BodyRead => read_body(
            ino,
            chunks,
            start,
            end,
            client,
            stream_url,
            runtime,
            cache,
            prefetch,
            debug_logging,
        ),
        ReadType::CacheHit => read_cached_chunks(ino, start, end, chunks, cache),
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
