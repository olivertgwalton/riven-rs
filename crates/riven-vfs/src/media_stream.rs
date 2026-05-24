use std::io;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use riven_core::local_source::LocalByteSource;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use crate::cache::{RangeCache, cache_evict, cache_get, cache_put};
use crate::chunks::{ChunkRange, FileLayout};
use crate::detect::{ReadType, detect_read_type};
use crate::stream::{fetch_range, open_stream, response_body_end};

pub enum ReadOutcome {
    Data(Bytes),
    Error(i32),
}

pub struct MediaStream {
    ino: u64,
    file_size: u64,
    layout: FileLayout,
    last_read_end: Option<u64>,
    sequential: Option<SequentialReader>,
}

struct ReadContext<'a> {
    stream_url: &'a str,
    cache: &'a RangeCache,
    client: &'a reqwest::Client,
    runtime: &'a tokio::runtime::Handle,
}

type HttpByteStream = BoxStream<'static, Result<Bytes, io::Error>>;
type ResponseReader = BufReader<StreamReader<HttpByteStream, Bytes>>;

struct SequentialReader {
    read_pos: u64,
    /// Exclusive end byte of the response body. Debrid CDN origins cap
    /// open-ended `bytes=start-` requests to a bounded window, so the body
    /// ends well before EOF. Reading past this returns early-EOF; we must
    /// reopen instead. (The in-process usenet path is unbounded — see
    /// `UsenetSession`, which sets this to the full file size.)
    body_end_exclusive: u64,
    reader: ResponseReader,
}

impl SequentialReader {
    const DISCARD_BUFFER_SIZE: usize = 64 * 1024;

    fn open(
        client: reqwest::Client,
        url: String,
        start_pos: u64,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Self> {
        let response = runtime
            .block_on(open_stream(&client, &url, start_pos))
            .ok()?;
        let body_end_exclusive = response_body_end(&response)
            .map(|end_inclusive| end_inclusive.saturating_add(1))
            .unwrap_or(u64::MAX);
        let stream = response.bytes_stream().map_err(io::Error::other).boxed();
        let reader = BufReader::with_capacity(
            riven_core::config::vfs::CHUNK_SIZE as usize * 2,
            StreamReader::new(stream),
        );

        Some(Self {
            read_pos: start_pos,
            body_end_exclusive,
            reader,
        })
    }

    /// Whether this reader's body still covers `[start, end_inclusive]`.
    /// Returns false when `end_inclusive` falls past the body window so
    /// the caller will reopen at `start` instead of reading an early-EOF.
    fn can_serve(&self, start: u64, end_inclusive: u64) -> bool {
        start >= self.read_pos && end_inclusive < self.body_end_exclusive
    }

    fn read_chunk(
        &mut self,
        chunk: ChunkRange,
        runtime: &tokio::runtime::Handle,
    ) -> io::Result<Bytes> {
        runtime.block_on(self.read_exact_at(chunk.start, chunk.len()))
    }

    async fn read_exact_at(&mut self, pos: u64, size: usize) -> io::Result<Bytes> {
        if pos < self.read_pos {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot rewind active stream from {} to {}",
                    self.read_pos, pos
                ),
            ));
        }

        if pos > self.read_pos {
            self.discard(pos - self.read_pos).await?;
            self.read_pos = pos;
        }

        // Read into uninitialized spare capacity via `read_buf` (no zero-fill
        // of a buffer we immediately overwrite). `take(size)` caps the read at
        // exactly `size` bytes so we never over-read the stream.
        let mut buf = Vec::with_capacity(size);
        let mut limited = (&mut self.reader).take(size as u64);
        while buf.len() < size {
            if limited.read_buf(&mut buf).await? == 0 {
                break;
            }
        }
        if buf.len() < size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stream ended before the requested chunk was filled",
            ));
        }
        self.read_pos += size as u64;

        Ok(Bytes::from(buf))
    }

    async fn discard(&mut self, bytes: u64) -> io::Result<()> {
        let mut remaining = bytes as usize;
        let mut scratch = vec![0; Self::DISCARD_BUFFER_SIZE.min(remaining.max(1))];

        while remaining > 0 {
            let n = scratch.len().min(remaining);
            self.reader.read_exact(&mut scratch[..n]).await?;
            remaining -= n;
        }

        Ok(())
    }
}

impl MediaStream {
    pub fn new(ino: u64, file_size: u64) -> Self {
        Self {
            ino,
            file_size,
            layout: FileLayout::new(file_size),
            last_read_end: None,
            sequential: None,
        }
    }

    pub fn read(
        &mut self,
        start: u64,
        end: u64,
        stream_url: &str,
        cache: &RangeCache,
        client: &reqwest::Client,
        runtime: &tokio::runtime::Handle,
    ) -> ReadOutcome {
        let chunks = self.layout.request_chunks(start, end);
        let ctx = ReadContext {
            stream_url,
            cache,
            client,
            runtime,
        };
        let read_type = detect_read_type(
            self.ino,
            start,
            end,
            (end - start + 1) as usize,
            self.last_read_end,
            &self.layout,
            &chunks,
            ctx.cache,
        );

        tracing::debug!(
            target: "streaming",
            ino = self.ino,
            offset = start,
            size = end - start + 1,
            read_type = ?read_type,
            chunks = chunks.len(),
            "media stream read"
        );

        let Some(first_chunk) = chunks.first().copied() else {
            tracing::error!(ino = self.ino, "media stream read with no chunks");
            return ReadOutcome::Error(libc::EIO);
        };

        let outcome = match read_type {
            ReadType::HeaderScan => self.read_scan_range(start, end, first_chunk, true, &ctx),
            ReadType::FooterScan | ReadType::FooterRead => {
                let chunk = chunks.last().copied().unwrap_or(first_chunk);
                self.read_scan_range(start, end, chunk, true, &ctx)
            }
            ReadType::GeneralScan => {
                self.read_scan_range(start, end, ChunkRange { start, end }, false, &ctx)
            }
            ReadType::BodyRead => self.read_body(&chunks, start, end, &ctx),
            ReadType::CacheHit => self.read_cached_chunks(start, end, &chunks, ctx.cache),
        };

        if matches!(outcome, ReadOutcome::Data(_)) {
            self.last_read_end = Some(end);
        }

        outcome
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    fn read_cached_chunks(
        &self,
        start: u64,
        end: u64,
        chunks: &[ChunkRange],
        cache: &RangeCache,
    ) -> ReadOutcome {
        let Some(first) = chunks.first() else {
            return ReadOutcome::Error(libc::EIO);
        };
        match self.collect_chunk_bytes(chunks, cache) {
            Ok(full) => match slice_request_bytes(&full, start, end, first.start) {
                Some(slice) => ReadOutcome::Data(slice),
                None => {
                    // Poisoned cache entry: data is present but too short to
                    // cover the requested range. Evict all chunks so the next
                    // retry re-fetches from the origin instead of looping on
                    // the same bad entry.
                    for chunk in chunks {
                        cache_evict(cache, (self.ino, chunk.start, chunk.end));
                    }
                    tracing::error!(
                        ino = self.ino,
                        start,
                        end,
                        cached_len = full.len(),
                        "cached chunk set shorter than requested range; evicted"
                    );
                    ReadOutcome::Error(libc::EIO)
                }
            },
            Err(()) => ReadOutcome::Error(libc::EIO),
        }
    }

    fn collect_chunk_bytes(&self, chunks: &[ChunkRange], cache: &RangeCache) -> Result<Bytes, ()> {
        let total_len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
        let mut full = BytesMut::with_capacity(total_len);

        for chunk in chunks {
            let Some(data) = cache_get(cache, (self.ino, chunk.start, chunk.end)) else {
                return Err(());
            };
            full.extend_from_slice(&data);
        }

        Ok(full.freeze())
    }

    fn read_scan_range(
        &mut self,
        start: u64,
        end: u64,
        chunk: ChunkRange,
        should_cache: bool,
        ctx: &ReadContext<'_>,
    ) -> ReadOutcome {
        self.sequential = None;

        let full = if should_cache {
            match cache_get(ctx.cache, (self.ino, chunk.start, chunk.end)) {
                Some(data) => data,
                None => match ctx.runtime.block_on(fetch_range(
                    ctx.client,
                    ctx.stream_url,
                    chunk.start,
                    chunk.end,
                )) {
                    Ok(data) => {
                        let expected = (chunk.end - chunk.start + 1) as usize;
                        if data.len() >= expected {
                            cache_put(ctx.cache, (self.ino, chunk.start, chunk.end), data.clone());
                        }
                        data
                    }
                    Err(error) => {
                        tracing::error!(ino = self.ino, error = %error, "range fetch failed");
                        return ReadOutcome::Error(libc::EIO);
                    }
                },
            }
        } else {
            match ctx
                .runtime
                .block_on(fetch_range(ctx.client, ctx.stream_url, start, end))
            {
                Ok(data) => data,
                Err(error) => {
                    tracing::error!(ino = self.ino, error = %error, "range fetch failed");
                    return ReadOutcome::Error(libc::EIO);
                }
            }
        };

        if !should_cache {
            return ReadOutcome::Data(full);
        }

        match slice_request_bytes(&full, start, end, chunk.start) {
            Some(slice) => ReadOutcome::Data(slice),
            None => {
                tracing::error!(
                    ino = self.ino,
                    start,
                    end,
                    chunk_start = chunk.start,
                    chunk_end = chunk.end,
                    fetched_len = full.len(),
                    "scan range shorter than requested"
                );
                ReadOutcome::Error(libc::EIO)
            }
        }
    }

    fn read_body(
        &mut self,
        chunks: &[ChunkRange],
        start: u64,
        end: u64,
        ctx: &ReadContext<'_>,
    ) -> ReadOutcome {
        let all_cached = chunks
            .iter()
            .all(|chunk| cache_get(ctx.cache, (self.ino, chunk.start, chunk.end)).is_some());
        if all_cached {
            return self.read_cached_chunks(start, end, chunks, ctx.cache);
        }

        for attempt in 0..2 {
            let mut failed = false;

            for chunk in chunks {
                if cache_get(ctx.cache, (self.ino, chunk.start, chunk.end)).is_some() {
                    continue;
                }

                if !self.ensure_sequential_reader_for(chunk.start, chunk.end, ctx) {
                    tracing::error!(ino = self.ino, "failed to start sequential reader");
                    return ReadOutcome::Error(libc::EIO);
                }

                match self.read_body_chunk(*chunk, ctx, attempt) {
                    Ok(data) => cache_put(ctx.cache, (self.ino, chunk.start, chunk.end), data),
                    Err(BodyReadError::Retryable) => {
                        failed = true;
                        break;
                    }
                    Err(BodyReadError::Fatal) => return ReadOutcome::Error(libc::EIO),
                }
            }

            if failed {
                continue;
            }

            return self.read_cached_chunks(start, end, chunks, ctx.cache);
        }

        ReadOutcome::Error(libc::EIO)
    }

    fn ensure_sequential_reader_for(
        &mut self,
        start: u64,
        end_inclusive: u64,
        ctx: &ReadContext<'_>,
    ) -> bool {
        let need_restart = self
            .sequential
            .as_ref()
            .is_none_or(|reader| !reader.can_serve(start, end_inclusive));

        if need_restart {
            tracing::debug!(target: "streaming", ino = self.ino, position = start, "starting sequential reader");
            self.sequential = SequentialReader::open(
                ctx.client.clone(),
                ctx.stream_url.to_string(),
                start,
                ctx.runtime,
            );
        }

        self.sequential.is_some()
    }

    fn read_body_chunk(
        &mut self,
        chunk: ChunkRange,
        ctx: &ReadContext<'_>,
        attempt: usize,
    ) -> Result<Bytes, BodyReadError> {
        match self
            .sequential
            .as_mut()
            .expect("sequential reader must exist after ensure_sequential_reader")
            .read_chunk(chunk, ctx.runtime)
        {
            Ok(data) => Ok(data),
            Err(error) => {
                if attempt == 0 {
                    tracing::warn!(
                        ino = self.ino,
                        error = %error,
                        "stream read failed, retrying once"
                    );
                    self.sequential = None;
                    Err(BodyReadError::Retryable)
                } else {
                    tracing::error!(
                        ino = self.ino,
                        error = %error,
                        "stream read failed after retry"
                    );
                    self.sequential = None;
                    Err(BodyReadError::Fatal)
                }
            }
        }
    }
}

enum BodyReadError {
    Retryable,
    Fatal,
}

fn slice_request_bytes(full: &Bytes, start: u64, end: u64, base_start: u64) -> Option<Bytes> {
    let offset = start.checked_sub(base_start)? as usize;
    if offset >= full.len() {
        return None;
    }
    let requested_len = (end - start + 1) as usize;
    let available_len = full.len() - offset;
    let slice_len = requested_len.min(available_len);
    Some(full.slice(offset..offset + slice_len))
}
/// In-process streaming session for usenet-backed files.
///
/// Each FUSE read is served directly by the streamer's `read_range`
/// (`LocalByteSource`), which fetches exactly the overlapping segments from
/// its decoded-segment cache (or de-duplicated cold fetch) and always runs to
/// completion — so there is no stateful forward-only reader to thrash. The
/// kernel issues reads for one handle in arbitrary order (large read-ahead,
/// 4 MB `max_read`), but order no longer matters: an out-of-order or backward
/// read just hits the cache instead of forcing a stream reopen.
///
/// The read-ahead *lead* that keeps a slow segment from stalling playback is
/// built by a separate fire-and-forget `prefetch`: each read warms a
/// `PREFETCH_LEAD`-byte window ahead of `start`, advancing a monotonic
/// `prefetch_frontier` so the warm work runs ahead of the read position
/// independent of the per-handle read serialization, and never re-warms or
/// runs backward.
pub struct UsenetSession {
    source: Arc<dyn LocalByteSource>,
    info_hash: Arc<str>,
    file_index: usize,
    file_size: u64,
    /// Highest offset the look-ahead prefetch has already been scheduled up
    /// to. Monotonic: forward reads extend it, backward reads leave it alone.
    prefetch_frontier: u64,
    /// Active-streams registry key (`"{info_hash}:{file_index}"`). Registered
    /// on first read, touched periodically, unregistered on drop — restoring
    /// the dashboard "now playing" view for in-process FUSE playback.
    stream_key: String,
    filename: Arc<str>,
    registered: bool,
    reads_since_touch: u32,
}

impl UsenetSession {
    pub fn new(
        source: Arc<dyn LocalByteSource>,
        info_hash: Arc<str>,
        file_index: usize,
        file_size: u64,
        filename: Arc<str>,
    ) -> Self {
        let stream_key = format!("{info_hash}:{file_index}");
        Self {
            source,
            info_hash,
            file_index,
            file_size,
            prefetch_frontier: 0,
            stream_key,
            filename,
            registered: false,
            reads_since_touch: 0,
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn read(&mut self, start: u64, end: u64, runtime: &tokio::runtime::Handle) -> ReadOutcome {
        // Register on the first read and refresh the heartbeat every ~16 reads
        // (each up to 4 MB) — cheap, and well inside the idle-detection window.
        const TOUCH_EVERY_N_READS: u32 = 16;
        if !self.registered {
            self.source.stream_register(
                &self.stream_key,
                &self.info_hash,
                &self.filename,
                self.file_size,
            );
            self.registered = true;
            self.reads_since_touch = 0;
        } else {
            self.reads_since_touch = self.reads_since_touch.wrapping_add(1);
            if self.reads_since_touch >= TOUCH_EVERY_N_READS {
                self.source.stream_touch(&self.stream_key);
                self.reads_since_touch = 0;
            }
        }
        if start >= self.file_size {
            return ReadOutcome::Data(Bytes::new());
        }
        let end = end.min(self.file_size - 1);

        // Warm a look-ahead window so the next sequential reads land in cache.
        // Bytes ahead of the current read to keep pre-fetched. ~46 MB matches
        // the old eager-producer read-ahead depth; deep enough to ride out a
        // multi-second provider latency excursion. Override with
        // `RIVEN_USENET_STREAM_READAHEAD_BYTES`.
        const DEFAULT_PREFETCH_LEAD: u64 = 46 * 1024 * 1024;
        let lead = std::env::var("RIVEN_USENET_STREAM_READAHEAD_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_PREFETCH_LEAD);
        let want_through = (start + lead).min(self.file_size - 1);
        if want_through > self.prefetch_frontier {
            // Cover the gap between what's already scheduled and the new
            // frontier (or from `start` on the first read / a forward seek
            // past the frontier). De-dup + concurrency bounding live in the
            // streamer, so a fire-and-forget overlapping window is cheap.
            let from = self.prefetch_frontier.max(start);
            self.prefetch_frontier = want_through;
            let source = Arc::clone(&self.source);
            let info_hash = Arc::clone(&self.info_hash);
            let file_index = self.file_index;
            runtime.spawn(async move {
                source
                    .prefetch(&info_hash, file_index, from, want_through)
                    .await;
            });
        }

        match runtime.block_on(self.source.read_range(
            &self.info_hash,
            self.file_index,
            start,
            end,
        )) {
            Ok(data) => {
                // Guard against a mid-file short read ever reaching the kernel.
                // The Linux FUSE client treats a read that returns fewer bytes
                // than requested as EOF and *permanently truncates* the file's
                // cached size to `offset + returned` — which makes playback die
                // after the first such read (everything past it returns EOF).
                // `read_range` fills the whole window except at the true end of
                // the file; if it ever comes up short anywhere else, surface a
                // (retryable) EIO rather than corrupting the kernel's view of
                // the file size.
                let want = (end - start + 1) as usize;
                if data.len() < want && end < self.file_size - 1 {
                    tracing::warn!(
                        target: "streaming",
                        info_hash = %self.info_hash,
                        file_index = self.file_index,
                        offset = start,
                        want,
                        got = data.len(),
                        "usenet read_range returned a mid-file short read; failing EIO to protect the FUSE size"
                    );
                    return ReadOutcome::Error(libc::EIO);
                }
                ReadOutcome::Data(data)
            }
            Err(error) => {
                tracing::warn!(
                    target: "streaming",
                    info_hash = %self.info_hash,
                    file_index = self.file_index,
                    offset = start,
                    error = %error,
                    "usenet read_range failed"
                );
                ReadOutcome::Error(libc::EIO)
            }
        }
    }
}

impl Drop for UsenetSession {
    fn drop(&mut self) {
        if self.registered {
            self.source.stream_unregister(&self.stream_key);
        }
    }
}
