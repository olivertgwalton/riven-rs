use std::io;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use riven_core::local_source::LocalByteSource;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use crate::cache::RangeCache;
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
            .map_or(u64::MAX, |end_inclusive| end_inclusive.saturating_add(1));
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
                    for chunk in chunks {
                        cache.evict((self.ino, chunk.start, chunk.end));
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
            let Some(data) = cache.get((self.ino, chunk.start, chunk.end)) else {
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
            match ctx.cache.get((self.ino, chunk.start, chunk.end)) {
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
                            ctx.cache
                                .put((self.ino, chunk.start, chunk.end), data.clone());
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
            .all(|chunk| ctx.cache.get((self.ino, chunk.start, chunk.end)).is_some());
        if all_cached {
            return self.read_cached_chunks(start, end, chunks, ctx.cache);
        }

        for attempt in 0..2 {
            let mut failed = false;

            for chunk in chunks {
                if ctx.cache.get((self.ino, chunk.start, chunk.end)).is_some() {
                    continue;
                }

                if !self.ensure_sequential_reader_for(chunk.start, chunk.end, ctx) {
                    tracing::error!(ino = self.ino, "failed to start sequential reader");
                    return ReadOutcome::Error(libc::EIO);
                }

                match self.read_body_chunk(*chunk, ctx, attempt) {
                    Ok(data) => ctx.cache.put((self.ino, chunk.start, chunk.end), data),
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

/// Decide whether `UsenetSession::read` should spawn a prefetch task for
/// this call, and the `[from, want_through)` window to cover if so.
///
/// Kernel FUSE reads on the usenet mount arrive in much smaller chunks
/// (observed ~128 KiB) than the mount's 4 MiB `max_read` ceiling. Since
/// `want_through` tracks `start` almost 1:1, spawning a prefetch task on
/// every call that merely clears the frontier meant one tokio task (+ meta
/// lookup + pool acquire) per ~128 KiB of forward progress — hundreds of
/// tasks all contending for the same connection pool to each fetch a sliver
/// smaller than one NNTP segment. Only spawn once there's a real batch of
/// new ground to cover; small increments accumulate against the
/// still-unmoved frontier until a later call's `want_through` clears the
/// threshold (or until EOF, which must always be covered even if the final
/// remainder is small).
///
/// The coalescing threshold is capped to `lead`: otherwise a configured
/// lead smaller than `MIN_PREFETCH_SPAWN_BYTES` could never accumulate
/// enough span to cross a fixed 4 MiB threshold (the very first spawn's
/// span is exactly `lead`, so `frontier` never gets its initial bump and
/// `from` keeps re-anchoring to `start` on every call), silently disabling
/// prefetch entirely instead of just batching it more finely.
fn prefetch_decision(start: u64, lead: u64, file_size: u64, frontier: u64) -> Option<(u64, u64)> {
    const MIN_PREFETCH_SPAWN_BYTES: u64 = 4 * 1024 * 1024;
    let min_spawn_bytes = MIN_PREFETCH_SPAWN_BYTES.min(lead);
    let want_through = (start + lead).min(file_size - 1);
    if want_through <= frontier {
        return None;
    }
    let from = frontier.max(start);
    let span = want_through - from;
    let near_eof = want_through >= file_size - 1;
    if span >= min_spawn_bytes || near_eof {
        Some((from, want_through))
    } else {
        None
    }
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

        tracing::debug!(
            target: "streaming",
            info_hash = %self.info_hash,
            file_index = self.file_index,
            start,
            end,
            len = end - start + 1,
            prefetch_frontier = self.prefetch_frontier,
            "usenet read() call"
        );

        const DEFAULT_PREFETCH_LEAD: u64 = 46 * 1024 * 1024;
        let lead = std::env::var("RIVEN_USENET_STREAM_READAHEAD_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_PREFETCH_LEAD);
        if let Some((from, want_through)) =
            prefetch_decision(start, lead, self.file_size, self.prefetch_frontier)
        {
            self.prefetch_frontier = want_through;
            tracing::debug!(
                target: "streaming",
                info_hash = %self.info_hash,
                file_index = self.file_index,
                from,
                want_through,
                span = want_through - from,
                "usenet prefetch spawned"
            );
            let source = Arc::clone(&self.source);
            let info_hash = Arc::clone(&self.info_hash);
            let file_index = self.file_index;
            runtime.spawn(async move {
                source
                    .prefetch(&info_hash, file_index, from, want_through)
                    .await;
            });
        }

        match runtime.block_on(
            self.source
                .read_range(&self.info_hash, self.file_index, start, end),
        ) {
            Ok(data) => {
                // A mid-file short read must never reach the kernel: the Linux
                // FUSE client treats short reads as EOF and permanently truncates
                // the cached file size to `offset + returned`, killing playback.
                // Surface a retryable EIO instead of forwarding it.
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use riven_core::local_source::LocalByteSource;

    use super::*;

    /// Records every `prefetch`/`read_range` call instead of doing real I/O,
    /// so tests can assert on call counts and ranges without a network.
    struct MockSource {
        prefetch_calls: Mutex<Vec<(u64, u64)>>,
        read_range_calls: Mutex<u32>,
    }

    impl MockSource {
        fn new() -> Self {
            Self {
                prefetch_calls: Mutex::new(Vec::new()),
                read_range_calls: Mutex::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl LocalByteSource for MockSource {
        async fn read_range(
            &self,
            _info_hash: &str,
            _file_index: usize,
            start: u64,
            end_inclusive: u64,
        ) -> anyhow::Result<Bytes> {
            *self.read_range_calls.lock().unwrap() += 1;
            let len = (end_inclusive - start + 1) as usize;
            Ok(Bytes::from(vec![0u8; len]))
        }

        async fn prefetch(
            &self,
            _info_hash: &str,
            _file_index: usize,
            start: u64,
            end_inclusive: u64,
        ) {
            self.prefetch_calls
                .lock()
                .unwrap()
                .push((start, end_inclusive));
        }

        fn stream_register(&self, _key: &str, _info_hash: &str, _filename: &str, _file_size: u64) {}
        fn stream_touch(&self, _key: &str) {}
        fn stream_unregister(&self, _key: &str) {}
    }

    /// Regression test for the prefetch-spawn-storm bug: real kernel FUSE
    /// reads on the usenet mount arrive in ~128 KiB increments (far smaller
    /// than the 4 MiB `max_read` ceiling), and the pre-fix code spawned a
    /// prefetch task on almost every one of them — hundreds of tiny tasks
    /// all contending for the same NNTP connection pool per 100MB read.
    /// This drives many small sequential reads and asserts prefetch spawns
    /// stay coalesced into large batches instead of firing on every read.
    #[test]
    fn small_sequential_reads_coalesce_prefetch_spawns() {
        const FILE_SIZE: u64 = 200 * 1024 * 1024;
        const READ_SIZE: u64 = 128 * 1024;
        const NUM_READS: u64 = 320; // 40 MiB of forward progress

        let rt = tokio::runtime::Runtime::new().unwrap();
        let mock = Arc::new(MockSource::new());
        let source: Arc<dyn LocalByteSource> = mock.clone();
        let mut session = UsenetSession::new(
            source,
            Arc::from("test-hash"),
            0,
            FILE_SIZE,
            Arc::from("test.mkv"),
        );

        for i in 0..NUM_READS {
            let start = i * READ_SIZE;
            let end = start + READ_SIZE - 1;
            match session.read(start, end, rt.handle()) {
                ReadOutcome::Data(data) => assert_eq!(data.len(), READ_SIZE as usize),
                ReadOutcome::Error(errno) => panic!("unexpected read error: {errno}"),
            }
        }

        // Let any in-flight fire-and-forget prefetch tasks finish.
        rt.block_on(async { tokio::time::sleep(std::time::Duration::from_millis(200)).await });

        assert_eq!(*mock.read_range_calls.lock().unwrap(), NUM_READS as u32);

        // Sort by `from` before checking count/contiguity: prefetch spawns
        // are independent fire-and-forget tokio tasks, so the order they
        // land in `prefetch_calls` reflects task-completion order, not the
        // chronological order they were spawned in — asserting on raw
        // insertion order would be flaky under real scheduling.
        let mut prefetch_calls = mock.prefetch_calls.lock().unwrap().clone();
        prefetch_calls.sort_unstable();

        // A zero-spawn regression (e.g. the coalescing threshold silently
        // disabling prefetch entirely) must fail this test, not slip through
        // an upper-bound-only check.
        assert!(
            !prefetch_calls.is_empty(),
            "expected at least one prefetch spawn for {NUM_READS} forward reads, got none"
        );
        // 40 MiB of forward progress in 4 MiB coalescing increments (plus the
        // initial big jump to `start + lead`) is a small, bounded number of
        // spawns — nowhere near one per 128 KiB read (which would be 320).
        assert!(
            prefetch_calls.len() < 20,
            "expected prefetch spawns to be coalesced into a handful of batches, got {} for {NUM_READS} reads",
            prefetch_calls.len()
        );

        // Coverage must be contiguous and monotonic: each spawned window
        // should start where the previous one left off (or at the read
        // position, for the very first spawn), with no gaps and no overlap.
        let mut prev_end: Option<u64> = None;
        for &(from, want_through) in prefetch_calls.iter() {
            assert!(want_through > from, "spawned window must be non-empty");
            if let Some(prev) = prev_end {
                assert_eq!(
                    from, prev,
                    "prefetch windows must be contiguous, no gaps/overlap"
                );
            }
            prev_end = Some(want_through);
        }
    }

    /// Regression tests for the `prefetch_decision` coalescing logic in
    /// isolation — pure function, no async/mocking needed.
    mod prefetch_decision_tests {
        use super::super::prefetch_decision;

        const FILE_SIZE: u64 = 200 * 1024 * 1024;

        #[test]
        fn large_lead_spawns_immediately_and_advances_frontier() {
            let decision = prefetch_decision(0, 46 * 1024 * 1024, FILE_SIZE, 0);
            assert_eq!(decision, Some((0, 46 * 1024 * 1024)));
        }

        #[test]
        fn small_increment_below_threshold_does_not_spawn() {
            // Frontier already established well ahead of `start`; a small
            // forward step shouldn't cross the 4 MiB coalescing threshold.
            let frontier = 46 * 1024 * 1024;
            let decision = prefetch_decision(128 * 1024, 46 * 1024 * 1024, FILE_SIZE, frontier);
            assert_eq!(decision, None);
        }

        /// Regression test: a configured lead smaller than the 4 MiB
        /// coalescing threshold must still spawn prefetch — not be silently
        /// disabled by a fixed threshold the lead itself can never cross.
        #[test]
        fn lead_smaller_than_threshold_still_spawns_on_first_read() {
            const SMALL_LEAD: u64 = 1024 * 1024; // 1 MiB, below MIN_PREFETCH_SPAWN_BYTES
            let decision = prefetch_decision(0, SMALL_LEAD, FILE_SIZE, 0);
            assert_eq!(
                decision,
                Some((0, SMALL_LEAD)),
                "a lead below the coalescing threshold must still trigger prefetch"
            );
        }

        /// Once the small-lead frontier is established, subsequent small
        /// forward reads must keep accumulating span against it (not
        /// re-anchor `from` to `start` and get stuck never spawning again).
        #[test]
        fn lead_smaller_than_threshold_keeps_accumulating_after_first_spawn() {
            const SMALL_LEAD: u64 = 1024 * 1024;
            let mut frontier = 0u64;
            let first = prefetch_decision(0, SMALL_LEAD, FILE_SIZE, frontier);
            frontier = first.expect("first read must spawn").1;

            // Advance in tiny steps; span accumulates against `frontier`
            // (fixed) rather than resetting to `lead` on every call.
            let initial_frontier = frontier;
            let mut second_spawn: Option<(u64, u64)> = None;
            let mut start = 0u64;
            for _ in 0..64 {
                start += 128 * 1024;
                if let Some(decision) = prefetch_decision(start, SMALL_LEAD, FILE_SIZE, frontier) {
                    second_spawn = Some(decision);
                    break;
                }
            }
            let (from, want_through) =
                second_spawn.expect("prefetch must eventually spawn again as reads advance");
            assert_eq!(
                from, initial_frontier,
                "the next spawn's window must pick up exactly where the first left off"
            );
            assert!(want_through > initial_frontier, "frontier must advance");
        }

        #[test]
        fn near_eof_spawns_regardless_of_span() {
            // frontier sits 1 MiB before `start`, well short of the 4 MiB
            // threshold, but a large lead pins `want_through` at file_size-1
            // (clamped) — this isolates near_eof as the only reason to spawn.
            let frontier = FILE_SIZE - 2 * 1024 * 1024;
            let start = FILE_SIZE - 1024 * 1024;
            let decision = prefetch_decision(start, 46 * 1024 * 1024, FILE_SIZE, frontier);
            let (_, want_through) = decision.expect("must always cover the tail up to EOF");
            assert_eq!(want_through, FILE_SIZE - 1);
        }

        #[test]
        fn frontier_already_covering_want_through_does_not_spawn() {
            let decision = prefetch_decision(0, 46 * 1024 * 1024, FILE_SIZE, 46 * 1024 * 1024);
            assert_eq!(decision, None);
        }
    }
}
