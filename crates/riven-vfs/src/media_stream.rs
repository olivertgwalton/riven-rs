use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use riven_core::local_source::LocalByteSource;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use crate::cache::RangeCache;
use crate::chunks::{ChunkRange, FileLayout};
use crate::detect::{ReadType, detect_read_type};
use crate::stream::{fetch_range, open_stream, response_body_end};
use riven_core::config::vfs::CHUNK_TIMEOUT_SECS;

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
        // `open_stream` waits for response headers. Debrid origins can stall
        // before sending them, so the body-read timeout below would never
        // start and playback would sit on reqwest's 60-second request timeout.
        // Keep stream establishment on the same short recovery path as each
        // body chunk; a failure bubbles up to the VFS URL-refresh retry.
        let response = match runtime.block_on(tokio::time::timeout(
            Duration::from_secs(CHUNK_TIMEOUT_SECS),
            open_stream(&client, &url, start_pos),
        )) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                tracing::debug!(error = %error, start_pos, "failed to open sequential stream");
                return None;
            }
            Err(_) => {
                tracing::debug!(
                    start_pos,
                    timeout_secs = CHUNK_TIMEOUT_SECS,
                    "sequential stream open timed out"
                );
                return None;
            }
        };
        let body_end_exclusive = response_body_end(&response)
            .map_or(u64::MAX, |end_inclusive| end_inclusive.saturating_add(1));
        let stream = response.bytes_stream().map_err(io::Error::other).boxed();
        let reader = BufReader::with_capacity(
            riven_core::config::vfs::CHUNK_SIZE as usize,
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
        sequential_request_is_contiguous(
            self.read_pos,
            self.body_end_exclusive,
            start,
            end_inclusive,
        )
    }

    fn read_range(
        &mut self,
        start: u64,
        size: usize,
        runtime: &tokio::runtime::Handle,
    ) -> io::Result<Bytes> {
        // A debrid CDN can leave an open response stalled until reqwest's
        // request-wide timeout expires. Bound each 1 MiB playback chunk so a
        // dead body is retried/refreshed quickly instead of making the player
        // buffer for a minute. This matches the TypeScript VFS chunk timeout.
        runtime
            .block_on(tokio::time::timeout(
                Duration::from_secs(CHUNK_TIMEOUT_SECS),
                self.read_exact_at(start, size),
            ))
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("stream chunk did not complete within {CHUNK_TIMEOUT_SECS}s"),
                )
            })?
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

fn sequential_request_is_contiguous(
    read_pos: u64,
    body_end_exclusive: u64,
    start: u64,
    end_inclusive: u64,
) -> bool {
    start == read_pos && end_inclusive < body_end_exclusive
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
            let key = (self.ino, chunk.start, chunk.end);
            let _miss_guard = match ctx.runtime.block_on(tokio::time::timeout(
                Duration::from_secs(CHUNK_TIMEOUT_SECS),
                ctx.cache.lock_miss(key),
            )) {
                Ok(guard) => guard,
                Err(_) => {
                    tracing::warn!(
                        ino = self.ino,
                        start = chunk.start,
                        end = chunk.end,
                        "timed out waiting for another reader to populate scan chunk"
                    );
                    return ReadOutcome::Error(libc::ETIMEDOUT);
                }
            };

            // A different fd may have populated this range while we waited.
            match ctx.cache.get(key) {
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
                            ctx.cache.put(key, data.clone());
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

        // Match the established TypeScript VFS behaviour: fetch and cache a
        // complete 1 MiB body chunk before satisfying its first small FUSE
        // read. This turns the following 128 KiB kernel reads into cache hits
        // and gives the player a meaningful read-ahead buffer instead of
        // making every individual read wait on the CDN.
        for chunk in chunks {
            let key = (self.ino, chunk.start, chunk.end);
            if ctx.cache.get(key).is_some() {
                continue;
            }

            let _miss_guard = match ctx.runtime.block_on(tokio::time::timeout(
                Duration::from_secs(CHUNK_TIMEOUT_SECS),
                ctx.cache.lock_miss(key),
            )) {
                Ok(guard) => guard,
                Err(_) => {
                    tracing::warn!(
                        ino = self.ino,
                        start = chunk.start,
                        end = chunk.end,
                        "timed out waiting for another reader to populate body chunk"
                    );
                    return ReadOutcome::Error(libc::ETIMEDOUT);
                }
            };

            // Equivalent to riven-ts `waitForChunk`: if another fd completed
            // this chunk while we waited, use the shared cache. This fd's HTTP
            // cursor stays where it was and will reconnect on the next miss.
            if ctx.cache.get(key).is_some() {
                continue;
            }

            let mut fetched = None;
            for attempt in 0..2 {
                if !self.ensure_sequential_reader_for(chunk.start, chunk.end, ctx) {
                    tracing::error!(ino = self.ino, "failed to start sequential reader");
                    return ReadOutcome::Error(libc::EIO);
                }

                match self
                    .sequential
                    .as_mut()
                    .expect("sequential reader must exist after ensure_sequential_reader")
                    .read_range(chunk.start, chunk.len(), ctx.runtime)
                {
                    Ok(data) => {
                        fetched = Some(data);
                        break;
                    }
                    Err(error) if attempt == 0 => {
                        tracing::warn!(ino = self.ino, error = %error, "stream chunk read failed, retrying once");
                        self.sequential = None;
                    }
                    Err(error) => {
                        tracing::error!(ino = self.ino, error = %error, "stream chunk read failed after retry");
                        self.sequential = None;
                        return ReadOutcome::Error(libc::EIO);
                    }
                }
            }

            let data = fetched.expect("successful chunk read must set data");
            if data.len() != chunk.len() {
                tracing::error!(
                    ino = self.ino,
                    want = chunk.len(),
                    got = data.len(),
                    "stream returned a short body chunk"
                );
                return ReadOutcome::Error(libc::EIO);
            }
            ctx.cache.put(key, data);
        }

        self.read_cached_chunks(start, end, chunks, ctx.cache)
    }

    fn ensure_sequential_reader_for(
        &mut self,
        start: u64,
        end_inclusive: u64,
        ctx: &ReadContext<'_>,
    ) -> bool {
        // An open CDN response is forward-only. Reconnect on *any*
        // discontinuity rather than discarding bytes to catch up: FUSE issues
        // concurrent readahead requests that can arrive out of order, and
        // draining a large forward gap makes the later lower-offset request
        // reopen again. This mirrors the TypeScript VFS seek handling.
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
/// A single producer maintains an ordered window of NZB segments ahead of the
/// latest read offset. Kernel-sized reads only update a watch value; they do
/// not create detached tasks or assign request priorities. The source owns
/// provider scheduling and skips segments already cached or in flight.
pub struct UsenetSession {
    source: Arc<dyn LocalByteSource>,
    info_hash: Arc<str>,
    file_index: usize,
    file_size: u64,
    /// One bounded producer per open file, rather than a new detached task
    /// for every read that advances the frontier. The producer only warms
    /// Riven's decoded-segment cache; it never changes the bytes returned to
    /// the FUSE caller.
    prefetch_target_tx: Option<tokio::sync::watch::Sender<Option<u64>>>,
    prefetch_task: Option<tokio::task::JoinHandle<()>>,
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
            prefetch_target_tx: None,
            prefetch_task: None,
            stream_key,
            filename,
            registered: false,
            reads_since_touch: 0,
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    fn ensure_prefetch_worker(&mut self, runtime: &tokio::runtime::Handle) {
        if self.prefetch_target_tx.is_some() {
            return;
        }

        // A watch channel retains only the latest requested watermark. If
        // NNTP is slower than the consumer, obsolete intermediate windows
        // are discarded instead of building an unbounded work queue.
        let (tx, mut targets) = tokio::sync::watch::channel(None::<u64>);
        let source = Arc::clone(&self.source);
        let info_hash = Arc::clone(&self.info_hash);
        let file_index = self.file_index;

        self.prefetch_task = Some(runtime.spawn(async move {
            while targets.changed().await.is_ok() {
                let Some(start) = *targets.borrow_and_update() else {
                    continue;
                };
                const DEFAULT_SEGMENT_WINDOW: usize = 60;
                let segment_window = std::env::var("RIVEN_USENET_PLAYBACK_SEGMENT_WINDOW")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .filter(|value| *value > 0)
                    .unwrap_or(DEFAULT_SEGMENT_WINDOW);
                source
                    .prefetch(&info_hash, file_index, start, segment_window)
                    .await;
            }
        }));
        self.prefetch_target_tx = Some(tx);
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
            "usenet read() call"
        );

        self.ensure_prefetch_worker(runtime);
        if let Some(tx) = &self.prefetch_target_tx {
            tx.send_replace(Some(start));
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
        // Closing the sender makes the worker exit after its current cache
        // window. Do not abort it: cancelling an in-flight NNTP BODY drops
        // the socket instead of returning it to the pool, causing the next
        // playback range to pay for a fresh TLS dial.
        self.prefetch_target_tx.take();
        self.prefetch_task.take();
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

    #[test]
    fn sequential_reader_reconnects_on_any_discontinuity() {
        let read_pos = 10 * 1024 * 1024;
        let body_end = read_pos + 32 * 1024 * 1024;
        let chunk_end = read_pos + riven_core::config::vfs::CHUNK_SIZE - 1;

        assert!(sequential_request_is_contiguous(
            read_pos, body_end, read_pos, chunk_end
        ));
        assert!(
            !sequential_request_is_contiguous(read_pos, body_end, read_pos + 1, chunk_end),
            "a forward gap must reconnect instead of being drained"
        );
        assert!(
            !sequential_request_is_contiguous(read_pos, body_end, read_pos - 1, chunk_end),
            "a backward read must reconnect"
        );
        assert!(
            !sequential_request_is_contiguous(read_pos, chunk_end, read_pos, chunk_end),
            "a request extending beyond the CDN response window must reconnect"
        );
    }

    /// Records every `prefetch`/`read_range` call instead of doing real I/O,
    /// so tests can assert on call counts and ranges without a network.
    struct MockSource {
        prefetch_calls: Mutex<Vec<(u64, usize)>>,
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
            segments_ahead: usize,
        ) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.prefetch_calls
                .lock()
                .unwrap()
                .push((start, segments_ahead));
        }

        fn stream_register(&self, _key: &str, _info_hash: &str, _filename: &str, _file_size: u64) {}
        fn stream_touch(&self, _key: &str) {}
        fn stream_unregister(&self, _key: &str) {}
    }

    /// Kernel-sized reads update one producer's latest offset rather than
    /// spawning one prefetch task per read.
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

        let prefetch_calls = mock.prefetch_calls.lock().unwrap().clone();

        // A zero-spawn regression (e.g. the coalescing threshold silently
        // disabling prefetch entirely) must fail this test, not slip through
        // an upper-bound-only check.
        assert!(
            !prefetch_calls.is_empty(),
            "expected the playback window producer to run"
        );
        assert!(
            prefetch_calls.len() < 20,
            "expected one coalescing producer, got {} windows for {NUM_READS} reads",
            prefetch_calls.len()
        );
        for &(_, segments) in &prefetch_calls {
            assert_eq!(segments, 60);
        }
        assert!(
            prefetch_calls.last().unwrap().0 >= (NUM_READS - 2) * READ_SIZE,
            "the coalesced producer must converge on the latest read offset"
        );
    }

    #[test]
    fn resumed_read_prefetch_starts_at_the_resume_offset() {
        const FILE_SIZE: u64 = 200 * 1024 * 1024;
        const RESUME_AT: u64 = 120 * 1024 * 1024;
        const READ_SIZE: u64 = 128 * 1024;

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

        assert!(matches!(
            session.read(RESUME_AT, RESUME_AT + READ_SIZE - 1, rt.handle()),
            ReadOutcome::Data(_)
        ));
        rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), async {
                while mock.prefetch_calls.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("prefetch worker should observe its initial target");
        });

        let calls = mock.prefetch_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1, "expected exactly one initial cache window");
        assert_eq!(
            calls[0].0, RESUME_AT,
            "a resumed stream must start its segment window at the resume offset"
        );
        assert_eq!(calls[0].1, 60);
    }
}
