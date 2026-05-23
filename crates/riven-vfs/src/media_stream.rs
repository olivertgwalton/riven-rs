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

/// Sequential reader over the usenet streamer's eagerly-pipelined byte
/// stream, consuming the decoded `Bytes` frames directly.
///
/// Unlike [`SequentialReader`] (the HTTP path) this holds **no**
/// `BufReader`/`StreamReader` and never copies through an intermediate
/// buffer: a read either splits the requested bytes out of the leftover
/// `carry` frame zero-copy (`Bytes::split_to`), or — when a read straddles
/// frame boundaries — concatenates the minimum number of frames into one
/// exactly-sized `BytesMut` (no zero-fill). The streamer already produces
/// zero-copy slices out of its decoded-segment cache, so this removes the
/// double buffering the old `FUSE → BufReader → Vec` path incurred.
struct UsenetFrameReader {
    stream: BoxStream<'static, Result<Bytes, io::Error>>,
    read_pos: u64,
    /// Full file size — the in-process stream has no HTTP window cap, so it
    /// can always serve up to EOF from `read_pos` onward.
    body_end_exclusive: u64,
    /// Bytes pulled from the stream but not yet consumed by a read.
    carry: Bytes,
}

impl UsenetFrameReader {
    fn new(
        stream: BoxStream<'static, Result<Bytes, io::Error>>,
        body_end_exclusive: u64,
        start_pos: u64,
    ) -> Self {
        Self {
            stream,
            read_pos: start_pos,
            body_end_exclusive,
            carry: Bytes::new(),
        }
    }

    fn can_serve(&self, start: u64, end_inclusive: u64) -> bool {
        start >= self.read_pos && end_inclusive < self.body_end_exclusive
    }

    /// Read up to `size` bytes at `pos`, tolerating an early EOF by returning
    /// the partial result (the last segment may decode a few bytes short).
    /// FUSE accepts short reads.
    async fn read_upto_at(&mut self, pos: u64, size: usize) -> io::Result<Bytes> {
        if pos < self.read_pos {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cannot rewind active stream from {} to {}", self.read_pos, pos),
            ));
        }
        if pos > self.read_pos {
            self.discard(pos - self.read_pos).await?;
            self.read_pos = pos;
        }

        // Fast path: the carry frame already covers the whole request — slice
        // it out with zero copy.
        if self.carry.len() >= size {
            let out = self.carry.split_to(size);
            self.read_pos += size as u64;
            return Ok(out);
        }

        // Straddles frames: concatenate the minimum span into one buffer.
        let mut buf = BytesMut::with_capacity(size);
        if !self.carry.is_empty() {
            let carry = std::mem::take(&mut self.carry);
            buf.extend_from_slice(&carry);
        }
        while buf.len() < size {
            match self.stream.next().await {
                Some(Ok(frame)) => {
                    let need = size - buf.len();
                    if frame.len() <= need {
                        buf.extend_from_slice(&frame);
                    } else {
                        buf.extend_from_slice(&frame[..need]);
                        self.carry = frame.slice(need..);
                    }
                }
                Some(Err(error)) => return Err(error),
                None => break, // EOF — return what we have
            }
        }
        self.read_pos += buf.len() as u64;
        Ok(buf.freeze())
    }

    /// Skip `bytes` of stream data (a forward seek), dropping whole frames and
    /// keeping any partial remainder in `carry`.
    async fn discard(&mut self, bytes: u64) -> io::Result<()> {
        let mut remaining = bytes;
        let from_carry = (self.carry.len() as u64).min(remaining);
        // `split_to` keeps the tail in `self.carry`, dropping the head.
        let _head = self.carry.split_to(from_carry as usize);
        remaining -= from_carry;

        while remaining > 0 {
            match self.stream.next().await {
                Some(Ok(frame)) => {
                    let flen = frame.len() as u64;
                    if flen <= remaining {
                        remaining -= flen;
                    } else {
                        self.carry = frame.slice(remaining as usize..);
                        remaining = 0;
                    }
                }
                Some(Err(error)) => return Err(error),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "stream ended during forward-seek discard",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// In-process streaming session for usenet-backed files.
///
/// Reads go directly into the usenet streamer via `LocalByteSource`.
/// Sequential reads pull from
/// an eagerly-pipelined `open_stream` (so a slow segment is absorbed by the
/// read-ahead buffer, not stalled on); a non-sequential seek reopens the
/// stream at the new position. There's no separate range cache here — the
/// streamer owns its own decoded-segment cache, so this avoids the
/// double-buffering the HTTP path incurred.
pub struct UsenetSession {
    source: Arc<dyn LocalByteSource>,
    info_hash: Arc<str>,
    file_index: usize,
    file_size: u64,
    sequential: Option<UsenetFrameReader>,
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
            sequential: None,
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

        // Reopen the pipeline on first read, on a backward seek, or on a
        // forward jump the current reader can't reach without discarding an
        // unreasonable amount (handled implicitly: can_serve only checks
        // start >= read_pos; large forward gaps still stream-and-discard,
        // which the kernel's sequential read-ahead rarely triggers).
        let need_open = self
            .sequential
            .as_ref()
            .is_none_or(|r| !r.can_serve(start, end));
        if need_open {
            let opened = runtime.block_on(self.source.open_stream(
                &self.info_hash,
                self.file_index,
                start,
            ));
            match opened {
                Ok(stream) => {
                    self.sequential =
                        Some(UsenetFrameReader::new(stream, self.file_size, start));
                }
                Err(error) => {
                    tracing::error!(
                        target: "streaming",
                        info_hash = %self.info_hash,
                        file_index = self.file_index,
                        error = %error,
                        "usenet open_stream failed"
                    );
                    return ReadOutcome::Error(libc::EIO);
                }
            }
        }

        let size = (end - start + 1) as usize;
        let reader = self
            .sequential
            .as_mut()
            .expect("sequential reader set above");
        match runtime.block_on(reader.read_upto_at(start, size)) {
            Ok(data) => ReadOutcome::Data(data),
            Err(error) => {
                // Drop the reader so the next read reopens cleanly.
                self.sequential = None;
                tracing::warn!(
                    target: "streaming",
                    info_hash = %self.info_hash,
                    file_index = self.file_index,
                    offset = start,
                    error = %error,
                    "usenet sequential read failed"
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
    use super::*;
    use futures::stream;

    /// Build a frame reader over a fixed sequence of `Bytes` frames.
    fn reader_from(frames: Vec<&[u8]>, file_size: u64, start: u64) -> UsenetFrameReader {
        let items: Vec<Result<Bytes, io::Error>> =
            frames.into_iter().map(|f| Ok(Bytes::copy_from_slice(f))).collect();
        UsenetFrameReader::new(stream::iter(items).boxed(), file_size, start)
    }

    #[tokio::test]
    async fn read_within_single_frame_is_zero_copy_carry() {
        // One 10-byte frame; two sub-frame reads should both succeed and the
        // second must come from the retained carry (no further stream pull).
        let mut r = reader_from(vec![b"0123456789"], 10, 0);
        let a = r.read_upto_at(0, 4).await.unwrap();
        assert_eq!(&a[..], b"0123");
        let b = r.read_upto_at(4, 6).await.unwrap();
        assert_eq!(&b[..], b"456789");
        assert_eq!(r.read_pos, 10);
    }

    #[tokio::test]
    async fn read_straddling_frames_concatenates() {
        let mut r = reader_from(vec![b"AAAA", b"BBBB", b"CCCC"], 12, 0);
        // Spans the first two frames plus part of the third.
        let out = r.read_upto_at(0, 10).await.unwrap();
        assert_eq!(&out[..], b"AAAABBBBCC");
        // Remaining two bytes come from the carry left over from frame 3.
        let rest = r.read_upto_at(10, 2).await.unwrap();
        assert_eq!(&rest[..], b"CC");
    }

    #[tokio::test]
    async fn read_tolerates_short_read_at_eof() {
        let mut r = reader_from(vec![b"AAAA"], 4, 0);
        // Ask for more than exists; should return only what's available.
        let out = r.read_upto_at(0, 16).await.unwrap();
        assert_eq!(&out[..], b"AAAA");
    }

    #[tokio::test]
    async fn forward_seek_discards_then_reads() {
        let mut r = reader_from(vec![b"AAAA", b"BBBB", b"CCCC"], 12, 0);
        // Seek to byte 6 (mid frame 2), then read 4 bytes spanning into frame 3.
        let out = r.read_upto_at(6, 4).await.unwrap();
        assert_eq!(&out[..], b"BBCC");
        assert_eq!(r.read_pos, 10);
    }

    #[tokio::test]
    async fn rewind_is_rejected() {
        let mut r = reader_from(vec![b"AAAA"], 4, 0);
        let _ = r.read_upto_at(0, 2).await.unwrap();
        assert!(r.read_upto_at(0, 2).await.is_err());
    }
}
