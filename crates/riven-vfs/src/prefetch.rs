//! Adaptive sequential read-ahead, shared by every backend.
//!
//! Modelled on awslabs/mountpoint-s3's prefetcher. A player issues small
//! (128 KiB) FUSE reads; fetching each one on demand caps throughput at
//! `read_size / origin_latency` no matter how fast the origin is. Instead a
//! background task pulls large sequential chunks ahead of the reader, so a
//! FUSE read is normally a memcpy out of a ready buffer.
//!
//! The window is *adaptive*, which is the part a fixed buffer gets wrong:
//!
//! - It starts small ([`INITIAL_WINDOW`]) so seeking and scrubbing do not pay
//!   for read-ahead they will throw away.
//! - Every time the reader consumes sequentially, it doubles, up to
//!   `max_window`. A long sequential play therefore ends up with a deep
//!   buffer that absorbs a slow origin; a random-access workload never
//!   builds one.
//! - Any seek outside the buffered region resets it.
//!
//! The 128 KiB added to the initial window is deliberate, and the same trick
//! mountpoint uses: Linux issues an extra 128 KiB readahead on top of a 1 MiB
//! read, and FUSE cannot distinguish it from a real read, so covering it up
//! front avoids a second round trip.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::sync::{Mutex, Notify};

use crate::source::ByteSource;

/// First window: 1 MiB plus Linux's 128 KiB readahead overshoot.
const INITIAL_WINDOW: u64 = 1024 * 1024 + 128 * 1024;
/// Window growth per sustained sequential read.
const WINDOW_MULTIPLIER: u64 = 2;
/// Kernel block size — the unit FUSE reads arrive in.
const BLOCK: u64 = 128 * 1024;
/// One fetch.
///
/// riven-ts uses 1 MiB here, but it streams from a low-latency CDN where a
/// small chunk still saturates the link. This origin is usenet: ~700 KiB
/// articles behind multi-second latency, fetched in parallel *within* a
/// range. A 1 MiB chunk is barely one article, so it would leave most of the
/// connection pool idle; 8 MiB is ~11 articles, which keeps it busy.
const CHUNK: u64 = 8 * 1024 * 1024;
/// Reads landing within this distance ahead of the buffer are still
/// sequential rather than a seek. riven-ts's scan tolerance: 25 blocks.
const SEQUENTIAL_TOLERANCE: u64 = 25 * BLOCK;
/// Header players probe for container metadata (riven-ts: 256 KiB).
const HEADER_SIZE: u64 = 256 * 1024;
/// Footer bounds — MP4 keeps its `moov` atom at the end, so players read the
/// tail before playing. riven-ts: 2% of the file, clamped to 16 KiB..10 MiB.
const MIN_FOOTER: u64 = 16 * 1024;
const MAX_FOOTER: u64 = 10 * 1024 * 1024;
const FOOTER_PERCENT: f64 = 0.02;
/// Chunks fetched concurrently. Without this the prefetcher alternates
/// fetch/serve with no overlap, capping throughput at `CHUNK / latency` and
/// passing every latency spike straight through to the player.
const MAX_INFLIGHT_CHUNKS: usize = 3;

struct Buffered {
    /// File offset of `data`'s first byte.
    start: u64,
    data: VecDeque<Bytes>,
    /// Total bytes across `data`.
    len: u64,
}

impl Buffered {
    fn end(&self) -> u64 {
        self.start + self.len
    }

    fn clear(&mut self, start: u64) {
        self.data.clear();
        self.len = 0;
        self.start = start;
    }

    /// Drop everything before `offset`.
    fn advance_to(&mut self, offset: u64) {
        while self.start < offset {
            let Some(front) = self.data.front_mut() else {
                self.start = offset;
                self.len = 0;
                return;
            };
            let skip = (offset - self.start).min(front.len() as u64);
            if skip as usize == front.len() {
                self.data.pop_front();
            } else {
                *front = front.slice(skip as usize..);
            }
            self.start += skip;
            self.len -= skip;
        }
    }

    /// Copy up to `want` bytes from `offset`, which must be within the buffer.
    fn read_at(&mut self, offset: u64, want: usize) -> Bytes {
        self.advance_to(offset);
        let take = (want as u64).min(self.len) as usize;
        let mut out = BytesMut::with_capacity(take);
        let mut remaining = take;
        for chunk in &self.data {
            if remaining == 0 {
                break;
            }
            let n = remaining.min(chunk.len());
            out.extend_from_slice(&chunk[..n]);
            remaining -= n;
        }
        out.freeze()
    }
}

struct Inner {
    buf: Buffered,
    /// Next byte to *dispatch*. Runs ahead of the buffer by whatever is in
    /// flight, so concurrent fetches never request the same range twice.
    frontier: u64,
    /// Current read-ahead depth, doubling while reads stay sequential.
    window: u64,
    /// Set when the origin fails, so waiters stop rather than hang.
    error: Option<String>,
    inflight: usize,
    /// Chunks that arrived out of order, keyed by start offset, waiting to be
    /// appended once the gap before them is filled.
    pending: BTreeMap<u64, Bytes>,
    /// Container metadata, pinned for the life of the handle. Players probe
    /// these repeatedly and out of order; serving them from the sequential
    /// window would reset it on every probe and refetch them each time.
    header: Option<Bytes>,
    footer: Option<Bytes>,
}

impl Inner {
    /// Move completed chunks into the contiguous buffer, in order.
    fn drain_pending(&mut self) {
        while let Some(data) = self.pending.remove(&self.buf.end()) {
            let got = data.len() as u64;
            if self.buf.len == 0 {
                self.buf.start = self.buf.end();
            }
            self.buf.data.push_back(data);
            self.buf.len += got;
        }
    }

    /// Abandon everything dispatched past `valid_end` — used when a chunk
    /// comes back short, which shifts every later boundary.
    fn rewind_to(&mut self, valid_end: u64) {
        self.pending.retain(|start, _| *start < valid_end);
        self.frontier = valid_end;
    }
}

/// Per-open-file adaptive read-ahead over a [`ByteSource`].
pub struct Prefetcher {
    source: Arc<dyn ByteSource>,
    inner: Mutex<Inner>,
    progress: Notify,
    size: u64,
    max_window: u64,
}

impl Prefetcher {
    pub fn new(source: Arc<dyn ByteSource>, max_window: u64) -> Self {
        let size = source.size();
        Self {
            source,
            inner: Mutex::new(Inner {
                buf: Buffered {
                    start: 0,
                    data: VecDeque::new(),
                    len: 0,
                },
                frontier: 0,
                window: INITIAL_WINDOW,
                error: None,
                inflight: 0,
                pending: BTreeMap::new(),
                header: None,
                footer: None,
            }),
            progress: Notify::new(),
            size,
            max_window: max_window.max(INITIAL_WINDOW),
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// First byte of the footer region.
    fn footer_start(&self) -> u64 {
        #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
        let target = (self.size as f64 * FOOTER_PERCENT) as u64;
        self.size
            .saturating_sub(target.clamp(MIN_FOOTER, MAX_FOOTER))
    }

    /// Fetch `[start, end]` in full, looping over short reads.
    ///
    /// Origins cap their own windows, so one call may return less than asked.
    /// The pinned regions must be whole before they are sliced, or a probe
    /// would be served short — which the kernel reads as EOF.
    async fn read_exact_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
        let want = (end - start + 1) as usize;
        let mut out = BytesMut::with_capacity(want);
        let mut pos = start;
        while pos <= end {
            let data = self.source.read_range(pos, end).await?;
            if data.is_empty() {
                break;
            }
            pos += data.len() as u64;
            out.extend_from_slice(&data);
        }
        Ok(out.freeze())
    }

    /// Serve a metadata probe without touching the sequential window.
    ///
    /// Fetched once and pinned: a player re-reads the header and footer many
    /// times while seeking, and routing those through the streaming window
    /// would reset it to the shallow depth on every probe.
    async fn read_pinned(&self, start: u64, want: usize, is_header: bool) -> io::Result<Bytes> {
        let (region_start, region_end) = if is_header {
            (0, HEADER_SIZE.min(self.size) - 1)
        } else {
            (self.footer_start(), self.size - 1)
        };

        {
            let inner = self.inner.lock().await;
            let cached = if is_header { &inner.header } else { &inner.footer };
            if let Some(data) = cached {
                return Ok(slice_at(data, region_start, start, want));
            }
        }

        let data = self.read_exact_range(region_start, region_end).await?;
        let mut inner = self.inner.lock().await;
        if is_header {
            inner.header = Some(data.clone());
        } else {
            inner.footer = Some(data.clone());
        }
        Ok(slice_at(&data, region_start, start, want))
    }

    /// Serve `[start, start+len)`, fetching only what is not already buffered.
    ///
    /// Returns a short read only at EOF: a mid-file short read must never
    /// reach the kernel, because Linux's FUSE client treats it as EOF and
    /// permanently truncates the cached file size, killing playback.
    pub async fn read(self: &Arc<Self>, start: u64, len: usize) -> io::Result<Bytes> {
        if start >= self.size {
            return Ok(Bytes::new());
        }
        let want = (len as u64).min(self.size - start) as usize;

        // Metadata probes bypass the window entirely.
        let end = start + want as u64 - 1;
        if end < HEADER_SIZE.min(self.size) {
            return self.read_pinned(start, want, true).await;
        }
        if start >= self.footer_start() {
            return self.read_pinned(start, want, false).await;
        }

        self.source.report_position(start).await;

        loop {
            // Registered before the buffer is inspected, so a fill completing
            // between the check and the await cannot be missed.
            let notified = self.progress.notified();

            {
                let mut inner = self.inner.lock().await;

                if let Some(error) = inner.error.take() {
                    return Err(io::Error::other(error));
                }

                // Seek: outside the buffer and past the tolerance window means
                // restart read-ahead, back at the shallow initial depth so
                // scrubbing does not pay for a deep window it will discard.
                let in_buf = start >= inner.buf.start && start < inner.buf.end();
                let near = start >= inner.buf.end()
                    && start - inner.buf.end() <= SEQUENTIAL_TOLERANCE;
                if !in_buf && !near {
                    inner.buf.clear(start);
                    inner.pending.clear();
                    inner.frontier = start;
                    inner.window = INITIAL_WINDOW;
                }

                if in_buf {
                    let available = inner.buf.end() - start;
                    // Serve when the full request is buffered, or at EOF where
                    // a short read is legitimate.
                    if available >= want as u64 || inner.buf.end() >= self.size {
                        let data = inner.buf.read_at(start, want);
                        // Sustained sequential reading earns a deeper window.
                        inner.window =
                            (inner.window * WINDOW_MULTIPLIER).min(self.max_window);
                        drop(inner);
                        self.dispatch_fills();
                        return Ok(data);
                    }
                }
            }

            // Keep the pipeline topped up, then wait for whichever chunk lands
            // next. Fills run as their own tasks, so fetching overlaps serving.
            if !self.dispatch_fills() {
                notified.await;
            } else {
                notified.await;
            }
        }
    }

    /// Start fills until the window is covered or the in-flight cap is hit.
    /// Returns whether anything was dispatched.
    fn dispatch_fills(self: &Arc<Self>) -> bool {
        let mut started = false;
        loop {
            let Ok(mut inner) = self.inner.try_lock() else {
                return started;
            };
            if inner.error.is_some()
                || inner.inflight >= MAX_INFLIGHT_CHUNKS
                || inner.frontier >= self.size
            {
                return started;
            }
            // Depth is measured from the buffer, so in-flight chunks count
            // toward the window and it cannot be over-committed.
            let ahead = inner.frontier.saturating_sub(inner.buf.start);
            if ahead >= inner.window {
                return started;
            }

            let from = inner.frontier;
            // Align the end to a CHUNK boundary so every pass over a file
            // splits segments at the same offsets, letting the origin's cache
            // serve the half it already has instead of refetching it.
            let aligned_end = from.next_multiple_of(CHUNK).max(from + 1);
            let to = (aligned_end - 1).min(self.size - 1);
            inner.frontier = to + 1;
            inner.inflight += 1;
            drop(inner);

            let this = Arc::clone(self);
            tokio::spawn(async move { this.fill(from, to).await });
            started = true;
        }
    }

    /// Fetch one chunk, slot it into place, and wake any waiters.
    async fn fill(&self, from: u64, to: u64) {
        let result = self.source.read_range(from, to).await;

        {
            let mut inner = self.inner.lock().await;
            inner.inflight -= 1;
            match result {
                Ok(data) if !data.is_empty() => {
                    let got = data.len() as u64;
                    let short = got < to - from + 1;
                    if from >= inner.buf.end() {
                        inner.pending.insert(from, data);
                        inner.drain_pending();
                    }
                    // A short chunk shifts every later boundary, so discard
                    // what was dispatched past it and re-dispatch from here.
                    if short {
                        inner.rewind_to(from + got);
                    }
                }
                Ok(_) => inner.error = Some("origin returned an empty range".into()),
                Err(error) => inner.error = Some(error.to_string()),
            }
        }
        self.progress.notify_waiters();
    }
}

/// Copy `want` bytes at absolute `start` out of a region beginning at
/// `region_start`. Short only where the region ends.
fn slice_at(data: &Bytes, region_start: u64, start: u64, want: usize) -> Bytes {
    let offset = (start - region_start) as usize;
    if offset >= data.len() {
        return Bytes::new();
    }
    data.slice(offset..(offset + want).min(data.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serves ranges but truncates every response, mimicking an origin that
    /// caps its own window (usenet segment boundaries, HTTP range caps).
    struct ShortSource {
        size: u64,
        cap: usize,
    }

    #[async_trait::async_trait]
    impl ByteSource for ShortSource {
        async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
            let want = (end - start + 1) as usize;
            Ok(Bytes::from(vec![b'x'; want.min(self.cap)]))
        }
        fn size(&self) -> u64 {
            self.size
        }
    }

    fn prefetcher(size: u64, cap: usize) -> Arc<Prefetcher> {
        Arc::new(Prefetcher::new(
            Arc::new(ShortSource { size, cap }),
            64 * 1024 * 1024,
        ))
    }

    #[tokio::test]
    async fn never_returns_a_mid_file_short_read() {
        // The origin only ever yields 4 KiB per call, far under the request.
        // A short read reaching the kernel makes Linux treat it as EOF and
        // permanently truncate the cached file size, which kills playback —
        // so the prefetcher must keep fetching until the request is whole.
        let p = prefetcher(8 * 1024 * 1024, 4096);
        let data = p.read(0, 128 * 1024).await.unwrap();
        assert_eq!(data.len(), 128 * 1024, "mid-file read must be complete");
    }

    #[tokio::test]
    async fn short_read_is_allowed_only_at_eof() {
        let size = 100 * 1024;
        let p = prefetcher(size, 4096);
        let data = p.read(size - 1000, 128 * 1024).await.unwrap();
        assert_eq!(data.len(), 1000, "EOF read is legitimately short");
        assert!(p.read(size, 4096).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn metadata_probes_do_not_disturb_the_streaming_window() {
        // A player reads the footer (MP4 moov) before playing. Routing that
        // through the sequential window would reset it to the shallow depth
        // on every probe, which is what riven-ts avoids by pinning the
        // header/footer regions.
        let p = prefetcher(512 * 1024 * 1024, 1024 * 1024);
        let mut offset = 0u64;
        for _ in 0..6 {
            p.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }
        let grown = p.inner.lock().await.window;

        p.read(p.size() - 8192, 8192).await.unwrap();
        assert_eq!(
            p.inner.lock().await.window,
            grown,
            "a footer probe must not reset the window"
        );
    }

    #[tokio::test]
    async fn window_grows_while_sequential_and_resets_on_seek() {
        let p = prefetcher(512 * 1024 * 1024, 1024 * 1024);
        let mut offset = 0u64;
        for _ in 0..6 {
            p.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }
        let grown = p.inner.lock().await.window;
        assert!(grown > INITIAL_WINDOW, "sequential reads deepen the window");

        // A distant seek must drop back to the shallow window so scrubbing
        // does not pay for read-ahead it will discard.
        p.read(400 * 1024 * 1024, 128 * 1024).await.unwrap();
        assert_eq!(p.inner.lock().await.window, INITIAL_WINDOW * WINDOW_MULTIPLIER);
    }
}
