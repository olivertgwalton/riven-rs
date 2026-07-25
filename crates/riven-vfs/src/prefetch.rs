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
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::{Mutex, Notify};

use riven_core::local_source::ReadIntent;

use crate::source::ByteSource;

/// First window: 1 MiB plus Linux's 128 KiB readahead overshoot.
const INITIAL_WINDOW: u64 = 1024 * 1024 + 128 * 1024;
/// Window growth per sustained sequential read.
const WINDOW_MULTIPLIER: u64 = 2;
/// Kernel block size — the unit FUSE reads arrive in.
const BLOCK: u64 = 128 * 1024;
/// One fetch.
///
/// The origin is usenet: ~700 KiB articles, fetched in parallel *within* a
/// range, so a chunk is ~11 articles and lands when its slowest one does.
///
/// It is tempting to shrink this — fewer articles per chunk means a shorter
/// race and a lower tail — and that is right when chunk latency is set by
/// article *count*. It is wrong when it is set by how slow the wire is, and
/// the second case is what a struggling deployment is actually in. Halving
/// the chunk there simply halves the fill rate, because
/// `permits x CHUNK / chunk_latency` drops with `CHUNK` while
/// `chunk_latency` barely moves. Measured: 4 MiB put fill rate at ~3.3 MB/s
/// against a title needing ~5.4 MB/s, i.e. below the bitrate, and playback
/// could not keep up no matter how deep the window was allowed to grow.
///
/// So: size this for fill-rate headroom, and deal with the tail by fetching a
/// chunk's articles concurrently (which the read paths do) rather than by
/// asking for fewer of them.
const CHUNK: u64 = 8 * 1024 * 1024;
/// Reads landing within this distance ahead of the buffer are still
/// sequential rather than a seek. riven-ts's scan tolerance: 25 blocks.
const SEQUENTIAL_TOLERANCE: u64 = 25 * BLOCK;
/// How far *behind* the buffer a read may land and still not count as a seek.
///
/// Players keep more than one read position on a file — Infuse opens the
/// video handle plus a probe range, and the kernel issues readahead in
/// parallel, so requests arrive interleaved and slightly out of order. Those
/// reads land behind the consumed frontier. Treating them as seeks resets the
/// window, and with a second reader alternating it never survives long enough
/// to grow: measured at a pinned 2 MiB with 84 resets in six minutes of
/// plainly sequential playback. Serve them without disturbing the window.
const BACKWARD_TOLERANCE: u64 = 32 * 1024 * 1024;
/// Header players probe for container metadata (riven-ts: 256 KiB).
const HEADER_SIZE: u64 = 256 * 1024;
/// Footer bounds — MP4 keeps its `moov` atom at the end, so players read the
/// tail before playing. riven-ts: 2% of the file, clamped to 16 KiB..10 MiB.
const MIN_FOOTER: u64 = 16 * 1024;
const MAX_FOOTER: u64 = 10 * 1024 * 1024;
const FOOTER_PERCENT: f64 = 0.02;
/// Chunk fetches in flight at once, **across the whole process**.
///
/// This is deliberately global rather than per-handle, and that is the point.
/// A player does not open one handle per file — Infuse opens the video plus a
/// probe, a media server may analyse several titles at once — and every open
/// handle used to get its own independent budget. Three handles therefore ran
/// three times the intended fetches, and since each chunk fans out across all
/// its articles, that reached ~130 concurrent article fetches against a
/// provider that stops rewarding concurrency long before then.
///
/// Past that point extra concurrency is not merely wasted, it is harmful: the
/// provider spreads a fixed budget over more connections, so every individual
/// article gets slower and the *tail* — the thing that actually stalls
/// playback — gets much worse. Measured standalone against Newshosting,
/// aggregate throughput was already flat from 16 connections while
/// per-connection rate fell from 63 to 36 Mbps.
///
/// So the ceiling belongs to the process, which is the thing that shares the
/// connection pool. `CHUNK_PERMITS x articles-per-chunk` is the real number
/// of concurrent article fetches: 5 x ~6 is ~30, which sits at the plateau
/// and leaves the rest of the account for ingest and repair. Fill rate is
/// still `permits x CHUNK / chunk_latency` ~ 50 MB/s, an order of magnitude
/// above any single title's bitrate.
///
/// Override with `RIVEN_VFS_INFLIGHT_CHUNKS`.
const DEFAULT_CHUNK_PERMITS: usize = 5;

fn chunk_permits() -> &'static tokio::sync::Semaphore {
    static PERMITS: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    PERMITS.get_or_init(|| {
        let n = std::env::var("RIVEN_VFS_INFLIGHT_CHUNKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_CHUNK_PERMITS);
        tokio::sync::Semaphore::new(n)
    })
}

/// Woken whenever a chunk permit is returned.
///
/// A budget shared between handles needs this: a handle that finds the budget
/// exhausted dispatches nothing, so it has no fetch of its own whose
/// completion could wake it, and its own `progress` signal would never fire.
/// Without a cross-handle wake, such a reader blocks until some *other* part
/// of its own pipeline happens to make progress — which, when it is starved
/// precisely because it owns no in-flight work, is never.
fn permit_freed() -> &'static Notify {
    static FREED: std::sync::OnceLock<Notify> = std::sync::OnceLock::new();
    FREED.get_or_init(Notify::new)
}

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

#[derive(Default)]
struct Stats {
    /// Reads served straight from the buffer — the healthy path.
    hits: u64,
    /// Reads that had to wait for a fetch. These are the stalls a player
    /// experiences as buffering.
    misses: u64,
    /// Total time reads spent blocked, and the worst single wait.
    wait_ms: u64,
    worst_wait_ms: u64,
    /// Window resets caused by a non-sequential read.
    seeks: u64,
    /// Metadata probes served from the pinned header/footer.
    probes: u64,
    /// Reads from a second, interleaved reader landing behind the buffer.
    /// Distinct from `probes`: these indicate how much the player is reading
    /// out of order, which is what used to destroy the window.
    behind: u64,
    /// Chunk fetches and their cost.
    chunks: u64,
    chunk_ms: u64,
    worst_chunk_ms: u64,
    bytes: u64,
    last_log: Option<Instant>,
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
    /// Rolling diagnostics — the buffer is invisible from the outside, so
    /// without these a stall cannot be attributed to a shallow window, a
    /// starved pipeline, or a slow origin.
    stats: Stats,
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
                stats: Stats::default(),
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
            // Always demand: every caller of this — a pinned header/footer
            // probe, an interleaved read landing behind the buffer — has a
            // reader blocked on the result.
            let data = self.source.read_range(pos, end, ReadIntent::Demand).await?;
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
            let cached = if is_header {
                &inner.header
            } else {
                &inner.footer
            };
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
            self.inner.lock().await.stats.probes += 1;
            return self.read_pinned(start, want, true).await;
        }
        if start >= self.footer_start() {
            self.inner.lock().await.stats.probes += 1;
            return self.read_pinned(start, want, false).await;
        }

        self.source.report_position(start).await;

        let began = Instant::now();
        let mut waited = false;

        loop {
            // Both registered before the buffer is inspected, so a fill
            // completing between the check and the await cannot be missed.
            // `budget` covers the other way this reader can be unblocked:
            // it dispatched nothing because the shared budget was spent, and
            // only a permit coming back lets it make progress.
            let notified = self.progress.notified();
            let budget = permit_freed().notified();

            {
                let mut inner = self.inner.lock().await;

                if let Some(error) = inner.error.take() {
                    return Err(io::Error::other(error));
                }

                // Seek: outside the buffer and past the tolerance window means
                // restart read-ahead, back at the shallow initial depth so
                // scrubbing does not pay for a deep window it will discard.
                let in_buf = start >= inner.buf.start && start < inner.buf.end();
                let near =
                    start >= inner.buf.end() && start - inner.buf.end() <= SEQUENTIAL_TOLERANCE;
                // A read just behind the buffer is an interleaved reader, not
                // a seek. Its bytes are already consumed so it is served by a
                // one-off fetch below, but the window must survive it.
                let behind =
                    start < inner.buf.start && inner.buf.start - start <= BACKWARD_TOLERANCE;
                if behind {
                    inner.stats.behind += 1;
                    drop(inner);
                    let end = start + want as u64 - 1;
                    return self.read_exact_range(start, end).await;
                }
                if !in_buf && !near {
                    tracing::debug!(
                        target: "streaming",
                        from = inner.buf.end(), to = start,
                        window_mb = inner.window >> 20,
                        "prefetch: seek — window reset"
                    );
                    inner.stats.seeks += 1;
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
                        inner.window = (inner.window * WINDOW_MULTIPLIER).min(self.max_window);

                        let waited_ms = began.elapsed().as_millis() as u64;
                        if waited {
                            inner.stats.misses += 1;
                            inner.stats.wait_ms += waited_ms;
                            inner.stats.worst_wait_ms = inner.stats.worst_wait_ms.max(waited_ms);
                        } else {
                            inner.stats.hits += 1;
                        }
                        inner.stats.bytes += data.len() as u64;
                        self.maybe_log(&mut inner, start);

                        drop(inner);
                        self.dispatch_fills().await;
                        return Ok(data);
                    }
                }
            }

            // Keep the pipeline topped up, then wait for whichever comes
            // first: a chunk of our own landing, or the shared budget freeing
            // up so we can dispatch one. Fills run as their own tasks, so
            // fetching overlaps serving.
            waited = true;
            self.dispatch_fills().await;
            tokio::select! {
                () = notified => {}
                () = budget => {}
            }
        }
    }

    /// Emit a rolling summary every 10s.
    ///
    /// The single most diagnostic number here is the hit rate: a read served
    /// from the buffer is invisible to the player, a miss is a stall. If
    /// misses are high while `window_mb` is large, the origin is too slow for
    /// the depth; if `window_mb` stays small, the window is being reset by
    /// seeks and never gets a chance to grow.
    fn maybe_log(&self, inner: &mut Inner, position: u64) {
        const EVERY: Duration = Duration::from_secs(10);
        let now = Instant::now();
        match inner.stats.last_log {
            Some(last) if now.duration_since(last) < EVERY => return,
            _ => inner.stats.last_log = Some(now),
        }
        let s = &inner.stats;
        let reads = s.hits + s.misses;
        if reads == 0 {
            return;
        }
        tracing::info!(
            target: "streaming",
            position_mb = position >> 20,
            window_mb = inner.window >> 20,
            buffered_mb = inner.buf.len >> 20,
            inflight = inner.inflight,
            pending = inner.pending.len(),
            reads,
            hit_pct = (s.hits * 100) / reads,
            misses = s.misses,
            avg_wait_ms = if s.misses > 0 { s.wait_ms / s.misses } else { 0 },
            worst_wait_ms = s.worst_wait_ms,
            chunks = s.chunks,
            avg_chunk_ms = if s.chunks > 0 { s.chunk_ms / s.chunks } else { 0 },
            worst_chunk_ms = s.worst_chunk_ms,
            seeks = s.seeks,
            probes = s.probes,
            behind = s.behind,
            served_mb = s.bytes >> 20,
            "prefetch stats"
        );
    }

    /// Start fills until the window is covered or the in-flight cap is hit.
    /// Returns whether anything was dispatched.
    async fn dispatch_fills(self: &Arc<Self>) -> bool {
        let mut started = false;
        loop {
            // Claimed before the lock so a handle that is over its share
            // simply stops dispatching rather than blocking here holding it.
            // Nothing is lost by giving up: `read` calls this again after
            // every served read and every wake, so a freed permit is picked
            // up promptly.
            let Ok(permit) = chunk_permits().try_acquire() else {
                return started;
            };

            // Must *wait* for the lock, not `try_lock`. Under load the buffer
            // mutex is held constantly by concurrent readers, so a `try_lock`
            // here silently abandoned the dispatch and pinned the pipeline at
            // ~2 chunks regardless of the configured ceiling — the fill rate
            // never rose above consumption and the window never filled.
            let mut inner = self.inner.lock().await;
            if inner.error.is_some() || inner.frontier >= self.size {
                return started;
            }
            // Depth is measured from the buffer, so in-flight chunks count
            // toward the window and it cannot be over-committed.
            let ahead = inner.frontier.saturating_sub(inner.buf.start);
            if ahead >= inner.window {
                return started;
            }

            let from = inner.frontier;
            // The chunk starting exactly where the buffer ends is the one the
            // reader hits next, so if it is slow the reader blocks on it —
            // that is a demand read however speculatively it was dispatched.
            // Chunks further ahead are pure read-ahead. At steady state the
            // frontier runs well past the buffer and everything is read-ahead;
            // this only promotes fills issued while the buffer is dry, which
            // is exactly when the player is about to stall.
            let intent = if from == inner.buf.end() {
                ReadIntent::Demand
            } else {
                ReadIntent::ReadAhead
            };
            // Align the end to a CHUNK boundary so every pass over a file
            // splits segments at the same offsets, letting the origin's cache
            // serve the half it already has instead of refetching it. The
            // boundary must be strictly *past* `from`: `next_multiple_of`
            // returns `from` itself when it is already aligned, which used to
            // dispatch a one-byte chunk — a full article fetch, and a wasted
            // slot, at every chunk boundary.
            let to = ((from / CHUNK + 1) * CHUNK - 1).min(self.size - 1);
            inner.frontier = to + 1;
            inner.inflight += 1;
            drop(inner);

            // The permit rides into the task and is released when the fetch
            // finishes, whatever the outcome.
            let this = Arc::clone(self);
            tokio::spawn(async move {
                this.fill(from, to, intent).await;
                drop(permit);
                permit_freed().notify_waiters();
            });
            started = true;
        }
    }

    /// Fetch one chunk, slot it into place, and wake any waiters.
    async fn fill(&self, from: u64, to: u64, intent: ReadIntent) {
        let began = Instant::now();
        let result = self.source.read_range(from, to, intent).await;
        let took_ms = began.elapsed().as_millis() as u64;

        {
            let mut inner = self.inner.lock().await;
            inner.inflight -= 1;
            inner.stats.chunks += 1;
            inner.stats.chunk_ms += took_ms;
            inner.stats.worst_chunk_ms = inner.stats.worst_chunk_ms.max(took_ms);
            if took_ms >= 2000 {
                tracing::debug!(
                    target: "streaming",
                    from, len_mb = (to - from + 1) >> 20, took_ms,
                    inflight = inner.inflight,
                    buffered_mb = inner.buf.len >> 20,
                    window_mb = inner.window >> 20,
                    "prefetch: slow chunk"
                );
            }
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
        async fn read_range(&self, start: u64, end: u64, _intent: ReadIntent) -> io::Result<Bytes> {
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
    async fn many_handles_sharing_the_chunk_budget_all_finish() {
        // The read-ahead budget is process-wide, so more open handles than
        // permits is the normal case, not an edge one. A handle that loses
        // the race for a permit dispatches nothing — and therefore has no
        // fetch of its own whose completion could wake it. It has to be woken
        // when some *other* handle returns a permit, or it blocks forever.
        // Without that cross-handle wake this test hangs rather than fails.
        let readers = (0..8).map(|_| {
            let p = prefetcher(64 * 1024 * 1024, 1024 * 1024);
            tokio::spawn(async move {
                let mut offset = 0u64;
                for _ in 0..12 {
                    let data = p.read(offset, 128 * 1024).await.unwrap();
                    assert_eq!(data.len(), 128 * 1024);
                    offset += 128 * 1024;
                }
            })
        });
        let all = futures::future::join_all(readers);
        for result in tokio::time::timeout(Duration::from_secs(20), all)
            .await
            .expect(
                "every handle must make progress; a starved one means no wake on permit release",
            )
        {
            result.unwrap();
        }
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
        assert_eq!(
            p.inner.lock().await.window,
            INITIAL_WINDOW * WINDOW_MULTIPLIER
        );
    }
}
