//! One bounded read-ahead engine for every immutable network source.
//!
//! HTTP/debrid uses an 8 MiB fetch unit. Usenet reports its decoded article
//! size, so the same eight-unit window becomes eight NNTP segments, matching
//! streamnzb's `DefaultReadAhead`. Archive boundaries widen it to 24, matching
//! its `PlaybackReadAheadSegments`. Demand units are always dispatched before
//! speculative units.
//!
//! Memory is bounded once, by the shared decoded-segment cache below this
//! layer — the same place streamnzb bounds it. There is no second per-file
//! reservation here.
//!
//! Decoded units live in a [`UnitCache`] owned by the *file*, not by the
//! handle; the window and cursor stay per-handle. streamnzb splits it the same
//! way — cache and in-flight map on its `File`, cursor on each
//! `SegmentReader` — and the split matters because players open the same file
//! repeatedly and at several positions at once. Sharing the bytes makes a
//! re-open warm; sharing a cursor would make those positions fight.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use lru::LruCache;
use parking_lot::Mutex;
use riven_core::local_source::SourceLayout;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::source::ByteSource;

const MIB: u64 = 1024 * 1024;
const HTTP_CHUNK: u64 = 8 * MIB;
const READ_AHEAD_UNITS: usize = 8;
/// Units of cushion held ahead of the cursor on an archive source.
///
/// An article's arrival time has a long tail — p90 around 2.3s and outliers
/// past 6s against this provider — so the cushion has to be measured in
/// seconds of playback, not units. 64 articles is ~46 MB, which is 5.3s at
/// 68.7 Mbps: enough to sit through the tail instead of draining into it.
const ARCHIVE_READ_AHEAD_UNITS: usize = 64;
/// Unit fetches allowed on the wire at once, independent of the cushion above.
///
/// A unit usually spans two articles, so this is roughly 32 concurrent article
/// fetches — the top of the measured plateau, and short of the connection count
/// where the provider's aggregate collapses.
const ARCHIVE_MAX_IN_FLIGHT_UNITS: usize = 16;
const RETAIN_BEHIND_UNITS: u64 = 2;
const MIN_TAIL_PROBE: u64 = 16 * 1024;
const MAX_TAIL_PROBE: u64 = 10 * MIB;
/// Decoded bytes retained per open file. Wide enough to cover several handles'
/// windows at once, since a player streams one file from several positions.
pub const UNIT_CACHE_BYTES: u64 = 192 * MIB;

/// Decoded read-ahead units for one file, shared by every handle open on it.
///
/// Bounded by bytes rather than entries because a unit is an 8 MiB HTTP chunk
/// on one backend and a ~700 KiB article on the other.
pub struct UnitCache {
    state: Mutex<UnitCacheState>,
    max_bytes: u64,
}

struct UnitCacheState {
    units: LruCache<u64, Bytes>,
    bytes: u64,
}

impl UnitCache {
    pub fn new(max_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(UnitCacheState {
                units: LruCache::unbounded(),
                bytes: 0,
            }),
            max_bytes,
        })
    }

    fn get(&self, start: u64) -> Option<Bytes> {
        self.state.lock().units.get(&start).cloned()
    }

    /// Whether a unit is held, without counting as a use: scheduling decisions
    /// must not keep a unit alive that no reader has actually wanted.
    fn contains(&self, start: u64) -> bool {
        self.state.lock().units.peek(&start).is_some()
    }

    fn put(&self, start: u64, data: Bytes) {
        let mut state = self.state.lock();
        let added = data.len() as u64;
        if let Some(previous) = state.units.put(start, data) {
            state.bytes = state.bytes.saturating_sub(previous.len() as u64);
        }
        state.bytes = state.bytes.saturating_add(added);
        while state.bytes > self.max_bytes && state.units.len() > 1 {
            let Some((_, evicted)) = state.units.pop_lru() else {
                break;
            };
            state.bytes = state.bytes.saturating_sub(evicted.len() as u64);
        }
    }

    fn len(&self) -> usize {
        self.state.lock().units.len()
    }
}

#[derive(Clone)]
struct Config {
    unit: u64,
    boundaries: Arc<[u64]>,
}

impl Config {
    fn from_layout(layout: Option<SourceLayout>) -> Self {
        match layout {
            Some(layout) => Self {
                unit: layout.chunk_size.max(1),
                boundaries: layout.boundaries.into(),
            },
            None => Self {
                unit: HTTP_CHUNK,
                boundaries: Arc::from([]),
            },
        }
    }

    /// How far ahead of the cursor to keep units buffered. This is the depth of
    /// the cushion, not a concurrency limit — see [`Config::max_in_flight`].
    fn window_units(&self) -> usize {
        if self.boundaries.is_empty() {
            READ_AHEAD_UNITS
        } else {
            ARCHIVE_READ_AHEAD_UNITS
        }
    }

    /// How many unit fetches may be on the wire at once.
    ///
    /// Separate from [`Config::window_units`] because the two want opposite
    /// things. Depth wants to be large, to ride out a slow origin. Concurrency
    /// wants to be small: a provider divides an account's bandwidth by
    /// connection count, and past its limit the aggregate collapses rather than
    /// plateaus — measured here as 438 Mbps over 16 connections against
    /// ~108 Mbps over 63. While they shared one number, buffering deeper meant
    /// connecting wider, and the second effect cancelled the first.
    fn max_in_flight(&self) -> usize {
        if self.boundaries.is_empty() {
            READ_AHEAD_UNITS
        } else {
            ARCHIVE_MAX_IN_FLIGHT_UNITS
        }
    }

    fn unit_start(&self, position: u64) -> u64 {
        position - position % self.unit
    }
}

struct ReadRequest {
    start: u64,
    len: usize,
    reply: oneshot::Sender<io::Result<Bytes>>,
}

impl ReadRequest {
    fn end(&self) -> u64 {
        self.start + self.len as u64
    }

    fn respond(self, result: io::Result<Bytes>) {
        drop(self.reply.send(result));
    }
}

enum Command {
    Read(ReadRequest),
    #[cfg(test)]
    Inspect(oneshot::Sender<Snapshot>),
}

struct Fill {
    start: u64,
    result: io::Result<Bytes>,
}

struct Direct {
    id: u64,
    request: ReadRequest,
    result: io::Result<Bytes>,
}

/// One open network file. Dropping it closes the command channel; the actor
/// then aborts every outstanding range request.
pub struct Prefetcher {
    commands: mpsc::UnboundedSender<Command>,
    size: u64,
}

impl Prefetcher {
    pub fn new(source: Arc<dyn ByteSource>, cache: Arc<UnitCache>, runtime: &Handle) -> Self {
        let size = source.size();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (fill_tx, fill_rx) = mpsc::unbounded_channel();
        let (direct_tx, direct_rx) = mpsc::unbounded_channel();
        runtime.spawn(
            Actor {
                source,
                size,
                config: Config::from_layout(None),
                commands: command_rx,
                fills: fill_rx,
                fill_tx,
                directs: direct_rx,
                direct_tx,
                cursor: 0,
                resets: 0,
                cache,
                active: BTreeMap::new(),
                direct_active: BTreeMap::new(),
                next_direct_id: 1,
                pending: Vec::new(),
            }
            .run(),
        );
        Self {
            commands: command_tx,
            size,
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub async fn read(&self, start: u64, len: usize) -> io::Result<Bytes> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Read(ReadRequest { start, len, reply }))
            .map_err(|_send_error| io::Error::new(io::ErrorKind::BrokenPipe, "stream is closed"))?;
        response.await.map_err(|_receive_error| {
            io::Error::new(io::ErrorKind::BrokenPipe, "stream is closed")
        })?
    }

    #[cfg(test)]
    async fn snapshot(&self) -> Snapshot {
        let (reply, response) = oneshot::channel();
        self.commands.send(Command::Inspect(reply)).unwrap();
        response.await.unwrap()
    }
}

struct Actor {
    source: Arc<dyn ByteSource>,
    size: u64,
    config: Config,
    commands: mpsc::UnboundedReceiver<Command>,
    fills: mpsc::UnboundedReceiver<Fill>,
    fill_tx: mpsc::UnboundedSender<Fill>,
    directs: mpsc::UnboundedReceiver<Direct>,
    direct_tx: mpsc::UnboundedSender<Direct>,
    cursor: u64,
    resets: u64,
    /// Shared with every other handle on this file; see [`UnitCache`].
    cache: Arc<UnitCache>,
    active: BTreeMap<u64, JoinHandle<()>>,
    direct_active: BTreeMap<u64, JoinHandle<()>>,
    next_direct_id: u64,
    pending: Vec<ReadRequest>,
}

impl Actor {
    async fn run(mut self) {
        self.config = Config::from_layout(self.source.layout().await);

        loop {
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(Command::Read(request)) => self.on_read(request),
                    #[cfg(test)]
                    Some(Command::Inspect(reply)) => {
                        drop(reply.send(self.snapshot()));
                    }
                    None => break,
                },
                Some(fill) = self.fills.recv() => self.on_fill(fill),
                Some(direct) = self.directs.recv() => self.on_direct(direct),
            }
        }

        self.abort_active();
        self.abort_direct();
        for request in self.pending.drain(..) {
            request.respond(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream is closed",
            )));
        }
    }

    fn on_read(&mut self, mut request: ReadRequest) {
        if request.start >= self.size || request.len == 0 {
            request.respond(Ok(Bytes::new()));
            return;
        }
        request.len = request.len.min((self.size - request.start) as usize);
        self.source.report_position(request.start);

        if self.is_tail_probe(&request) {
            self.spawn_direct(request);
            return;
        }

        if let Some(data) = self.read_cached(request.start, request.len) {
            self.finish(request, data);
            self.dispatch();
            return;
        }

        if !self.in_current_window(&request) {
            self.reset(request.start);
        }

        let demand_units = self.unit_count(request.start, request.end());
        if demand_units > self.config.window_units() {
            self.spawn_direct(request);
            return;
        }

        self.schedule_range(request.start, request.end());
        self.pending.push(request);
        // Match streamnzb's SegmentReader: requested bytes complete before
        // speculative work starts. This protects startup and keeps metadata
        // probes from filling every NNTP connection ahead of their demand.
    }

    fn on_fill(&mut self, fill: Fill) {
        self.active.remove(&fill.start);
        match fill.result {
            Ok(data) => {
                self.cache.put(fill.start, data);
                self.serve_pending();
                self.dispatch();
            }
            // One unreadable unit fails the reads waiting on it and nothing
            // else. Sibling fills stay in flight and keep their bytes, the way
            // streamnzb leaves a shared segment download alone when one reader
            // gives up on it.
            Err(error) => {
                let message = error.to_string();
                for request in self.pending.drain(..) {
                    request.respond(Err(io::Error::new(error.kind(), message.clone())));
                }
            }
        }
    }

    fn on_direct(&mut self, direct: Direct) {
        self.direct_active.remove(&direct.id);
        direct.request.respond(direct.result);
    }

    /// Repoint this handle's window after a seek or a probe. Fills already on
    /// the wire keep running and their bytes stay in the shared [`UnitCache`],
    /// so a backward read or a probe never throws away work another handle —
    /// or this one, a moment later — still needs. The cache's own LRU bound
    /// reclaims what nothing reads again.
    ///
    /// Logged because a reset means this window starts over. It is no longer
    /// the stall it once was, now that the bytes survive it, but a burst of
    /// them still says the player is reading further apart than one window
    /// spans.
    fn reset(&mut self, position: u64) {
        self.resets += 1;
        tracing::debug!(
            target: "streaming",
            resets = self.resets,
            from = self.cursor,
            to = position,
            jump = position as i64 - self.cursor as i64,
            window_bytes = self.config.unit * self.config.window_units() as u64,
            cached_units = self.cache.len(),
            inflight_units = self.active.len(),
            "read-ahead window reset"
        );
        for request in self.pending.drain(..).collect::<Vec<_>>() {
            self.spawn_direct(request);
        }
        self.cursor = position;
    }

    fn abort_active(&mut self) {
        for (_, task) in std::mem::take(&mut self.active) {
            task.abort();
        }
    }

    fn abort_direct(&mut self) {
        for (_, task) in std::mem::take(&mut self.direct_active) {
            task.abort();
        }
    }

    fn in_current_window(&self, request: &ReadRequest) -> bool {
        let behind = self
            .cursor
            .saturating_sub(self.config.unit.saturating_mul(RETAIN_BEHIND_UNITS));
        let ahead = self.cursor.saturating_add(
            self.config
                .unit
                .saturating_mul(self.config.window_units() as u64),
        );
        request.start >= behind && request.end() <= ahead
    }

    /// Top the cushion back up, oldest gap first.
    ///
    /// Walks the whole window so a deep cushion is filled in cursor order, but
    /// stops as soon as the wire is busy. The units past that point are picked
    /// up by the next call, once a fetch lands — so depth costs memory and
    /// patience rather than connections.
    fn dispatch(&mut self) {
        let horizon = self.config.window_units();
        let in_flight = self.config.max_in_flight();
        let mut start = self.config.unit_start(self.cursor);
        for _ in 0..horizon {
            if start >= self.size || self.active.len() >= in_flight {
                break;
            }
            self.schedule_unit(start);
            start = start.saturating_add(self.config.unit);
        }
    }

    fn schedule_range(&mut self, start: u64, end: u64) {
        let mut unit = self.config.unit_start(start);
        while unit < end && unit < self.size {
            self.schedule_unit(unit);
            unit = unit.saturating_add(self.config.unit);
        }
    }

    fn schedule_unit(&mut self, start: u64) {
        if self.cache.contains(start) || self.active.contains_key(&start) || start >= self.size {
            return;
        }
        let end = start.saturating_add(self.config.unit).min(self.size);
        let source = self.source.clone();
        let sender = self.fill_tx.clone();
        let size = self.size;
        let task = tokio::spawn(async move {
            let result = read_exact(source, size, start, (end - start) as usize).await;
            drop(sender.send(Fill { start, result }));
        });
        self.active.insert(start, task);
    }

    fn spawn_direct(&mut self, request: ReadRequest) {
        let source = self.source.clone();
        let sender = self.direct_tx.clone();
        let size = self.size;
        let id = self.next_direct_id;
        self.next_direct_id = self.next_direct_id.wrapping_add(1);
        let task = tokio::spawn(async move {
            let result = read_exact(source, size, request.start, request.len).await;
            drop(sender.send(Direct {
                id,
                request,
                result,
            }));
        });
        self.direct_active.insert(id, task);
    }

    fn serve_pending(&mut self) {
        let mut waiting = Vec::new();
        for request in std::mem::take(&mut self.pending) {
            if let Some(data) = self.read_cached(request.start, request.len) {
                self.finish(request, data);
            } else {
                waiting.push(request);
            }
        }
        self.pending = waiting;
    }

    fn finish(&mut self, request: ReadRequest, data: Bytes) {
        self.cursor = self.cursor.max(request.start + data.len() as u64);
        request.respond(Ok(data));
    }

    fn read_cached(&self, start: u64, len: usize) -> Option<Bytes> {
        let end = start.checked_add(len as u64)?;
        let first_start = self.config.unit_start(start);
        if end <= first_start.saturating_add(self.config.unit) {
            let data = self.cache.get(first_start)?;
            let offset = (start - first_start) as usize;
            let wanted_end = offset.checked_add(len)?;
            return (wanted_end <= data.len()).then(|| data.slice(offset..wanted_end));
        }

        let mut output = BytesMut::with_capacity(len);
        let mut position = start;
        while position < end {
            let unit_start = self.config.unit_start(position);
            let data = self.cache.get(unit_start)?;
            let offset = (position - unit_start) as usize;
            if offset >= data.len() {
                return None;
            }
            let take = (end - position).min((data.len() - offset) as u64) as usize;
            output.extend_from_slice(&data[offset..offset + take]);
            position += take as u64;
        }
        Some(output.freeze())
    }

    fn unit_count(&self, start: u64, end: u64) -> usize {
        let first = self.config.unit_start(start);
        end.saturating_sub(first).div_ceil(self.config.unit) as usize
    }

    fn is_tail_probe(&self, request: &ReadRequest) -> bool {
        let tail = (self.size / 50).clamp(MIN_TAIL_PROBE, MAX_TAIL_PROBE);
        self.size > tail && request.start >= self.size - tail
    }

    #[cfg(test)]
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            unit: self.config.unit,
            window: self.config.window_units(),
            cursor: self.cursor,
            active: self.active.len(),
        }
    }
}

async fn read_exact(
    source: Arc<dyn ByteSource>,
    file_size: u64,
    start: u64,
    len: usize,
) -> io::Result<Bytes> {
    if start >= file_size || len == 0 {
        return Ok(Bytes::new());
    }
    let wanted = len.min((file_size - start) as usize);
    let end = start + wanted as u64;
    let mut position = start;
    let mut output = BytesMut::with_capacity(wanted);

    while position < end {
        let data = source.read_range(position, end - 1).await?;
        if data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("origin returned no data at {position}"),
            ));
        }
        let take = data.len().min((end - position) as usize);
        output.extend_from_slice(&data[..take]);
        position += take as u64;
    }
    Ok(output.freeze())
}

#[cfg(test)]
#[derive(Debug)]
struct Snapshot {
    unit: u64,
    window: usize,
    cursor: u64,
    active: usize,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    struct Source {
        size: u64,
        unit: u64,
        boundaries: Vec<u64>,
        max_return: usize,
        calls: Mutex<Vec<(u64, u64)>>,
        active: AtomicUsize,
        peak: AtomicUsize,
    }

    impl Source {
        fn new(size: u64, unit: u64) -> Arc<Self> {
            Arc::new(Self {
                size,
                unit,
                boundaries: Vec::new(),
                max_return: usize::MAX,
                calls: Mutex::new(Vec::new()),
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            })
        }

        fn with_boundaries(size: u64, unit: u64, boundaries: Vec<u64>) -> Arc<Self> {
            Arc::new(Self {
                size,
                unit,
                boundaries,
                max_return: usize::MAX,
                calls: Mutex::new(Vec::new()),
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            })
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl ByteSource for Source {
        async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
            self.calls.lock().unwrap().push((start, end));
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            tokio::task::yield_now().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            let len = (end - start + 1) as usize;
            Ok(Bytes::from(vec![b'x'; len.min(self.max_return)]))
        }

        async fn layout(&self) -> Option<SourceLayout> {
            Some(SourceLayout {
                chunk_size: self.unit,
                boundaries: self.boundaries.clone(),
            })
        }

        fn size(&self) -> u64 {
            self.size
        }
    }

    fn reader(source: Arc<Source>) -> Prefetcher {
        Prefetcher::new(source, UnitCache::new(UNIT_CACHE_BYTES), &Handle::current())
    }

    async fn wait_for_calls(source: &Source, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while source.call_count() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn one_engine_keeps_exactly_eight_units_in_flight() {
        let source = Source::new(10_000, 100);
        let prefetcher = reader(source.clone());

        let data = prefetcher.read(0, 50).await.unwrap();
        assert_eq!(data.len(), 50);
        wait_for_calls(&source, READ_AHEAD_UNITS).await;
        tokio::task::yield_now().await;

        assert_eq!(source.call_count(), READ_AHEAD_UNITS);
        assert!(source.peak.load(Ordering::SeqCst) <= READ_AHEAD_UNITS);
        let snapshot = prefetcher.snapshot().await;
        assert_eq!(snapshot.unit, 100);
        assert_eq!(snapshot.window, READ_AHEAD_UNITS);
    }

    #[tokio::test]
    async fn an_archive_buffers_deeper_than_it_connects() {
        let source = Source::with_boundaries(1_000_000, 100, vec![800]);
        let prefetcher = reader(source.clone());

        prefetcher.read(0, 50).await.unwrap();
        // The cushion is deeper than the wire is wide, so the whole window is
        // eventually fetched...
        wait_for_calls(&source, ARCHIVE_READ_AHEAD_UNITS).await;
        let snapshot = prefetcher.snapshot().await;
        assert_eq!(snapshot.window, ARCHIVE_READ_AHEAD_UNITS);

        // ...without ever opening more than the in-flight cap at once. This is
        // the whole point of the split: depth must not cost connections.
        assert!(
            source.peak.load(Ordering::SeqCst) <= ARCHIVE_MAX_IN_FLIGHT_UNITS,
            "peak {} exceeded in-flight cap {ARCHIVE_MAX_IN_FLIGHT_UNITS}",
            source.peak.load(Ordering::SeqCst)
        );
        const { assert!(ARCHIVE_MAX_IN_FLIGHT_UNITS < ARCHIVE_READ_AHEAD_UNITS) };
    }

    #[tokio::test]
    async fn cached_reads_do_not_touch_the_origin_again() {
        let source = Source::new(10_000, 100);
        let prefetcher = reader(source.clone());

        prefetcher.read(0, 50).await.unwrap();
        wait_for_calls(&source, READ_AHEAD_UNITS).await;
        let before = source.call_count();
        assert_eq!(prefetcher.read(10, 20).await.unwrap().len(), 20);
        tokio::task::yield_now().await;
        assert_eq!(source.call_count(), before);
    }

    #[tokio::test]
    async fn a_reopened_handle_reads_the_previous_one_s_bytes_off_the_wire() {
        let source = Source::new(100_000, 100);
        let cache = UnitCache::new(UNIT_CACHE_BYTES);

        // One handle warms the window, then goes away — as a player's range
        // request does every couple of seconds.
        let first = Prefetcher::new(source.clone(), Arc::clone(&cache), &Handle::current());
        first.read(5_000, 50).await.unwrap();
        wait_for_calls(&source, READ_AHEAD_UNITS).await;
        tokio::task::yield_now().await;
        let warm = source.call_count();
        drop(first);

        // Its replacement starts with a cold cursor but a warm cache: the read
        // it opens with must be served without touching the origin again.
        let second = Prefetcher::new(source.clone(), cache, &Handle::current());
        assert_eq!(second.read(5_000, 50).await.unwrap().len(), 50);
        assert_eq!(source.call_count(), warm);
    }

    #[tokio::test]
    async fn seek_repoints_the_window_without_widening_it() {
        let source = Source::new(100_000, 100);
        let prefetcher = reader(source);

        prefetcher.read(0, 50).await.unwrap();
        prefetcher.read(50_000, 50).await.unwrap();
        let after = prefetcher.snapshot().await;

        assert!(after.cursor >= 50_050);
        assert!(after.active <= READ_AHEAD_UNITS);
    }

    #[tokio::test]
    async fn a_backward_probe_keeps_the_bytes_the_window_already_holds() {
        let source = Source::new(100_000, 100);
        let prefetcher = reader(source.clone());

        prefetcher.read(5_000, 50).await.unwrap();
        wait_for_calls(&source, READ_AHEAD_UNITS).await;

        // Far enough back to leave the window, the way a player probes.
        prefetcher.read(0, 50).await.unwrap();
        wait_for_calls(&source, READ_AHEAD_UNITS * 2).await;
        tokio::task::yield_now().await;
        let settled = source.call_count();

        // Back to where playback was: those bytes were never discarded.
        prefetcher.read(5_000, 50).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(source.call_count(), settled);
    }

    #[tokio::test]
    async fn fills_short_origin_responses_before_replying() {
        let source = Arc::new(Source {
            size: 10_000,
            unit: 100,
            boundaries: Vec::new(),
            max_return: 7,
            calls: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let prefetcher = reader(source);
        assert_eq!(prefetcher.read(0, 80).await.unwrap().len(), 80);
    }

    #[tokio::test]
    async fn only_true_eof_returns_short() {
        let source = Source::new(1_000, 100);
        let prefetcher = reader(source);
        assert_eq!(prefetcher.read(950, 100).await.unwrap().len(), 50);
        assert!(prefetcher.read(1_000, 100).await.unwrap().is_empty());
    }
}
