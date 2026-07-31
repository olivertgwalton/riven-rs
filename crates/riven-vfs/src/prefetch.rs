//! One bounded read-ahead engine for every immutable network source.
//!
//! HTTP/debrid uses an 8 MiB fetch unit and buffers eight of them. Usenet
//! reports its decoded article size instead, and buffers eight articles — the
//! same depth streamnzb uses. A count rather than a byte budget means the
//! cushion's duration varies with the poster's segment size; that is accepted
//! deliberately, to stream at streamnzb's depth. Depth is held to at least twice
//! the number of fetches allowed on the wire, so a slow article never leaves the
//! window with nothing left to schedule. Demand units are always dispatched
//! before speculative units.
//!
//! Decoded units live in one process-wide [`UnitCache`] keyed by file; the
//! window and cursor stay per-handle. streamnzb splits it the same way, and the
//! split matters because players open the same file repeatedly and at several
//! positions at once: sharing the bytes makes a re-open warm, sharing a cursor
//! would make those positions fight.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::sync::OnceLock;

use bytes::{Bytes, BytesMut};
use riven_core::cache::{ByteLru, CacheStats, READ_AHEAD};
use riven_core::local_source::SourceLayout;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::source::ByteSource;

const MIB: u64 = 1024 * 1024;
/// The range request we impose on a plain ranged HTTP origin, which has no
/// fetch unit of its own.
///
/// 1 MiB, matching riven-ts's `chunkSize` (`vfs/config.ts`) against the same
/// debrid origins. It was 8 MiB, which made the cushion below 64 MiB for a
/// single open file — a sixth of the whole read-ahead cache per stream. riven
/// picks this number, unlike an article's size, which the poster picks.
const HTTP_CHUNK: u64 = MIB;
/// Units of cushion on the HTTP origin, where a unit is one [`HTTP_CHUNK`]
/// range request.
///
/// riven-ts holds no application-level cushion here at all — it keeps one
/// open-ended response per handle and pulls chunks off it as the player asks,
/// so its cushion is whatever the socket has buffered. riven fetches discrete
/// ranges instead, so it has to hold its own; 8 units is 8 MiB of it, which is
/// strictly more buffer than riven-ts runs on, at 8 MiB of memory per stream.
const HTTP_READ_AHEAD_UNITS: usize = 8;
/// Cushion held ahead of the cursor on an article origin, in **articles**.
///
/// Set to match streamnzb's `DefaultReadAhead` (8) in
/// `pkg/media/loader/segment_reader.go`, so the two stream with the same depth.
///
/// Note the tradeoff this accepts: a unit is one article and posters choose the
/// segment size, so a fixed count buys a cushion whose *duration* varies with
/// the post — 8 articles is ~5.6 MB on a 720 KiB post but ~30 MB on a 3.84 MB
/// one. The previous policy sized the cushion in bytes (96 MiB) for that reason.
/// Matching streamnzb's count was chosen deliberately over matching its bytes.
const ARTICLE_READ_AHEAD_UNITS: usize = 8;
/// Range requests allowed on the wire at once for one HTTP handle.
///
/// Was 8 — the whole window, so depth equalled width. That is the arrangement
/// the article path documents as the reason its wire went idle while a player's
/// buffer drained: with every unit around a slow one cached-or-active, and
/// nothing beyond the window schedulable, `dispatch` had nothing to pull
/// forward. The HTTP path had the same defect and no test pinning it.
///
/// 4 keeps the same half-the-window margin the article path holds, and is still
/// four times the parallelism riven-ts gets away with — it reads one open-ended
/// response per handle, sequentially.
const HTTP_MAX_IN_FLIGHT_UNITS: usize = 4;
/// Article fetches allowed on the wire at once, independent of the cushion
/// above.
///
/// Separate from the cushion because the two want opposite things: depth wants
/// to be large, to ride out a slow origin, while concurrency wants to be small
/// — a provider divides an account's bandwidth by connection count, and past
/// its limit the aggregate collapses rather than plateaus (measured as
/// 438 Mbps over 16 connections against ~108 Mbps over 63).
///
/// Defined in `riven-core` beside the segment cache, which has to be able to
/// hold a whole generation of these fetches at once.
const ARTICLE_MAX_IN_FLIGHT_UNITS: usize = riven_core::cache::ARTICLE_MAX_IN_FLIGHT;
const RETAIN_BEHIND_UNITS: u64 = 2;
/// How many windows' worth of units one stream may hold — its own, plus the
/// ones a seek stranded ahead of the cursor. Three, so a player can probe away
/// and back without refetching, while a file's worth of abandoned windows can
/// never accumulate behind one open handle.
const ABANDONED_WINDOWS: usize = 3;
const MIN_TAIL_PROBE: u64 = 16 * 1024;
const MAX_TAIL_PROBE: u64 = 10 * MIB;
/// The one read-ahead cache: every open file, both origins, one budget. Its hit
/// rate is the one that answers "did the player's read touch the origin?".
pub fn shared_unit_cache() -> &'static UnitCache {
    static CACHE: OnceLock<UnitCache> = OnceLock::new();
    CACHE.get_or_init(|| UnitCache::with_budget(READ_AHEAD))
}

pub fn read_ahead_stats() -> CacheStats {
    shared_unit_cache().stats()
}

/// Bumping `revision` makes every unit cached under the old one unreachable.
/// That is the whole of read-ahead invalidation: no sweep, and the stale units
/// age out of the LRU as the cold entries they are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileKey {
    pub revision: u64,
    pub ino: u64,
}

impl FileKey {
    /// Key for a reader outside the FUSE mount — the HTTP media bridge, which
    /// reads by entry id rather than by inode.
    ///
    /// The reserved revision keeps the two namespaces apart. Mounted files take
    /// their revision from the filesystem settings counter, which starts at 0
    /// and is bumped a handful of times per process, so it can never reach this
    /// value — and it must not, because an entry id colliding with an unrelated
    /// inode would serve one file's cached bytes as another's.
    pub fn bridge(entry_id: i64) -> Self {
        Self {
            revision: u64::MAX,
            ino: entry_id.cast_unsigned(),
        }
    }
}

/// Bounded by bytes, not entries: a unit is an 8 MiB HTTP chunk on one backend
/// and a ~700 KiB article on the other. Every file shares the one LRU, so a
/// file that stops being read loses its place to one that has not.
pub type UnitCache = ByteLru<(FileKey, u64), Bytes>;

#[derive(Clone)]
struct Config {
    unit: u64,
    /// Whether the origin has a natural fetch unit of its own — an article —
    /// as opposed to the 8 MiB chunk we impose on a plain ranged HTTP origin.
    ///
    /// This, and not whether the file happens to be a RAR, is what decides the
    /// read-ahead policy. Keying it off container layout was the bug: a
    /// non-RAR usenet post fell through to the HTTP numbers and streamed 8
    /// articles deep on 8 connections, out of the 100 the provider allows.
    articles: bool,
}

impl Config {
    fn from_layout(layout: Option<SourceLayout>) -> Self {
        match layout {
            Some(layout) => Self {
                unit: layout.chunk_size.max(1),
                articles: true,
            },
            None => Self {
                unit: HTTP_CHUNK,
                articles: false,
            },
        }
    }

    /// How far ahead of the cursor to keep units buffered. This is the depth of
    /// the cushion, not a concurrency limit — see [`Config::max_in_flight`].
    fn window_units(&self) -> usize {
        if !self.articles {
            return HTTP_READ_AHEAD_UNITS;
        }
        // A flat article count, matching streamnzb. Floored at twice the wire so
        // [`Config::max_in_flight`] keeps its margin whatever the constants are
        // set to.
        ARTICLE_READ_AHEAD_UNITS
            .max(ARTICLE_MAX_IN_FLIGHT_UNITS * 2)
            .max(2)
    }

    /// How many unit fetches may be on the wire at once.
    ///
    /// Held to half the window so the cushion is always deeper than the wire is
    /// wide. That margin is what [`Actor::dispatch`] needs to pull work forward
    /// past a unit that is taking its time: while the two were one number, the
    /// units around a slow one all went cached-or-active, nothing further was
    /// schedulable, and the wire went idle at exactly the moment the player's
    /// buffer was draining.
    fn max_in_flight(&self) -> usize {
        let cap = if self.articles {
            ARTICLE_MAX_IN_FLIGHT_UNITS
        } else {
            HTTP_MAX_IN_FLIGHT_UNITS
        };
        cap.min(self.window_units() / 2).max(1)
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
    pub fn new(source: Arc<dyn ByteSource>, file: FileKey, runtime: &Handle) -> Self {
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
                file,
                cache: shared_unit_cache(),
                active: BTreeMap::new(),
                direct_active: BTreeMap::new(),
                next_direct_id: 1,
                pending: Vec::new(),
                held: BTreeSet::new(),
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
    /// Which file this actor's units belong to in the shared cache.
    file: FileKey,
    cache: &'static UnitCache,
    active: BTreeMap<u64, JoinHandle<()>>,
    direct_active: BTreeMap<u64, JoinHandle<()>>,
    next_direct_id: u64,
    pending: Vec<ReadRequest>,
    /// Unit starts this actor has put into the shared cache and not yet
    /// released. The actor evicts only its own keys, so trimming one stream
    /// can never take another's cushion.
    held: BTreeSet<u64>,
}

impl Actor {
    fn key(&self, start: u64) -> (FileKey, u64) {
        (self.file, start)
    }

    /// Release the units this stream is done with, so its footprint is what it
    /// is reading rather than everything it has ever read.
    ///
    /// This is the per-stream half of the read-ahead budget. The shared cache's
    /// byte ceiling is a backstop for many streams at once; on its own it is
    /// the wrong instrument, because an LRU only sheds bytes under global
    /// pressure — so a single stream would fill it with consumed tail and hold
    /// that until some other file needed the room. Playback is sequential and
    /// almost none of that tail is read twice: the cache measured 561 entries
    /// at an 80 % hit rate, nearly all of it behind the cursor.
    ///
    /// Only the tail goes. Units *ahead* of the cursor are left alone even when
    /// a seek has stranded them, because a player that probes backwards — for a
    /// header, or a footer — and then resumes must find its playback bytes
    /// still there; discarding them on the way past is the stall this window
    /// was built to survive. What bounds them instead is
    /// [`ABANDONED_WINDOWS`]: a couple of stranded windows are worth keeping,
    /// a file's worth of them is not.
    fn trim_to_window(&mut self) {
        let unit = self.config.unit;
        let behind = self.cursor.saturating_sub(unit * RETAIN_BEHIND_UNITS);
        let consumed: Vec<u64> = self
            .held
            .range(..behind)
            .copied()
            .filter(|start| start.saturating_add(unit) <= behind)
            .collect();
        for start in consumed {
            self.release(start);
        }

        // Whatever seeks have stranded ahead of the cursor, drop the furthest
        // first — that is the one playback is least likely to return to.
        while self.held.len() > self.held_unit_cap() {
            let (Some(&first), Some(&last)) =
                (self.held.iter().next(), self.held.iter().next_back())
            else {
                break;
            };
            let furthest = if self.cursor.abs_diff(first) >= self.cursor.abs_diff(last) {
                first
            } else {
                last
            };
            self.release(furthest);
        }
    }

    /// Units one stream may hold: its own window plus room for the windows a
    /// recent seek left behind.
    fn held_unit_cap(&self) -> usize {
        (self.config.window_units() + RETAIN_BEHIND_UNITS as usize) * ABANDONED_WINDOWS
    }

    fn release(&mut self, start: u64) {
        self.cache.remove(&self.key(start));
        self.held.remove(&start);
    }

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
            // Bypasses the cache for the origin, so it costs what a miss costs.
            self.cache.record(false);
            self.spawn_direct(request);
            return;
        }

        if let Some(data) = self.read_cached(request.start, request.len) {
            self.cache.record(true);
            self.finish(request, data);
            self.dispatch();
            return;
        }
        self.cache.record(false);

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
                let weight = data.len() as u64;
                self.cache.put(self.key(fill.start), data, weight);
                self.held.insert(fill.start);
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
            cached_units = self.cache.stats().entries,
            inflight_units = self.active.len(),
            "read-ahead window reset"
        );
        for request in self.pending.drain(..).collect::<Vec<_>>() {
            self.spawn_direct(request);
        }
        self.cursor = position;
        self.trim_to_window();
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
        if self.cache.contains(&self.key(start))
            || self.active.contains_key(&start)
            || start >= self.size
        {
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
        self.trim_to_window();
        request.respond(Ok(data));
    }

    fn read_cached(&self, start: u64, len: usize) -> Option<Bytes> {
        let end = start.checked_add(len as u64)?;
        let first_start = self.config.unit_start(start);
        if end <= first_start.saturating_add(self.config.unit) {
            let data = self.cache.touch(&self.key(first_start))?;
            let offset = (start - first_start) as usize;
            let wanted_end = offset.checked_add(len)?;
            return (wanted_end <= data.len()).then(|| data.slice(offset..wanted_end));
        }

        let mut output = BytesMut::with_capacity(len);
        let mut position = start;
        while position < end {
            let unit_start = self.config.unit_start(position);
            let data = self.cache.touch(&self.key(unit_start))?;
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
            held: self.held.len(),
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
    /// Units this stream is holding in the shared cache — its memory footprint,
    /// in units.
    held: usize,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    struct Source {
        size: u64,
        unit: u64,
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
            })
        }

        fn size(&self) -> u64 {
            self.size
        }
    }

    /// Tests share the one process-wide cache, so each takes its own key.
    fn next_file() -> FileKey {
        static NEXT_INO: AtomicU64 = AtomicU64::new(1);
        FileKey {
            revision: 0,
            ino: NEXT_INO.fetch_add(1, Ordering::SeqCst),
        }
    }

    fn reader(source: Arc<Source>) -> Prefetcher {
        Prefetcher::new(source, next_file(), &Handle::current())
    }

    /// Wait until read-ahead stops issuing fetches. These tests care that the
    /// window has settled, not how many units that took — which depends on where
    /// the cursor landed and on the cushion arithmetic under test.
    async fn wait_until_settled(source: &Source) {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut last = usize::MAX;
            loop {
                let seen = source.call_count();
                if seen > 0 && seen == last {
                    return;
                }
                last = seen;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_stream_fills_its_cushion_and_stops_there() {
        let source = Source::new(10_000, 100);
        let prefetcher = reader(source.clone());

        let data = prefetcher.read(0, 50).await.unwrap();
        assert_eq!(data.len(), 50);
        wait_until_settled(&source).await;

        let snapshot = prefetcher.snapshot().await;
        assert_eq!(snapshot.unit, 100);
        // The cushion is a flat unit count, so read-ahead stops one window past
        // the cursor rather than running to the end of the file. The demand unit
        // is fetched alongside the window, hence the +1.
        let units_available = 10_000usize.div_ceil(100);
        assert!(
            source.call_count() <= snapshot.window + 1,
            "fetched {} units for a {}-unit window",
            source.call_count(),
            snapshot.window
        );
        assert!(
            source.call_count() < units_available,
            "read-ahead ran to the end of the file instead of stopping at the cushion"
        );
    }

    /// The cushion is a flat article count, matching streamnzb — so it is the
    /// same number of units on every post, and the bytes it buys vary with the
    /// poster's segment size. That variance is the accepted cost of matching
    /// streamnzb's depth rather than its byte cushion.
    #[test]
    fn the_article_cushion_is_a_flat_unit_count_whatever_the_segment_size() {
        for unit in [716 * 1024, 768_000, 3_840_000] {
            let config = Config {
                unit,
                articles: true,
            };
            assert_eq!(
                config.window_units(),
                ARTICLE_READ_AHEAD_UNITS,
                "a {unit}-byte segment changed the unit depth"
            );
        }
    }

    /// Depth must exceed the wire, or `dispatch` has nothing left to schedule
    /// once the units around a slow one are all cached-or-active — which is how
    /// the wire went idle at exactly the moment the player's buffer drained.
    ///
    /// Both origins, now. The HTTP path used to set width equal to depth and
    /// had no test saying it should not, so it carried the same defect the
    /// article path had already been fixed for.
    #[test]
    fn the_cushion_is_always_deeper_than_the_wire_is_wide() {
        let mut configs: Vec<Config> = [1, 716 * 1024, 3_840_000, MIB, 32 * MIB]
            .into_iter()
            .map(|unit| Config {
                unit,
                articles: true,
            })
            .collect();
        configs.push(Config::from_layout(None));

        for config in configs {
            assert!(
                config.max_in_flight() < config.window_units(),
                "unit {} (articles={}): {} in flight against a {}-unit window",
                config.unit,
                config.articles,
                config.max_in_flight(),
                config.window_units()
            );
        }
    }

    /// The flat count applies to large segments too, so the bytes held ahead of
    /// the cursor scale with the segment size rather than being capped. Recorded
    /// here because it is the sharp edge of matching streamnzb by count: a
    /// 32 MiB-segment post buffers 8 × 32 MiB.
    #[test]
    fn a_large_segment_scales_the_cushion_in_bytes() {
        for unit in [MIB, 8 * MIB, 32 * MIB] {
            let config = Config {
                unit,
                articles: true,
            };
            assert_eq!(config.window_units(), ARTICLE_READ_AHEAD_UNITS);
            assert_eq!(
                config.window_units() as u64 * unit,
                ARTICLE_READ_AHEAD_UNITS as u64 * unit
            );
        }
    }

    /// The HTTP origin keeps its own unit and depth, but no longer its own
    /// answer on width — see [`HTTP_MAX_IN_FLIGHT_UNITS`].
    #[test]
    fn an_http_origin_keeps_its_own_numbers() {
        let config = Config::from_layout(None);
        assert_eq!(config.unit, HTTP_CHUNK);
        assert_eq!(config.window_units(), HTTP_READ_AHEAD_UNITS);
        assert_eq!(config.max_in_flight(), HTTP_MAX_IN_FLIGHT_UNITS);
    }

    /// The regression the per-stream trim exists for. A stream reading a long
    /// file end to end used to leave every unit it had consumed in the shared
    /// cache, because an LRU only sheds bytes under global pressure — so one
    /// player filled a 384 MiB cache with tail that sequential playback would
    /// never read again. What a stream holds must be its window, not its
    /// history, however far it has read.
    #[tokio::test]
    async fn a_long_read_holds_its_window_not_its_history() {
        let unit = 64 * 1024;
        let source = Source::new(unit * 400, unit);
        let prefetcher = reader(source.clone());

        let mut position = 0;
        for _ in 0..200 {
            prefetcher.read(position, unit as usize).await.unwrap();
            position += unit;
        }
        wait_until_settled(&source).await;

        let snapshot = prefetcher.snapshot().await;
        let ceiling = snapshot.window + RETAIN_BEHIND_UNITS as usize + 1;
        assert!(
            snapshot.held <= ceiling,
            "held {} units after reading {} — the window is {}",
            snapshot.held,
            position / unit,
            snapshot.window
        );
    }

    /// Seeking around a file strands windows ahead of the cursor, which the
    /// tail trim never looks at. They are deliberately kept — a probe that
    /// returns must not refetch — but they are capped, so a player that seeks
    /// all day cannot pin a file's worth of units behind one handle.
    #[tokio::test]
    async fn seeking_around_a_file_cannot_grow_a_stream_without_bound() {
        let unit = 64 * 1024;
        let source = Source::new(unit * 400, unit);
        let prefetcher = reader(source.clone());

        for target in [200u64, 10, 300, 40, 350, 90, 250, 5, 150, 320] {
            prefetcher.read(unit * target, unit as usize).await.unwrap();
            wait_until_settled(&source).await;
        }

        let snapshot = prefetcher.snapshot().await;
        let cap = (snapshot.window + RETAIN_BEHIND_UNITS as usize) * ABANDONED_WINDOWS;
        assert!(
            snapshot.held <= cap,
            "held {} units after ten seeks — the cap is {cap}",
            snapshot.held
        );
    }

    /// `riven-core` sizes the process against a per-stream figure it cannot
    /// derive, because the window arithmetic lives here. If this window grows
    /// past what that figure prices, the memory target silently stops holding —
    /// so the two are pinned together rather than kept in step by hand.
    #[test]
    fn one_stream_costs_what_the_memory_budget_prices_it_at() {
        let config = Config::from_layout(None);
        let held = (config.window_units() as u64 + RETAIN_BEHIND_UNITS) * config.unit;
        assert!(
            held <= riven_core::cache::READ_AHEAD_PER_STREAM,
            "an HTTP stream holds {held} bytes, over the {} it is budgeted",
            riven_core::cache::READ_AHEAD_PER_STREAM
        );
    }

    #[tokio::test]
    async fn depth_never_costs_connections() {
        let source = Source::new(1_000_000, 100);
        let prefetcher = reader(source.clone());

        prefetcher.read(0, 50).await.unwrap();
        // Wait for the window to settle rather than for a fixed call count: the
        // cushion is now a flat 8 units, so it stops well short of any multiple
        // of the in-flight cap.
        wait_until_settled(&source).await;

        let snapshot = prefetcher.snapshot().await;
        assert!(
            snapshot.window > ARTICLE_MAX_IN_FLIGHT_UNITS,
            "a {}-unit window leaves nothing to schedule past a slow unit",
            snapshot.window
        );

        // However deep the cushion, the wire stays capped. That is the whole
        // point of the split: buffering deeper must not mean connecting wider,
        // because past the provider's limit the aggregate collapses.
        assert!(
            source.peak.load(Ordering::SeqCst) <= ARTICLE_MAX_IN_FLIGHT_UNITS,
            "peak {} exceeded the in-flight cap {ARTICLE_MAX_IN_FLIGHT_UNITS}",
            source.peak.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn cached_reads_do_not_touch_the_origin_again() {
        let source = Source::new(10_000, 100);
        let prefetcher = reader(source.clone());

        prefetcher.read(0, 50).await.unwrap();
        wait_until_settled(&source).await;
        let before = source.call_count();
        assert_eq!(prefetcher.read(10, 20).await.unwrap().len(), 20);
        tokio::task::yield_now().await;
        assert_eq!(source.call_count(), before);
    }

    #[tokio::test]
    async fn a_reopened_handle_reads_the_previous_one_s_bytes_off_the_wire() {
        let source = Source::new(100_000, 100);
        let file = next_file();

        // One handle warms the window, then goes away — as a player's range
        // request does every couple of seconds.
        let first = Prefetcher::new(source.clone(), file, &Handle::current());
        first.read(5_000, 50).await.unwrap();
        wait_until_settled(&source).await;
        let warm = source.call_count();
        drop(first);

        // Its replacement starts with a cold cursor but the same key, so it
        // finds those units and must not touch the origin again.
        let second = Prefetcher::new(source.clone(), file, &Handle::current());
        assert_eq!(second.read(5_000, 50).await.unwrap().len(), 50);
        assert_eq!(source.call_count(), warm);
    }

    #[tokio::test]
    async fn a_new_revision_makes_warm_units_unreachable() {
        let source = Source::new(100_000, 100);
        let file = next_file();

        let first = Prefetcher::new(source.clone(), file, &Handle::current());
        first.read(5_000, 50).await.unwrap();
        wait_until_settled(&source).await;
        let warm = source.call_count();
        drop(first);

        // A settings change can repoint the path at different bytes, so the
        // warm units must not be reused however hot they are.
        let bumped = FileKey {
            revision: file.revision + 1,
            ..file
        };
        let second = Prefetcher::new(source.clone(), bumped, &Handle::current());
        assert_eq!(second.read(5_000, 50).await.unwrap().len(), 50);
        assert!(
            source.call_count() > warm,
            "a new revision must refetch rather than serve stale bytes"
        );
    }

    #[tokio::test]
    async fn seek_repoints_the_window_without_widening_it() {
        let source = Source::new(100_000, 100);
        let prefetcher = reader(source);

        prefetcher.read(0, 50).await.unwrap();
        prefetcher.read(50_000, 50).await.unwrap();
        let after = prefetcher.snapshot().await;

        assert!(after.cursor >= 50_050);
        assert!(after.active <= ARTICLE_MAX_IN_FLIGHT_UNITS);
    }

    #[tokio::test]
    async fn a_backward_probe_keeps_the_bytes_the_window_already_holds() {
        let source = Source::new(100_000, 100);
        let prefetcher = reader(source.clone());

        prefetcher.read(5_000, 50).await.unwrap();
        wait_until_settled(&source).await;

        // Far enough back to leave the window, the way a player probes.
        prefetcher.read(0, 50).await.unwrap();
        wait_until_settled(&source).await;
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
