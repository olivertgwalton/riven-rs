//! Per-handle read-ahead for immutable network files.
//!
//! The implementation is deliberately an actor. One task owns the buffer,
//! cursor, pending reads, and fill generations; FUSE threads only exchange
//! commands with it. This makes seek/reset races impossible without spreading
//! synchronization across every state transition.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use riven_core::local_source::SourceLayout;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::source::ByteSource;

const MIB: u64 = 1024 * 1024;
/// Chunk size for origins with no natural one of their own (HTTP/debrid).
/// Usenet supplies its article size instead, via [`ByteSource::layout`].
const DEFAULT_CHUNK: u64 = 8 * MIB;
/// Chunks kept in flight ahead of the read position. Each one is its own task
/// and may take its own origin connection; the hard ceiling on connections is
/// the origin's, not this.
const WINDOW_CHUNKS: usize = 8;
/// Widened window used when playback is about to cross into another container
/// volume, where a boundary stall would otherwise be audible.
const BOUNDARY_WINDOW_CHUNKS: usize = 24;
const ARM_AFTER: u32 = 3;
const PROBING_TOLERANCE: u64 = 256 * 1024;
const FORWARD_TOLERANCE: u64 = 4 * MIB;
const BACKWARD_RETENTION: u64 = 16 * MIB;
const BACKWARD_PASSTHROUGH: u64 = 32 * MIB;
/// Reservoir built before buffered reads are released to a player.
///
/// Players initially pull much faster than the title bitrate to fill their
/// own cache. Releasing the first completed chunk lets playback begin quickly
/// but makes it catch the still-cold origin pipeline a few seconds later.
const STARTUP_BUFFER: u64 = 16 * MIB;
const HEADER_SIZE: u64 = 256 * 1024;
const MIN_FOOTER: u64 = 16 * 1024;
const MAX_FOOTER: u64 = 10 * MIB;
/// A read the player is blocked on is only interesting once it is long enough
/// to be seen. Both thresholds sit well above the cost of a single cold origin
/// chunk (a usenet article is a few hundred milliseconds by itself), so a
/// healthy cold fetch is not reported as a stall.
const SLOW_DEMAND_MS: u64 = 800;
/// Fills are speculative and overlap each other, so a slow one is only a
/// problem if it is slow enough to arrive after the cursor reaches it.
const SLOW_FILL_MS: u64 = 1_000;

fn env_positive(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Default ceiling on read-ahead memory across every open handle.
const DEFAULT_BUDGET_MB: usize = 256;

static BUDGET: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// Set the process-wide read-ahead memory ceiling. Called once at VFS
/// construction; the env var still wins so a deployment can override without
/// touching settings. Later calls are ignored.
pub fn init_budget_mb(budget_mb: usize) {
    let budget = env_positive(
        "RIVEN_VFS_READAHEAD_BUDGET_MB",
        if budget_mb > 0 {
            budget_mb
        } else {
            DEFAULT_BUDGET_MB
        },
    );
    drop(BUDGET.set(Arc::new(Semaphore::new(budget))));
}

fn global_budget() -> Arc<Semaphore> {
    BUDGET
        .get_or_init(|| {
            Arc::new(Semaphore::new(env_positive(
                "RIVEN_VFS_READAHEAD_BUDGET_MB",
                DEFAULT_BUDGET_MB,
            )))
        })
        .clone()
}

#[derive(Clone)]
struct Config {
    chunk: u64,
    window_chunks: usize,
    boundary_window_chunks: usize,
    /// Virtual offsets at which the origin starts a new container volume,
    /// ascending. Empty for origins that are one flat stream.
    boundaries: Arc<[u64]>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            chunk: DEFAULT_CHUNK,
            window_chunks: env_positive("RIVEN_VFS_READAHEAD_CHUNKS", WINDOW_CHUNKS),
            boundary_window_chunks: BOUNDARY_WINDOW_CHUNKS,
            boundaries: Arc::from([]),
        }
    }
}

impl Config {
    fn from_layout(layout: Option<SourceLayout>) -> Self {
        let defaults = Self::default();
        let Some(layout) = layout else {
            return defaults;
        };
        Self {
            chunk: layout.chunk_size.max(1),
            boundaries: Arc::from(layout.boundaries),
            ..defaults
        }
    }

    /// Chunks to keep in flight from `cursor`. Widens as the next volume
    /// boundary comes within the ordinary window.
    fn chunks_at(&self, cursor: u64) -> usize {
        let ordinary = self.chunk * self.window_chunks as u64;
        let approaching = self
            .boundaries
            .iter()
            .any(|&boundary| boundary >= cursor && boundary - cursor <= ordinary);
        if approaching {
            self.boundary_window_chunks.max(self.window_chunks)
        } else {
            self.window_chunks
        }
    }

    fn window_at(&self, cursor: u64) -> u64 {
        self.chunk * self.chunks_at(cursor) as u64
    }

    /// The largest window this handle can ever ask for — what its memory
    /// reservation has to cover, since the window widens without re-reserving.
    fn max_window(&self) -> u64 {
        let chunks = if self.boundaries.is_empty() {
            self.window_chunks
        } else {
            self.boundary_window_chunks.max(self.window_chunks)
        };
        self.chunk * chunks as u64
    }

    fn budget_mib(&self) -> u32 {
        self.max_window()
            .saturating_add(BACKWARD_RETENTION)
            .div_ceil(MIB)
            .min(u64::from(u32::MAX)) as u32
    }
}

struct Buffer {
    start: u64,
    len: u64,
    chunks: VecDeque<Bytes>,
}

impl Buffer {
    fn empty(start: u64) -> Self {
        Self {
            start,
            len: 0,
            chunks: VecDeque::new(),
        }
    }

    fn end(&self) -> u64 {
        self.start + self.len
    }

    fn reset(&mut self, start: u64) {
        self.start = start;
        self.len = 0;
        self.chunks.clear();
    }

    fn push(&mut self, bytes: Bytes) {
        self.len += bytes.len() as u64;
        self.chunks.push_back(bytes);
    }

    fn discard_before(&mut self, offset: u64) {
        let offset = offset.min(self.end());
        while self.start < offset {
            let Some(front) = self.chunks.front_mut() else {
                self.start = offset;
                self.len = 0;
                return;
            };
            let skip = (offset - self.start).min(front.len() as u64) as usize;
            if skip == front.len() {
                self.chunks.pop_front();
            } else {
                *front = front.slice(skip..);
            }
            self.start += skip as u64;
            self.len -= skip as u64;
        }
    }

    fn read(&self, start: u64, len: usize) -> Option<Bytes> {
        let end = start.checked_add(len as u64)?;
        if start < self.start || end > self.end() {
            return None;
        }

        let mut skip = (start - self.start) as usize;
        let mut remaining = len;
        let mut output = BytesMut::with_capacity(len);
        for chunk in &self.chunks {
            if skip >= chunk.len() {
                skip -= chunk.len();
                continue;
            }
            let take = remaining.min(chunk.len() - skip);
            output.extend_from_slice(&chunk[skip..skip + take]);
            remaining -= take;
            skip = 0;
            if remaining == 0 {
                return Some(output.freeze());
            }
        }
        None
    }
}

struct ReadRequest {
    start: u64,
    len: usize,
    queued_at: Instant,
    waited: bool,
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
    generation: u64,
    start: u64,
    elapsed: Duration,
    result: io::Result<Bytes>,
}

struct Direct {
    generation: u64,
    arm_after: bool,
    request: ReadRequest,
    result: io::Result<Bytes>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Probing,
    Warming,
    Streaming,
}

#[derive(Default)]
struct Stats {
    hits: u64,
    misses: u64,
    waited_ms: u64,
    worst_wait_ms: u64,
    fills: u64,
    fill_ms: u64,
    worst_fill_ms: u64,
    seeks: u64,
    probes: u64,
    behind: u64,
    bytes: u64,
    last_log: Option<Instant>,
}

/// One open file handle. All mutable read-ahead state lives in its actor task.
pub struct Prefetcher {
    commands: mpsc::UnboundedSender<Command>,
    size: u64,
}

impl Prefetcher {
    pub fn new(source: Arc<dyn ByteSource>, runtime: &Handle) -> Self {
        let size = source.size();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (fill_tx, fill_rx) = mpsc::unbounded_channel();
        let (direct_tx, direct_rx) = mpsc::unbounded_channel();
        let actor = Actor {
            source,
            config: Config::default(),
            size,
            commands: command_rx,
            fill_tx,
            fills: fill_rx,
            direct_tx,
            directs: direct_rx,
            buffer: Buffer::empty(0),
            completed: BTreeMap::new(),
            pending: Vec::new(),
            mode: Mode::Probing,
            expected_next: 0,
            seq_run: 0,
            cursor: 0,
            frontier: 0,
            generation: 0,
            inflight: 0,
            reservation: None,
            stats: Stats::default(),
        };
        runtime.spawn(actor.run());
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
            .send(Command::Read(ReadRequest {
                start,
                len,
                queued_at: Instant::now(),
                waited: false,
                reply,
            }))
            .map_err(|_send_error| {
                io::Error::new(io::ErrorKind::BrokenPipe, "read handle is closed")
            })?;
        response.await.map_err(|_receive_error| {
            io::Error::new(io::ErrorKind::BrokenPipe, "read handle is closed")
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
    config: Config,
    size: u64,
    commands: mpsc::UnboundedReceiver<Command>,
    fill_tx: mpsc::UnboundedSender<Fill>,
    fills: mpsc::UnboundedReceiver<Fill>,
    direct_tx: mpsc::UnboundedSender<Direct>,
    directs: mpsc::UnboundedReceiver<Direct>,

    buffer: Buffer,
    completed: BTreeMap<u64, Bytes>,
    pending: Vec<ReadRequest>,
    mode: Mode,
    expected_next: u64,
    seq_run: u32,
    cursor: u64,
    frontier: u64,
    generation: u64,
    inflight: usize,
    reservation: Option<OwnedSemaphorePermit>,
    stats: Stats,
}

impl Actor {
    async fn run(mut self) {
        // Resolved here rather than at construction: the FUSE open path is
        // synchronous, and discovering an origin's chunking may need I/O.
        self.config = Config::from_layout(self.source.layout().await);
        loop {
            tokio::select! {
                command = self.commands.recv() => {
                    match command {
                        Some(Command::Read(request)) => self.on_read(request),
                        #[cfg(test)]
                        Some(Command::Inspect(reply)) => {
                            drop(reply.send(self.snapshot()));
                        }
                        None => break,
                    }
                }
                Some(fill) = self.fills.recv(), if self.inflight > 0 => self.on_fill(fill),
                Some(direct) = self.directs.recv() => self.on_direct(direct),
            }
        }
    }

    fn on_read(&mut self, mut request: ReadRequest) {
        if request.start >= self.size || request.len == 0 {
            request.respond(Ok(Bytes::new()));
            return;
        }
        request.len = request.len.min((self.size - request.start) as usize);
        self.source.report_position(request.start);

        if self.is_metadata_probe(&request) {
            self.stats.probes += 1;
            self.spawn_direct(request, false);
            return;
        }

        if self.mode == Mode::Probing {
            let arm_after = self.observe_probing_read(&request);
            self.spawn_direct(request, arm_after);
            return;
        }

        if self.mode == Mode::Streaming
            && let Some(data) = self.buffer.read(request.start, request.len)
        {
            self.finish_buffered(request, data);
            self.dispatch();
            return;
        }

        if request.start < self.buffer.start
            && self.buffer.start - request.start <= BACKWARD_PASSTHROUGH
        {
            self.stats.behind += 1;
            self.spawn_direct(request, false);
            return;
        }

        let request_end = request.end();
        let waits_for_current_window = request.start >= self.buffer.start
            && request.start <= self.frontier.saturating_add(FORWARD_TOLERANCE)
            && request_end
                <= self
                    .cursor
                    .saturating_add(self.config.window_at(self.cursor));
        if waits_for_current_window {
            request.waited = true;
            self.pending.push(request);
            self.dispatch();
            return;
        }

        tracing::debug!(
            target: "streaming",
            from = self.cursor,
            to = request.start,
            "read-ahead seek; returning handle to passthrough"
        );
        self.stats.seeks += 1;
        self.demote();
        let arm_after = self.observe_probing_read(&request);
        self.spawn_direct(request, arm_after);
    }

    fn observe_probing_read(&mut self, request: &ReadRequest) -> bool {
        if request.start >= self.expected_next
            && request.start - self.expected_next <= PROBING_TOLERANCE
        {
            self.seq_run += 1;
        } else {
            self.seq_run = 1;
        }
        self.expected_next = request.end();

        self.seq_run >= ARM_AFTER && self.size > self.config.window_at(0)
    }

    fn promote(&mut self, start: u64) {
        if self.mode != Mode::Probing {
            return;
        }
        let Ok(permit) = global_budget().try_acquire_many_owned(self.config.budget_mib()) else {
            return;
        };

        self.mode = Mode::Warming;
        self.generation = self.generation.wrapping_add(1);
        self.cursor = start;
        self.frontier = self.cursor;
        self.buffer.reset(self.cursor);
        self.completed.clear();
        self.reservation = Some(permit);
        self.dispatch();
    }

    fn demote(&mut self) {
        self.mode = Mode::Probing;
        self.generation = self.generation.wrapping_add(1);
        self.seq_run = 0;
        self.buffer.reset(0);
        self.completed.clear();
        self.reservation = None;

        for request in std::mem::take(&mut self.pending) {
            self.spawn_direct(request, false);
        }
    }

    fn dispatch(&mut self) {
        while self.mode != Mode::Probing
            && self.inflight < self.config.chunks_at(self.cursor)
            && self.frontier < self.size
            && self.frontier.saturating_sub(self.cursor) < self.config.window_at(self.cursor)
        {
            let start = self.frontier;
            // The boundary has to be strictly ahead of `start`. `next_multiple_of`
            // returns `start` unchanged when it is already on one — and every fill
            // after the first ends on a boundary, so that was the common case. The
            // old `.max(start + 1)` then turned it into a one-byte fill, which
            // still costs a whole article fetch and an in-flight slot: each chunk
            // burned two slots and read-ahead ran at half its configured depth.
            let boundary = start
                .checked_add(self.config.chunk - start % self.config.chunk)
                .unwrap_or(self.size);
            let end = boundary
                .min(
                    self.cursor
                        .saturating_add(self.config.window_at(self.cursor)),
                )
                .min(self.size);
            if end <= start {
                break;
            }
            self.frontier = end;
            self.inflight += 1;

            let source = Arc::clone(&self.source);
            let sender = self.fill_tx.clone();
            let generation = self.generation;
            let size = self.size;
            tokio::spawn(async move {
                let started = Instant::now();
                let result = read_exact(source, size, start, (end - start) as usize).await;
                drop(sender.send(Fill {
                    generation,
                    start,
                    elapsed: started.elapsed(),
                    result,
                }));
            });
        }
    }

    fn on_fill(&mut self, fill: Fill) {
        self.inflight = self.inflight.saturating_sub(1);
        if fill.generation != self.generation || self.mode == Mode::Probing {
            self.dispatch();
            return;
        }

        let elapsed_ms = fill.elapsed.as_millis() as u64;
        self.stats.fills += 1;
        self.stats.fill_ms += elapsed_ms;
        self.stats.worst_fill_ms = self.stats.worst_fill_ms.max(elapsed_ms);
        if elapsed_ms >= SLOW_FILL_MS {
            // Not a stutter on its own — read-ahead runs ahead of the player.
            // `ahead_mb` is what says whether it had room to be late.
            tracing::warn!(
                target: "streaming",
                start = fill.start,
                ahead_mb = fill.start.saturating_sub(self.cursor) >> 20,
                inflight = self.inflight,
                elapsed_ms,
                "slow read-ahead fill"
            );
        }

        match fill.result {
            Ok(data) => {
                self.completed.insert(fill.start, data);
                while let Some(data) = self.completed.remove(&self.buffer.end()) {
                    self.buffer.push(data);
                }
                if self.mode == Mode::Warming && self.startup_ready() {
                    self.mode = Mode::Streaming;
                }
                self.serve_pending();
                self.dispatch();
            }
            Err(error) => {
                tracing::warn!(
                    target: "streaming",
                    start = fill.start,
                    %error,
                    "read-ahead fill failed; retrying demand reads directly"
                );
                self.demote();
            }
        }
    }

    fn serve_pending(&mut self) {
        if self.mode != Mode::Streaming {
            return;
        }
        let mut waiting = Vec::with_capacity(self.pending.len());
        for request in std::mem::take(&mut self.pending) {
            if let Some(data) = self.buffer.read(request.start, request.len) {
                self.finish_buffered(request, data);
            } else if request.start < self.buffer.start {
                self.spawn_direct(request, false);
            } else {
                waiting.push(request);
            }
        }
        self.pending = waiting;
        self.evict_consumed();
    }

    fn finish_buffered(&mut self, request: ReadRequest, data: Bytes) {
        let waited_ms = request.queued_at.elapsed().as_millis() as u64;
        if request.waited {
            self.stats.misses += 1;
            self.stats.waited_ms += waited_ms;
            self.stats.worst_wait_ms = self.stats.worst_wait_ms.max(waited_ms);
            self.log_slow_demand(&request, "buffer");
        } else {
            self.stats.hits += 1;
        }
        self.stats.bytes += data.len() as u64;
        self.cursor = self.cursor.max(request.start + data.len() as u64);
        self.expected_next = self.cursor;
        request.respond(Ok(data));
        self.evict_consumed();
        self.maybe_log();
    }

    fn evict_consumed(&mut self) {
        let mut keep_from = self.cursor.saturating_sub(BACKWARD_RETENTION);
        if let Some(oldest_waiter) = self.pending.iter().map(|request| request.start).min() {
            keep_from = keep_from.min(oldest_waiter);
        }
        self.buffer.discard_before(keep_from);
    }

    fn spawn_direct(&self, request: ReadRequest, arm_after: bool) {
        let source = Arc::clone(&self.source);
        let sender = self.direct_tx.clone();
        let size = self.size;
        let generation = self.generation;
        tokio::spawn(async move {
            let result = read_exact(source, size, request.start, request.len).await;
            drop(sender.send(Direct {
                generation,
                arm_after,
                request,
                result,
            }));
        });
    }

    fn on_direct(&mut self, direct: Direct) {
        let succeeded = direct.result.as_ref().is_ok_and(|data| !data.is_empty());
        let promote_from = self.expected_next.max(direct.request.end());
        self.log_slow_demand(&direct.request, "passthrough");
        direct.request.respond(direct.result);

        // Starting six speculative fills while the read that proved the
        // stream is still waiting lets read-ahead starve demand during cold
        // start. Promote only after that demand read has completed.
        if direct.arm_after
            && succeeded
            && direct.generation == self.generation
            && self.mode == Mode::Probing
        {
            self.promote(promote_from);
        }
    }

    fn startup_ready(&self) -> bool {
        let remaining = self.size.saturating_sub(self.cursor);
        let target = self
            .config
            .window_at(self.cursor)
            .min(STARTUP_BUFFER)
            .min(remaining);
        self.buffer.len >= target || self.buffer.end() >= self.size
    }

    fn is_metadata_probe(&self, request: &ReadRequest) -> bool {
        let footer_size = (self.size / 50).clamp(MIN_FOOTER, MAX_FOOTER);
        request.end() <= HEADER_SIZE.min(self.size)
            || request.start >= self.size.saturating_sub(footer_size)
    }

    /// Warn about a read a player was actually blocked on. `served_from`
    /// separates the two causes that need different fixes: `passthrough` is the
    /// origin itself being slow, `buffer` is read-ahead that fell behind the
    /// cursor.
    fn log_slow_demand(&self, request: &ReadRequest, served_from: &'static str) {
        let waited_ms = request.queued_at.elapsed().as_millis() as u64;
        if waited_ms < SLOW_DEMAND_MS {
            return;
        }
        tracing::warn!(
            target: "streaming",
            offset = request.start,
            len = request.len,
            served_from,
            phase = ?self.mode,
            waited_ms,
            "slow playback read"
        );
    }

    fn maybe_log(&mut self) {
        let now = Instant::now();
        if self
            .stats
            .last_log
            .is_some_and(|last| now.duration_since(last) < Duration::from_secs(10))
        {
            return;
        }
        self.stats.last_log = Some(now);
        let reads = self.stats.hits + self.stats.misses;
        tracing::info!(
            target: "streaming",
            phase = ?self.mode,
            position_mb = self.cursor >> 20,
            window_mb = self.config.window_at(self.cursor) >> 20,
            buffered_mb = self.buffer.len >> 20,
            inflight = self.inflight,
            pending = self.pending.len(),
            reads,
            hit_pct = self.stats.hits * 100 / reads.max(1),
            misses = self.stats.misses,
            avg_wait_ms = self.stats.waited_ms / self.stats.misses.max(1),
            worst_wait_ms = self.stats.worst_wait_ms,
            fills = self.stats.fills,
            avg_fill_ms = self.stats.fill_ms / self.stats.fills.max(1),
            worst_fill_ms = self.stats.worst_fill_ms,
            seeks = self.stats.seeks,
            probes = self.stats.probes,
            behind = self.stats.behind,
            served_mb = self.stats.bytes >> 20,
            "read-ahead stats"
        );
    }

    #[cfg(test)]
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            streaming: self.mode != Mode::Probing,
            warming: self.mode == Mode::Warming,
            generation: self.generation,
            buffer_start: self.buffer.start,
            buffer_end: self.buffer.end(),
            inflight: self.inflight,
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
    let len = len.min((file_size - start) as usize);
    let end = start + len as u64;
    let mut position = start;
    let mut output = BytesMut::with_capacity(len);

    while position < end {
        let data = source.read_range(position, end - 1).await?;
        if data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("origin returned no data at offset {position} before file end"),
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
    streaming: bool,
    warming: bool,
    generation: u64,
    buffer_start: u64,
    buffer_end: u64,
    inflight: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Notify, Semaphore};

    struct Source {
        size: u64,
        cap: usize,
    }

    #[async_trait::async_trait]
    impl ByteSource for Source {
        async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
            let want = (end - start + 1) as usize;
            Ok(Bytes::from(vec![b'x'; want.min(self.cap)]))
        }

        fn size(&self) -> u64 {
            self.size
        }
    }

    /// Every prefetcher reserves its window from one process-wide budget that
    /// the whole test binary shares, so tests running in parallel would
    /// otherwise starve each other out of read-ahead entirely.
    fn unbounded_budget() {
        init_budget_mb(1024 * 1024);
    }

    fn prefetcher(size: u64, cap: usize) -> Prefetcher {
        unbounded_budget();
        Prefetcher::new(Arc::new(Source { size, cap }), &Handle::current())
    }

    #[tokio::test]
    async fn fills_mid_file_short_reads() {
        let reader = prefetcher(128 * MIB, 4096);
        let data = reader.read(MIB, 128 * 1024).await.unwrap();
        assert_eq!(data.len(), 128 * 1024);
    }

    #[tokio::test]
    async fn only_eof_may_be_short() {
        let size = 100 * 1024;
        let reader = prefetcher(size, 4096);
        assert_eq!(
            reader.read(size - 1000, 128 * 1024).await.unwrap().len(),
            1000
        );
        assert!(reader.read(size, 4096).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn empty_mid_file_response_is_an_error() {
        let reader = prefetcher(128 * MIB, 0);
        let error = reader.read(MIB, 4096).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn scattered_reads_do_not_arm_read_ahead() {
        let reader = prefetcher(512 * MIB, MIB as usize);
        for offset in [MIB, 100 * MIB, 200 * MIB] {
            reader.read(offset, 128 * 1024).await.unwrap();
        }
        assert!(!reader.snapshot().await.streaming);
    }

    #[tokio::test]
    async fn sequential_reads_arm_one_fixed_window() {
        let reader = prefetcher(512 * MIB, MIB as usize);
        let mut offset = MIB;
        for _ in 0..ARM_AFTER {
            reader.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }
        let snapshot = reader.snapshot().await;
        assert!(snapshot.streaming);
        assert!(snapshot.inflight > 0 || snapshot.buffer_end > snapshot.buffer_start);
    }

    /// Records every range the origin is asked for, so a test can assert on the
    /// shape of read-ahead rather than only on its results.
    struct RecordingSource {
        size: u64,
        chunk: u64,
        lengths: Mutex<Vec<u64>>,
    }

    #[async_trait::async_trait]
    impl ByteSource for RecordingSource {
        async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
            let len = end - start + 1;
            self.lengths.lock().unwrap().push(len);
            Ok(Bytes::from(vec![b'x'; len as usize]))
        }

        fn size(&self) -> u64 {
            self.size
        }

        async fn layout(&self) -> Option<SourceLayout> {
            Some(SourceLayout {
                chunk_size: self.chunk,
                boundaries: Vec::new(),
            })
        }
    }

    /// A frontier sitting exactly on a chunk boundary must advance by a whole
    /// chunk. It used to advance by one byte, so every chunk cost two fills —
    /// one of them a full origin fetch for a single byte — and read-ahead ran
    /// at half the depth it was configured for.
    #[tokio::test]
    async fn boundary_aligned_fills_span_whole_chunks() {
        let chunk = 700 * 1024;
        let source = Arc::new(RecordingSource {
            size: 512 * MIB,
            chunk,
            lengths: Mutex::new(Vec::new()),
        });
        unbounded_budget();
        let reader = Prefetcher::new(source.clone(), &Handle::current());

        let mut offset = MIB;
        for _ in 0..ARM_AFTER {
            reader.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }
        // Let the armed window issue its fills.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }

        let lengths = source.lengths.lock().unwrap().clone();
        let fills: Vec<u64> = lengths
            .iter()
            .copied()
            .filter(|len| *len > 128 * 1024)
            .collect();
        assert!(!fills.is_empty(), "read-ahead never dispatched a fill");
        assert!(
            lengths.iter().all(|len| *len != 1),
            "one-byte fill dispatched: {lengths:?}"
        );
        // Only the very first fill may be partial, squaring the armed cursor up
        // with a chunk boundary. Fills complete out of order, so this counts
        // rather than indexes.
        let partial = fills.iter().filter(|len| **len != chunk).count();
        assert!(
            partial <= 1,
            "only the initial alignment fill may be partial: {fills:?}"
        );
    }

    struct GatedProbeSource {
        size: u64,
        calls: AtomicUsize,
        third_started: Notify,
        third_gate: Semaphore,
        fill_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ByteSource for GatedProbeSource {
        async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
            let len = (end - start + 1) as usize;
            if len > 128 * 1024 {
                self.fill_calls.fetch_add(1, Ordering::SeqCst);
            } else if self.calls.fetch_add(1, Ordering::SeqCst) == 2 {
                self.third_started.notify_one();
                self.third_gate.acquire().await.unwrap().forget();
            }
            Ok(Bytes::from(vec![b'x'; len]))
        }

        fn size(&self) -> u64 {
            self.size
        }
    }

    #[tokio::test]
    async fn proving_read_finishes_before_read_ahead_starts() {
        let source = Arc::new(GatedProbeSource {
            size: 512 * MIB,
            calls: AtomicUsize::new(0),
            third_started: Notify::new(),
            third_gate: Semaphore::new(0),
            fill_calls: AtomicUsize::new(0),
        });
        unbounded_budget();
        let reader = Arc::new(Prefetcher::new(source.clone(), &Handle::current()));
        reader.read(MIB, 128 * 1024).await.unwrap();
        reader.read(MIB + 128 * 1024, 128 * 1024).await.unwrap();

        let third = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(MIB + 256 * 1024, 128 * 1024).await })
        };
        source.third_started.notified().await;
        assert!(!reader.snapshot().await.streaming);
        assert_eq!(source.fill_calls.load(Ordering::SeqCst), 0);

        source.third_gate.add_permits(1);
        third.await.unwrap().unwrap();
        let snapshot = reader.snapshot().await;
        assert!(snapshot.streaming);
        assert!(snapshot.inflight > 0 || snapshot.buffer_end > snapshot.buffer_start);
    }

    struct StartupSource {
        size: u64,
        first_started: Notify,
        first_gate: Semaphore,
        later_gate: Semaphore,
    }

    #[async_trait::async_trait]
    impl ByteSource for StartupSource {
        async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
            let len = (end - start + 1) as usize;
            if len > 128 * 1024 {
                if start < 8 * MIB {
                    self.first_started.notify_one();
                    self.first_gate.acquire().await.unwrap().forget();
                } else {
                    self.later_gate.acquire().await.unwrap().forget();
                }
            }
            Ok(Bytes::from(vec![b'x'; len]))
        }

        fn size(&self) -> u64 {
            self.size
        }
    }

    #[tokio::test]
    async fn warming_holds_reads_until_startup_reservoir_is_ready() {
        let source = Arc::new(StartupSource {
            size: 512 * MIB,
            first_started: Notify::new(),
            first_gate: Semaphore::new(0),
            later_gate: Semaphore::new(0),
        });
        unbounded_budget();
        let reader = Arc::new(Prefetcher::new(source.clone(), &Handle::current()));
        let mut offset = MIB;
        for _ in 0..ARM_AFTER {
            reader.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }

        source.first_started.notified().await;
        let waiting = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(offset, 128 * 1024).await })
        };
        source.first_gate.add_permits(1);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        assert!(reader.snapshot().await.warming);
        assert!(
            !waiting.is_finished(),
            "the first chunk must not start playback before the reservoir is full"
        );

        source.later_gate.add_permits(WINDOW_CHUNKS);
        waiting.await.unwrap().unwrap();
        assert!(!reader.snapshot().await.warming);
    }

    #[tokio::test]
    async fn seek_starts_a_new_generation() {
        let reader = prefetcher(512 * MIB, MIB as usize);
        let mut offset = MIB;
        for _ in 0..ARM_AFTER {
            reader.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }
        let armed = reader.snapshot().await;

        reader.read(400 * MIB, 128 * 1024).await.unwrap();
        let sought = reader.snapshot().await;
        assert!(!sought.streaming);
        assert!(sought.generation > armed.generation);
        assert_eq!(sought.buffer_start, 0);
        assert_eq!(sought.buffer_end, 0);
    }

    #[tokio::test]
    async fn interleaved_read_behind_retention_does_not_demote() {
        let reader = prefetcher(512 * MIB, MIB as usize);
        let mut offset = MIB;
        for _ in 0..100 {
            reader.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }
        let before = reader.snapshot().await;
        assert!(before.streaming);
        assert!(before.buffer_start > MIB);

        reader
            .read(before.buffer_start - MIB, 128 * 1024)
            .await
            .unwrap();
        let after = reader.snapshot().await;
        assert!(after.streaming);
        assert_eq!(after.generation, before.generation);
    }

    struct GatedSource {
        size: u64,
        fill_started: Notify,
        fill_gate: Semaphore,
    }

    #[async_trait::async_trait]
    impl ByteSource for GatedSource {
        async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
            let len = (end - start + 1) as usize;
            if len > 128 * 1024 && start < 100 * MIB {
                self.fill_started.notify_one();
                self.fill_gate.acquire().await.unwrap().forget();
            }
            Ok(Bytes::from(vec![b'x'; len]))
        }

        fn size(&self) -> u64 {
            self.size
        }
    }

    #[tokio::test]
    async fn stale_fill_cannot_enter_a_new_seek_generation() {
        let source = Arc::new(GatedSource {
            size: 512 * MIB,
            fill_started: Notify::new(),
            fill_gate: Semaphore::new(0),
        });
        unbounded_budget();
        let reader = Prefetcher::new(source.clone(), &Handle::current());

        let mut offset = MIB;
        for _ in 0..ARM_AFTER {
            reader.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }
        source.fill_started.notified().await;
        let first_generation = reader.snapshot().await.generation;

        let mut seek = 400 * MIB;
        for _ in 0..ARM_AFTER {
            reader.read(seek, 128 * 1024).await.unwrap();
            seek += 128 * 1024;
        }
        let second_generation = reader.snapshot().await.generation;
        assert!(second_generation > first_generation);

        source.fill_gate.add_permits(WINDOW_CHUNKS);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let snapshot = reader.snapshot().await;
        assert!(snapshot.streaming);
        assert!(
            snapshot.buffer_start >= 400 * MIB,
            "old-generation bytes entered the new buffer: {snapshot:?}"
        );
    }

    #[tokio::test]
    async fn concurrent_kernel_reads_complete_out_of_order() {
        let reader = Arc::new(prefetcher(512 * MIB, MIB as usize));
        let mut offset = MIB;
        for _ in 0..ARM_AFTER {
            reader.read(offset, 128 * 1024).await.unwrap();
            offset += 128 * 1024;
        }

        let reads = (0..32).map(|index| {
            let reader = reader.clone();
            async move {
                reader
                    .read(offset + index * 128 * 1024, 128 * 1024)
                    .await
                    .map(|data| data.len())
            }
        });
        let lengths =
            tokio::time::timeout(Duration::from_secs(1), futures::future::join_all(reads))
                .await
                .expect("concurrent reads must not deadlock");
        assert!(
            lengths
                .into_iter()
                .all(|result| matches!(result, Ok(131_072)))
        );
    }

    #[test]
    fn buffer_retains_out_of_order_reads_without_advancing() {
        let mut buffer = Buffer::empty(100);
        buffer.push(Bytes::from_static(b"abcdefghijkl"));
        assert_eq!(&buffer.read(104, 4).unwrap()[..], b"efgh");
        assert_eq!(&buffer.read(100, 4).unwrap()[..], b"abcd");
        assert_eq!(buffer.start, 100);
    }
}
