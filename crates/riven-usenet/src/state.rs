//! Process-global mutable state for the streamer: deserialized NzbMeta
//! cache, decoded-segment-size memoization, permanent-fail tracking, and
//! the active-streams registry.
//!
//! All entries are keyed in a way that's stable across requests (info_hash,
//! message_id) so the same instance is reused by the ingest path and the
//! read path inside the same process.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use lru::LruCache;
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};

use crate::cache::SegmentCache;

/// Default decoded-segment cache budget. Overridable via env var.
/// Size up linearly with concurrent stream count: each stream needs ~10-20 MB
/// of warm segments. Default 256 MB ≈ 12 concurrent streams.
const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// Default budget for the deserialized-meta cache. Each `NzbMeta` holds the
/// full per-segment address book (message-ids + offsets) for one file, so a
/// big remux can be tens of MB while a TV episode is a few hundred KB. A
/// library scan touches every ingested file, so without a bound the cache
/// grew to hold *all* of them (observed ~2 GB resident with ~1,200 files).
/// 256 MB keeps a healthy working set of recently-streamed files hot;
/// cold ones re-load from Postgres on the next access. Override with
/// `RIVEN_USENET_META_CACHE_BYTES`.
const DEFAULT_META_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// Default cap on the decoded-segment-size memo. One `(message_id, u64)`
/// entry per RAR segment ever fetched; ~80 bytes each, so 500k entries is
/// ~40 MB. Override with `RIVEN_USENET_DECODED_SIZES_ENTRIES`.
const DEFAULT_DECODED_SIZES_ENTRIES: usize = 500_000;

/// Default cap on concurrent head/tail precache operations. Each warms
/// ~8 MB (head + tail) with internal prefetch fan-out, so a handful in
/// flight is plenty to keep playback probes hot without letting a library
/// scan spawn hundreds of simultaneous fetch+decode pipelines.
const DEFAULT_PRECACHE_CONCURRENCY: usize = 4;

/// Floor for ingest concurrency when the connection budget is tiny or
/// unknown — preserves the historical default so small setups behave as before.
pub(crate) const MIN_INGEST_CONCURRENCY: usize = 4;

/// Derive the number of NZBs that may ingest concurrently from the NNTP
/// connection budget (`max_connections` summed across primary providers).
///
/// Ingest is gated separately from streaming so a backlog of new releases
/// can't monopolise the provider and stall playback. Rather than a fixed
/// cap (which left large connection allowances idle — 4 ingests against a
/// 100-connection account), we take half the budget: enough to drain a
/// scrape backlog quickly while leaving the other half as headroom for
/// streaming, which already preempts ingest via NNTP `Priority`. The pool's
/// own `PrioritizedSemaphore(max_connections)` stays the hard ceiling, so
/// this can never oversubscribe the provider. `RIVEN_USENET_INGEST_CONCURRENCY`
/// overrides the derived value for manual tuning.
pub fn ingest_concurrency_for(total_capacity: usize) -> usize {
    env_positive(
        "RIVEN_USENET_INGEST_CONCURRENCY",
        (total_capacity / 2).max(MIN_INGEST_CONCURRENCY),
    )
}

/// Aggregated process-wide state shared by every `UsenetStreamer`
/// instance. Sharing means RAR header bytes fetched at ingest time stay
/// hot for subsequent read-path serves, and a single in-flight fetch
/// deduplicates across all concerns.
pub struct StreamerState {
    pub cache: SegmentCache,
    pub meta_cache: MetaCache,
    pub decoded_sizes: DecodedSizes,
    pub fails: PermanentFails,
    pub in_flight: InFlight,
    pub precached: PrecachedFiles,
    pub migrated: MigratedMetas,
    /// Cumulative NNTP fetch counters (cache misses that hit the wire),
    /// driving the API's usenet-streaming health view.
    pub fetch_metrics: FetchMetrics,
    /// Caps concurrent head/tail precache operations. A Plex/Jellyfin
    /// library scan HEADs hundreds of files in a burst, each of which
    /// fires a fire-and-forget `precache_head_tail`. Without a limit
    /// that's hundreds of simultaneous 8 MB fetch+decode pipelines —
    /// a multi-GB allocation spike that musl never returns to the OS
    /// (RSS observed climbing to ~3.5 GB and holding). Bounding peak
    /// precache concurrency caps that high-water mark; the trade-off is
    /// only that head/tail warming during a mass scan happens a few
    /// files at a time. `RIVEN_USENET_PRECACHE_CONCURRENCY` overrides.
    pub precache_sem: tokio::sync::Semaphore,
}

impl StreamerState {
    fn new() -> Self {
        let cache_bytes = env_positive("RIVEN_USENET_CACHE_BYTES", DEFAULT_CACHE_BYTES);
        let meta_cache_bytes =
            env_positive("RIVEN_USENET_META_CACHE_BYTES", DEFAULT_META_CACHE_BYTES);
        let decoded_sizes_entries = env_positive(
            "RIVEN_USENET_DECODED_SIZES_ENTRIES",
            DEFAULT_DECODED_SIZES_ENTRIES,
        );
        let precache_concurrency = env_positive(
            "RIVEN_USENET_PRECACHE_CONCURRENCY",
            DEFAULT_PRECACHE_CONCURRENCY,
        );
        Self {
            cache: SegmentCache::new(cache_bytes),
            meta_cache: MetaCache::new(meta_cache_bytes),
            decoded_sizes: DecodedSizes::new(decoded_sizes_entries),
            precache_sem: tokio::sync::Semaphore::new(precache_concurrency),
            fails: PermanentFails::default(),
            in_flight: InFlight::default(),
            precached: PrecachedFiles::default(),
            migrated: MigratedMetas::default(),
            fetch_metrics: FetchMetrics::default(),
        }
    }

    pub fn global() -> Arc<Self> {
        static C: OnceLock<Arc<StreamerState>> = OnceLock::new();
        C.get_or_init(|| Arc::new(Self::new())).clone()
    }
}

/// Cumulative counters for NNTP segment fetches that actually hit the wire
/// (i.e. cache misses). Atomic + lock-free; the API reads them to derive a
/// fetch success rate and decode throughput by sampling deltas over time.
#[derive(Default)]
pub struct FetchMetrics {
    ok: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicU64,
    bytes_decoded: std::sync::atomic::AtomicU64,
}

impl FetchMetrics {
    pub fn record_ok(&self, decoded_bytes: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.ok.fetch_add(1, Relaxed);
        self.bytes_decoded.fetch_add(decoded_bytes, Relaxed);
    }

    pub fn record_failed(&self) {
        self.failed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn ok(&self) -> u64 {
        self.ok.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn failed(&self) -> u64 {
        self.failed.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn bytes_decoded(&self) -> u64 {
        self.bytes_decoded.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub fn global_active_streams() -> Arc<ActiveStreams> {
    static C: OnceLock<Arc<ActiveStreams>> = OnceLock::new();
    C.get_or_init(|| Arc::new(ActiveStreams::default())).clone()
}

/// Read a positive number from an env var, falling back to `default` when
/// unset or unparseable. Zero is treated as "use default" to avoid an
/// accidental empty cache or zero concurrency.
fn env_positive<T: std::str::FromStr + Default + PartialOrd>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<T>().ok())
        .filter(|n| *n > T::default())
        .unwrap_or(default)
}

/// Deserialized metadata cache. Eliminates a per-read Postgres round-trip +
/// JSON deserialization on the streaming hot path. Bounded by an estimate
/// of each entry's deserialized footprint (dominated by the per-segment
/// message-id strings) and evicted LRU — a library scan that touches every
/// ingested file can no longer pin all of them in memory at once. Cold
/// files re-load from Postgres on the next access; an in-flight stream is
/// unaffected because it holds its own `Arc<NzbMeta>` for the duration of
/// each read.
pub struct MetaCache {
    state: Mutex<MetaCacheState>,
    max_bytes: u64,
}

struct MetaCacheState {
    /// Value carries the entry's estimated weight so eviction accounting
    /// doesn't have to re-walk a (potentially 200k-segment) meta.
    lru: LruCache<String, (Arc<crate::streamer::NzbMeta>, u64)>,
    current_bytes: u64,
}

impl MetaCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            state: Mutex::new(MetaCacheState {
                lru: LruCache::unbounded(),
                current_bytes: 0,
            }),
            max_bytes,
        }
    }

    pub fn get(&self, info_hash: &str) -> Option<Arc<crate::streamer::NzbMeta>> {
        let mut state = self.state.lock();
        state.lru.get(info_hash).map(|(meta, _)| meta.clone())
    }

    pub fn put(&self, info_hash: String, meta: Arc<crate::streamer::NzbMeta>) {
        let weight = estimate_meta_bytes(&meta);
        let mut state = self.state.lock();
        if let Some((_, prev_weight)) = state.lru.put(info_hash, (meta, weight)) {
            state.current_bytes = state.current_bytes.saturating_sub(prev_weight);
        }
        state.current_bytes = state.current_bytes.saturating_add(weight);

        // Evict least-recently-used entries until under budget, but always
        // keep at least the just-inserted entry — a single meta larger than
        // the whole budget (e.g. a 40 MB remux address book) must still be
        // cached for the stream that just requested it.
        while state.current_bytes > self.max_bytes && state.lru.len() > 1 {
            let Some((_, (_, popped_weight))) = state.lru.pop_lru() else {
                break;
            };
            state.current_bytes = state.current_bytes.saturating_sub(popped_weight);
        }
    }

    #[cfg(test)]
    pub fn current_bytes(&self) -> u64 {
        self.state.lock().current_bytes
    }

    #[cfg(test)]
    pub fn entry_count(&self) -> usize {
        self.state.lock().lru.len()
    }
}

/// Estimate the heap footprint of a deserialized `NzbMeta`. Dominated by
/// the per-segment `message_id` strings plus the fixed-size segment/offset
/// vectors. Walks every segment once — cheap relative to the deserialize
/// that just produced the meta, and only runs on cache insert.
fn estimate_meta_bytes(meta: &crate::streamer::NzbMeta) -> u64 {
    use crate::streamer::NzbMetaSource;
    let seg = std::mem::size_of::<crate::nzb::NzbSegment>();
    let mut bytes = 0u64;
    for file in &meta.files {
        match &file.source {
            NzbMetaSource::Direct { offsets, segments } => {
                bytes += (offsets.len() * 8) as u64;
                for s in segments {
                    bytes += (seg + s.message_id.len()) as u64;
                }
            }
            NzbMetaSource::Rar { parts, slices } => {
                for p in parts {
                    bytes += (p.offsets.len() * 8) as u64;
                    for s in &p.segments {
                        bytes += (seg + s.message_id.len()) as u64;
                    }
                }
                bytes += (slices.len() * std::mem::size_of::<crate::streamer::NzbRarSlice>()) as u64;
            }
        }
    }
    bytes.max(1)
}

/// Memoized decoded size of NNTP segments keyed by message-id. Populated as
/// segments are fetched. Lets us know "this segment is N decoded bytes"
/// without re-fetching — required to binary-search into the middle of a
/// part when serving a random seek. LRU-bounded so it can't grow without
/// limit across many RAR files; an evicted entry just forces the read path
/// to fall back to the segment walk (correct, slightly slower).
pub struct DecodedSizes {
    inner: Mutex<LruCache<Arc<str>, u64>>,
}

impl DecodedSizes {
    pub fn new(max_entries: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    pub fn get(&self, message_id: &str) -> Option<u64> {
        self.inner.lock().get(message_id).copied()
    }

    pub fn put(&self, message_id: Arc<str>, size: u64) {
        self.inner.lock().put(message_id, size);
    }
}

/// Coordinates concurrent fetches of the same segment. Without this, the
/// body stream and the optional eager prefetch can both issue an NNTP
/// `BODY` for the same message-id; with it, the second caller waits on
/// the first's promise and then reads from the segment cache.
///
/// Race-free against the classic Notify pitfall (`notify_waiters()`
/// doesn't store a permit, so a waiter that registers after the call
/// would deadlock) via a `done: AtomicBool` flag checked AFTER the
/// `Notified` future is registered via `enable()`.
#[derive(Default)]
pub struct InFlight {
    inner: Mutex<HashMap<Arc<str>, Arc<PromiseSlot>>>,
}

#[derive(Default)]
pub struct PromiseSlot {
    pub done: AtomicBool,
    pub notify: Notify,
}

impl PromiseSlot {
    /// Wait for the slot's Owner to complete. Returns once `mark_done` has
    /// been called.
    ///
    /// Pattern: register the waker via `Notified::enable()` BEFORE reading
    /// the flag. That way, if `mark_done` lands between the registration
    /// and the await, the waker is already armed; the next .await wakes
    /// immediately. If `mark_done` already ran before `enable()`, the
    /// flag check returns early without awaiting.
    pub async fn wait(self: &Arc<Self>) {
        let mut fut = std::pin::pin!(self.notify.notified());
        fut.as_mut().enable();
        if self.done.load(Ordering::Acquire) {
            return;
        }
        fut.await;
    }

    pub fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

pub enum FetchEntry {
    /// You are the first caller — perform the fetch, then call
    /// `finish(message_id, &slot)` to release waiters. The `Arc<str>` is the
    /// shared message-id key: reuse it for `cache.put`/`decoded_sizes.put` so
    /// the cold-fetch path allocates the id exactly once.
    Owner(Arc<PromiseSlot>, Arc<str>),
    /// Another caller is already fetching this message-id. Await the
    /// slot, then re-check the segment cache.
    Wait(Arc<PromiseSlot>),
}

impl InFlight {
    pub fn enter_or_wait(&self, message_id: &str) -> FetchEntry {
        let mut map = self.inner.lock();
        if let Some(slot) = map.get(message_id) {
            FetchEntry::Wait(slot.clone())
        } else {
            let key: Arc<str> = Arc::from(message_id);
            let slot = Arc::new(PromiseSlot::default());
            map.insert(key.clone(), slot.clone());
            FetchEntry::Owner(slot, key)
        }
    }

    pub fn finish(&self, message_id: &str, slot: &Arc<PromiseSlot>) {
        // Mark done BEFORE removing from map. A new caller arriving in the
        // gap would become their own Owner and re-fetch — wasteful but correct.
        slot.mark_done();
        self.inner.lock().remove(message_id);
    }

    /// Segments currently being fetched + decoded (de-dup in flight).
    /// Telemetry-only; no `is_empty` companion is needed.
    #[expect(clippy::len_without_is_empty, reason = "telemetry-only counter; emptiness is never queried")]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }
}

/// Tracks segments that we know are permanently missing on the provider
/// (NNTP `430 No such article`). Repeated reads short-circuit instead of
/// re-spending the round-trip.
#[derive(Default)]
pub struct PermanentFails {
    inner: Mutex<HashSet<String>>,
}

impl PermanentFails {
    pub fn is_dead(&self, message_id: &str) -> bool {
        self.inner.lock().contains(message_id)
    }

    pub fn mark_dead(&self, message_id: String) {
        self.inner.lock().insert(message_id);
    }

    /// Segments known to be permanently missing on every provider.
    /// Telemetry-only; no `is_empty` companion is needed.
    #[expect(clippy::len_without_is_empty, reason = "telemetry-only counter; emptiness is never queried")]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }
}

#[derive(Debug, Clone)]
pub struct DeadSegmentEvent {
    pub info_hash: String,
    pub file_index: usize,
    pub detail: String,
}

struct DeadSegmentChannel {
    tx: mpsc::UnboundedSender<DeadSegmentEvent>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<DeadSegmentEvent>>>,
    claimed: Mutex<HashSet<String>>,
}

fn dead_segment_channel() -> &'static DeadSegmentChannel {
    static C: OnceLock<DeadSegmentChannel> = OnceLock::new();
    C.get_or_init(|| {
        let (tx, rx) = mpsc::unbounded_channel();
        DeadSegmentChannel {
            tx,
            rx: Mutex::new(Some(rx)),
            claimed: Mutex::new(HashSet::new()),
        }
    })
}

pub fn report_dead_segment(info_hash: &str, file_index: usize, detail: &str) {
    let ch = dead_segment_channel();
    let key = format!("{info_hash}:{file_index}");
    if !ch.claimed.lock().insert(key) {
        return;
    }
    drop(ch.tx.send(DeadSegmentEvent {
        info_hash: info_hash.to_string(),
        file_index,
        detail: detail.to_string(),
    }));
}

pub fn take_dead_segment_receiver() -> Option<mpsc::UnboundedReceiver<DeadSegmentEvent>> {
    dead_segment_channel().rx.lock().take()
}

/// Tracks files for which the head+tail precache has already been
/// kicked off in this process. The first stream request for a file
/// eagerly warms the first and last few MB so probes and seek-to-end
/// hits are served from cache.
#[derive(Default)]
pub struct PrecachedFiles {
    inner: Mutex<HashSet<String>>,
}

impl PrecachedFiles {
    /// Returns `true` exactly once per `(info_hash, file_index)` pair —
    /// the caller is responsible for actually performing the precache.
    /// Subsequent callers see `false` and skip.
    pub fn claim(&self, info_hash: &str, file_index: usize) -> bool {
        let key = format!("{info_hash}:{file_index}");
        self.inner.lock().insert(key)
    }
}

/// Tracks NzbMeta instances for which the in-place backfill of
/// `decoded_seg_size` (for old metas ingested before that field existed)
/// has been started. Single-shot per `info_hash` per process.
#[derive(Default)]
pub struct MigratedMetas {
    inner: Mutex<HashSet<String>>,
}

impl MigratedMetas {
    pub fn claim(&self, info_hash: &str) -> bool {
        self.inner.lock().insert(info_hash.to_string())
    }
}

/// One active playback stream, as registered when a VFS usenet session
/// begins serving and removed when its file handle is dropped.
#[derive(Debug, Clone)]
pub struct ActiveStream {
    pub info_hash: String,
    pub filename: String,
    pub file_size: u64,
    pub started_at: i64,
    pub last_active: i64,
    pub client: String,
}

#[derive(Default)]
pub struct ActiveStreams {
    inner: Mutex<HashMap<String, ActiveStream>>,
}

impl ActiveStreams {
    pub fn register(&self, key: String, stream: ActiveStream) {
        self.inner.lock().insert(key, stream);
    }

    pub fn touch(&self, key: &str, now: i64) {
        if let Some(s) = self.inner.lock().get_mut(key) {
            s.last_active = now;
        }
    }

    pub fn unregister(&self, key: &str) {
        self.inner.lock().remove(key);
    }

    pub fn has_any(&self) -> bool {
        !self.inner.lock().is_empty()
    }

    /// Number of usenet file handles the VFS is currently serving.
    pub fn count(&self) -> usize {
        self.inner.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nzb::NzbSegment;
    use crate::streamer::{NzbMeta, NzbMetaFile, NzbMetaSource};

    fn meta_with_segments(info_hash: &str, n: usize) -> Arc<NzbMeta> {
        let segments: Vec<NzbSegment> = (0..n)
            .map(|i| NzbSegment {
                bytes: 700_000,
                number: i as u32 + 1,
                // ~40-char message-ids, like real usenet posts.
                message_id: format!("{i:08}@news.example.invalid.padding.xx"),
            })
            .collect();
        let offsets: Vec<u64> = (0..=n as u64).map(|i| i * 700_000).collect();
        Arc::new(NzbMeta {
            info_hash: info_hash.to_string(),
            password: None,
            files: vec![NzbMetaFile {
                filename: format!("{info_hash}.mkv"),
                total_size: (n as u64) * 700_000,
                source: NzbMetaSource::Direct { offsets, segments },
            }],
        })
    }

    #[test]
    fn meta_cache_evicts_lru_over_budget() {
        // Budget for ~2 of these metas.
        let one = estimate_meta_bytes(&meta_with_segments("probe", 1_000));
        let cache = MetaCache::new(one * 2 + one / 2);

        cache.put("a".into(), meta_with_segments("a", 1_000));
        cache.put("b".into(), meta_with_segments("b", 1_000));
        // Touch "a" so "b" becomes the LRU victim.
        assert!(cache.get("a").is_some());
        cache.put("c".into(), meta_with_segments("c", 1_000));

        assert!(cache.get("a").is_some(), "recently-used survives");
        assert!(cache.get("b").is_none(), "LRU evicted");
        assert!(cache.get("c").is_some(), "newest survives");
        assert!(cache.current_bytes() <= one * 2 + one / 2);
    }

    #[test]
    fn meta_cache_keeps_oversized_single_entry() {
        // A meta larger than the whole budget must still be cached for the
        // stream that just asked for it.
        let big = meta_with_segments("big", 50_000);
        let cache = MetaCache::new(1024); // absurdly small
        cache.put("big".into(), big);
        assert!(cache.get("big").is_some());
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn decoded_sizes_evicts_lru() {
        let sizes = DecodedSizes::new(2);
        sizes.put("a".into(), 1);
        sizes.put("b".into(), 2);
        let _ = sizes.get("a"); // a → MRU
        sizes.put("c".into(), 3); // evicts b
        assert_eq!(sizes.get("a"), Some(1));
        assert_eq!(sizes.get("b"), None);
        assert_eq!(sizes.get("c"), Some(3));
    }
}
