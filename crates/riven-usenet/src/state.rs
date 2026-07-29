//! Process-global state that outlives any one `UsenetStreamer`: the
//! deserialized meta cache, single-flight coordination, and the
//! active-streams registry.
//!
//! Segment bytes, permanent-missing ids and fetch counters live in
//! [`crate::pool`] — they belong to the thing that does the fetching.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use riven_core::cache::{ByteLru, NZB_META};
use tokio::sync::{Notify, mpsc};

pub struct StreamerState {
    pub meta_cache: MetaCache,
    /// Single-flight for `load_meta`. Every episode of a season pack resolves
    /// to one `usenet_meta` row, so a scanner opening 24 of them at once would
    /// otherwise run 24 simultaneous loads and deserializes of the same
    /// document.
    pub meta_loads: InFlight,
    pub migrated: MigratedMetas,
}

impl StreamerState {
    fn new() -> Self {
        Self {
            meta_cache: MetaCache::with_budget(NZB_META),
            meta_loads: InFlight::default(),
            migrated: MigratedMetas::default(),
        }
    }

    pub fn global() -> Arc<Self> {
        static CELL: OnceLock<Arc<StreamerState>> = OnceLock::new();
        CELL.get_or_init(|| Arc::new(Self::new())).clone()
    }
}

pub fn global_active_streams() -> Arc<ActiveStreams> {
    static CELL: OnceLock<Arc<ActiveStreams>> = OnceLock::new();
    CELL.get_or_init(|| Arc::new(ActiveStreams::default()))
        .clone()
}

/// Deserialized metadata, weighed by [`estimate_meta_bytes`]. Cold releases
/// re-load from Postgres; an in-flight stream is unaffected because it holds
/// its own `Arc<NzbMeta>`. Every usenet read consults this before it can plan
/// anything, so unlike the segment cache it is genuinely high-hit.
pub type MetaCache = ByteLru<String, Arc<crate::streamer::NzbMeta>>;

pub fn cache_meta(cache: &MetaCache, info_hash: String, meta: Arc<crate::streamer::NzbMeta>) {
    let weight = estimate_meta_bytes(&meta);
    cache.put(info_hash, meta, weight);
}

/// Estimate the heap footprint of a deserialized `NzbMeta`, dominated by the
/// per-segment message-id strings.
fn estimate_meta_bytes(meta: &crate::streamer::NzbMeta) -> u64 {
    use crate::streamer::NzbMetaSource;
    let segment = std::mem::size_of::<crate::nzb::NzbSegment>();
    let mut bytes = 0u64;
    for file in &meta.files {
        match &file.source {
            NzbMetaSource::Direct { offsets, segments } => {
                bytes += (offsets.len() * 8) as u64;
                for s in segments {
                    bytes += (segment + s.message_id.len()) as u64;
                }
            }
            NzbMetaSource::Rar { parts, slices } => {
                for part in parts {
                    bytes += (part.offsets.len() * 8) as u64;
                    for s in &part.segments {
                        bytes += (segment + s.message_id.len()) as u64;
                    }
                }
                bytes +=
                    (slices.len() * std::mem::size_of::<crate::streamer::NzbRarSlice>()) as u64;
            }
        }
    }
    bytes.max(1)
}

/// Coordinates concurrent work on the same key so only the first caller does
/// it and the rest wait.
///
/// Race-free against the classic `Notify` pitfall (`notify_waiters` stores no
/// permit) by registering the waker via `Notified::enable()` *before* reading
/// the done flag.
#[derive(Default)]
pub struct InFlight {
    inner: Mutex<HashMap<Arc<str>, Arc<PromiseSlot>>>,
}

#[derive(Default)]
pub struct PromiseSlot {
    done: AtomicBool,
    notify: Notify,
}

impl PromiseSlot {
    pub async fn wait(self: &Arc<Self>) {
        let mut waiter = std::pin::pin!(self.notify.notified());
        waiter.as_mut().enable();
        if self.done.load(Ordering::Acquire) {
            return;
        }
        waiter.await;
    }

    fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

pub enum FetchEntry {
    /// You are the first caller — do the work, then `finish`. The `Arc<str>`
    /// is the shared key: reuse it downstream so the cold path allocates the
    /// id exactly once.
    Owner(Arc<PromiseSlot>, Arc<str>),
    /// Someone else is already doing it. Await the slot, then re-check.
    Wait(Arc<PromiseSlot>),
}

impl InFlight {
    pub fn enter_or_wait(&self, key: &str) -> FetchEntry {
        let mut map = self.inner.lock();
        if let Some(slot) = map.get(key) {
            return FetchEntry::Wait(slot.clone());
        }
        let owned: Arc<str> = Arc::from(key);
        let slot = Arc::new(PromiseSlot::default());
        map.insert(owned.clone(), slot.clone());
        FetchEntry::Owner(slot, owned)
    }

    pub fn finish(&self, key: &str, slot: &Arc<PromiseSlot>) {
        slot.mark_done();
        self.inner.lock().remove(key);
    }

    #[expect(
        clippy::len_without_is_empty,
        reason = "telemetry-only counter; emptiness is never queried"
    )]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }
}

#[derive(Debug, Clone)]
pub struct DeadSegmentEvent {
    pub info_hash: String,
    pub file_index: usize,
    /// Carried on the event because the repair loop in riven-app has no meta
    /// of its own, and a bare info_hash names no title.
    pub filename: String,
    pub detail: String,
}

struct DeadSegmentChannel {
    tx: mpsc::UnboundedSender<DeadSegmentEvent>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<DeadSegmentEvent>>>,
    claimed: Mutex<HashSet<String>>,
}

fn dead_segment_channel() -> &'static DeadSegmentChannel {
    static CELL: OnceLock<DeadSegmentChannel> = OnceLock::new();
    CELL.get_or_init(|| {
        let (tx, rx) = mpsc::unbounded_channel();
        DeadSegmentChannel {
            tx,
            rx: Mutex::new(Some(rx)),
            claimed: Mutex::new(HashSet::new()),
        }
    })
}

pub fn report_dead_segment(info_hash: &str, file_index: usize, filename: &str, detail: &str) {
    let channel = dead_segment_channel();
    let key = format!("{info_hash}:{file_index}");
    if !channel.claimed.lock().insert(key) {
        return;
    }
    drop(channel.tx.send(DeadSegmentEvent {
        info_hash: info_hash.to_string(),
        file_index,
        filename: filename.to_string(),
        detail: detail.to_string(),
    }));
}

pub fn take_dead_segment_receiver() -> Option<mpsc::UnboundedReceiver<DeadSegmentEvent>> {
    dead_segment_channel().rx.lock().take()
}

/// Tracks metas whose in-place backfill of `decoded_seg_size` has already
/// been started. Single-shot per info_hash per process.
#[derive(Default)]
pub struct MigratedMetas {
    inner: Mutex<HashSet<String>>,
}

impl MigratedMetas {
    pub fn claim(&self, info_hash: &str) -> bool {
        self.inner.lock().insert(info_hash.to_string())
    }
}

/// One active playback stream, registered when a VFS usenet handle opens and
/// removed when it is dropped.
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
        if let Some(stream) = self.inner.lock().get_mut(key) {
            stream.last_active = now;
        }
    }

    pub fn unregister(&self, key: &str) {
        self.inner.lock().remove(key);
    }

    pub fn has_any(&self) -> bool {
        !self.inner.lock().is_empty()
    }

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
        let one = estimate_meta_bytes(&meta_with_segments("probe", 1_000));
        let cache = MetaCache::new(one * 2 + one / 2);

        cache_meta(&cache, "a".into(), meta_with_segments("a", 1_000));
        cache_meta(&cache, "b".into(), meta_with_segments("b", 1_000));
        assert!(cache.get("a").is_some());
        cache_meta(&cache, "c".into(), meta_with_segments("c", 1_000));

        assert!(cache.get("a").is_some(), "recently-used survives");
        assert!(cache.get("b").is_none(), "LRU evicted");
        assert!(cache.get("c").is_some(), "newest survives");
        assert!(cache.stats().bytes_used <= one * 2 + one / 2);
    }

    #[test]
    fn meta_cache_keeps_oversized_single_entry() {
        let cache = MetaCache::new(1024);
        cache_meta(&cache, "big".into(), meta_with_segments("big", 50_000));
        assert!(cache.get("big").is_some());
        assert_eq!(cache.stats().entries, 1);
    }
}
