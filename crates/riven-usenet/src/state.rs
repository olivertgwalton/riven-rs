//! Process-global mutable state for the streamer: deserialized NzbMeta
//! cache, decoded-segment-size memoization, permanent-fail tracking, and
//! the active-streams registry.
//!
//! All entries are keyed in a way that's stable across requests (info_hash,
//! message_id) so the same instance is reused by the ingest path and the
//! read path inside the same process.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

/// Deserialized metadata cache. Eliminates a per-read Redis round-trip +
/// JSON deserialization. Entries are dropped when the underlying Redis key
/// expires (we re-fetch on miss).
#[derive(Default)]
pub struct MetaCache {
    inner: Mutex<HashMap<String, Arc<crate::streamer::NzbMeta>>>,
}

impl MetaCache {
    pub fn get(&self, info_hash: &str) -> Option<Arc<crate::streamer::NzbMeta>> {
        self.inner.lock().get(info_hash).cloned()
    }

    pub fn put(&self, info_hash: String, meta: Arc<crate::streamer::NzbMeta>) {
        self.inner.lock().insert(info_hash, meta);
    }

    pub fn invalidate(&self, info_hash: &str) {
        self.inner.lock().remove(info_hash);
    }
}

/// Memoized decoded size of NNTP segments keyed by message-id. Populated as
/// segments are fetched. Lets us know "this segment is N decoded bytes"
/// without re-fetching — required to binary-search into the middle of a
/// part when serving a random seek.
#[derive(Default)]
pub struct DecodedSizes {
    inner: Mutex<HashMap<String, u64>>,
}

impl DecodedSizes {
    pub fn get(&self, message_id: &str) -> Option<u64> {
        self.inner.lock().get(message_id).copied()
    }

    pub fn put(&self, message_id: String, size: u64) {
        self.inner.lock().insert(message_id, size);
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
    inner: Mutex<HashMap<String, Arc<PromiseSlot>>>,
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
    /// `finish(message_id, &slot)` to release waiters.
    Owner(Arc<PromiseSlot>),
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
            let slot = Arc::new(PromiseSlot::default());
            map.insert(message_id.to_string(), slot.clone());
            FetchEntry::Owner(slot)
        }
    }

    pub fn finish(&self, message_id: &str, slot: &Arc<PromiseSlot>) {
        // Mark done BEFORE removing from map. A new caller arriving in the
        // gap would become their own Owner and re-fetch — wasteful but correct.
        slot.mark_done();
        self.inner.lock().remove(message_id);
    }
}

/// Tracks segments that we know are permanently missing on the provider
/// (NNTP `430 No such article`). Repeated reads short-circuit instead of
/// re-spending the round-trip.
#[derive(Default)]
pub struct PermanentFails {
    inner: Mutex<HashMap<String, ()>>,
}

impl PermanentFails {
    pub fn is_dead(&self, message_id: &str) -> bool {
        self.inner.lock().contains_key(message_id)
    }

    pub fn mark_dead(&self, message_id: String) {
        self.inner.lock().insert(message_id, ());
    }
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

/// One active playback stream, as registered when a `/usenet/` body stream
/// begins serving and removed when it ends.
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

    pub fn snapshot(&self) -> Vec<ActiveStream> {
        self.inner.lock().values().cloned().collect()
    }
}
