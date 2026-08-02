//! Message-ids every provider has reported as `430`.

use std::num::NonZeroUsize;
use std::time::Duration;

use lru::LruCache;
use parking_lot::Mutex;
use tokio::time::Instant;

/// Entries retained before the oldest is forgotten.
const DEFAULT_CAPACITY: usize = 50_000;

/// How long a message-id stays believed-missing before it is probed again.
///
/// An article that every provider has genuinely dropped does not come back, so
/// the entry exists to spare the playback path a round trip per read. But
/// "every provider said 430" is also what a provider-side spool glitch, a
/// mid-rotation backend, or an account problem misreported as `430` looks like
/// — and those *do* come back. Without an expiry a minute of provider trouble
/// marked an article dead for the lifetime of the process, and the only way
/// back was a restart.
///
/// Ten minutes is comet's `MISSING_TTL`, and the cost of being wrong in this
/// direction is one STAT per article per ten minutes.
const MISSING_TTL: Duration = Duration::from_secs(10 * 60);

pub struct MissingCache {
    inner: Mutex<LruCache<String, Instant>>,
    ttl: Duration,
}

impl Default for MissingCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl MissingCache {
    pub fn new(capacity: usize) -> Self {
        Self::with_ttl(capacity, MISSING_TTL)
    }

    pub fn with_ttl(capacity: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
            ttl,
        }
    }

    /// True while this id is still believed missing. An entry past its TTL is
    /// dropped here rather than swept, so a stale belief costs one probe and
    /// then corrects itself.
    pub fn contains(&self, message_id: &str) -> bool {
        let mut inner = self.inner.lock();
        let Some(recorded) = inner.peek(message_id) else {
            return false;
        };
        if recorded.elapsed() < self.ttl {
            return true;
        }
        inner.pop(message_id);
        false
    }

    pub fn insert(&self, message_id: &str) {
        self.inner
            .lock()
            .put(message_id.to_string(), Instant::now());
    }

    /// Entries still within their TTL. Walks the map rather than reporting its
    /// raw size, because this feeds the `dead_segments` figure the health view
    /// shows and expired entries are not dead segments.
    pub fn len(&self) -> usize {
        let inner = self.inner.lock();
        inner
            .iter()
            .filter(|(_, recorded)| recorded.elapsed() < self.ttl)
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_oldest_beyond_capacity() {
        let cache = MissingCache::new(2);
        cache.insert("a");
        cache.insert("b");
        assert!(cache.contains("a"));
        cache.insert("c");
        assert!(!cache.contains("a"));
        assert!(cache.contains("b"));
        assert!(cache.contains("c"));
    }

    /// The regression this TTL exists for: a provider blip that answers `430`
    /// for a live article must not mark it dead until the process restarts.
    #[tokio::test(start_paused = true)]
    async fn a_missing_id_is_probed_again_once_its_ttl_expires() {
        let cache = MissingCache::with_ttl(8, Duration::from_secs(600));
        cache.insert("blip@test");
        assert!(cache.contains("blip@test"));
        assert_eq!(cache.len(), 1);

        tokio::time::advance(Duration::from_secs(599)).await;
        assert!(cache.contains("blip@test"), "still inside the TTL");

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!cache.contains("blip@test"));
        assert_eq!(cache.len(), 0, "an expired entry is not a dead segment");
    }
}
