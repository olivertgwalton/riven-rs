//! Message-ids every provider has definitively reported as `430`.

use std::num::NonZeroUsize;

use lru::LruCache;
use parking_lot::Mutex;

/// Entries retained before the oldest is forgotten. No time-based expiry: an
/// article missing from every provider does not come back, and re-probing one
/// costs a round-trip on the playback path.
const DEFAULT_CAPACITY: usize = 50_000;

pub struct MissingCache {
    inner: Mutex<LruCache<String, ()>>,
}

impl Default for MissingCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl MissingCache {
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
        }
    }

    pub fn contains(&self, message_id: &str) -> bool {
        self.inner.lock().contains(message_id)
    }

    pub fn insert(&self, message_id: &str) {
        self.inner.lock().put(message_id.to_string(), ());
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
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
}
