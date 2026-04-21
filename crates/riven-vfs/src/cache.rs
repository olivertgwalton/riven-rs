use std::num::NonZeroUsize;

use bytes::Bytes;
use lru::LruCache;
use std::sync::Mutex;

pub type CacheKey = (u64, u64, u64);

/// Shared byte-range cache keyed by `(ino, start_byte, end_byte)`.
///
/// One shared LRU for
/// fetched chunks, without extra in-flight dedupe or sharding layers.
pub struct RangeCache {
    inner: Mutex<LruCache<CacheKey, Bytes>>,
}

impl RangeCache {
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
        }
    }

    pub fn get(&self, key: CacheKey) -> Option<Bytes> {
        self.inner
            .lock()
            .expect("range cache poisoned")
            .get(&key)
            .cloned()
    }

    pub fn put(&self, key: CacheKey, data: Bytes) {
        self.inner
            .lock()
            .expect("range cache poisoned")
            .put(key, data);
    }
}

pub fn cache_get(cache: &RangeCache, key: CacheKey) -> Option<Bytes> {
    cache.get(key)
}

pub fn cache_put(cache: &RangeCache, key: CacheKey, data: Bytes) {
    cache.put(key, data);
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{RangeCache, cache_get, cache_put};

    #[test]
    fn stores_and_reads_cached_ranges() {
        let cache = RangeCache::new(2);
        let key = (1, 0, 9);

        assert!(cache_get(&cache, key).is_none());

        cache_put(&cache, key, Bytes::from_static(b"1234567890"));

        assert_eq!(cache_get(&cache, key).unwrap().len(), 10);
    }
}
