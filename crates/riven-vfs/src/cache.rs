use bytes::Bytes;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;

pub type CacheKey = (u64, u64, u64);

/// Shared byte-range cache keyed by `(ino, start_byte, end_byte)`.
///
pub struct RangeCache {
    inner: Mutex<Inner>,
}

struct Inner {
    map: HashMap<CacheKey, Bytes>,
    order: VecDeque<CacheKey>,
    bytes_used: usize,
    bytes_capacity: usize,
}

impl RangeCache {
    /// `capacity_bytes` is the total resident-bytes budget. A `0` capacity
    /// disables caching (every `get` misses, every `put` is a no-op).
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: VecDeque::new(),
                bytes_used: 0,
                bytes_capacity: capacity_bytes,
            }),
        }
    }

    pub fn get(&self, key: CacheKey) -> Option<Bytes> {
        let mut inner = self.inner.lock().expect("range cache poisoned");
        let hit = inner.map.get(&key).cloned()?;
        // Bring to MRU. Linear scan is fine; the cache is small (low
        // thousands of entries at most given our chunk sizes), and read
        // hits are far rarer than misses on a Plex scan workload.
        if let Some(pos) = inner.order.iter().position(|k| k == &key) {
            inner.order.remove(pos);
        }
        inner.order.push_back(key);
        Some(hit)
    }

    pub fn evict(&self, key: CacheKey) {
        let mut inner = self.inner.lock().expect("range cache poisoned");
        if let Some(existing) = inner.map.remove(&key) {
            inner.bytes_used = inner.bytes_used.saturating_sub(existing.len());
            if let Some(pos) = inner.order.iter().position(|k| k == &key) {
                inner.order.remove(pos);
            }
        }
    }

    pub fn put(&self, key: CacheKey, data: Bytes) {
        let mut inner = self.inner.lock().expect("range cache poisoned");
        if inner.bytes_capacity == 0 {
            return;
        }
        if let Some(existing) = inner.map.remove(&key) {
            inner.bytes_used = inner.bytes_used.saturating_sub(existing.len());
            if let Some(pos) = inner.order.iter().position(|k| k == &key) {
                inner.order.remove(pos);
            }
        }
        let incoming = data.len();
        // A single entry larger than the whole cache is dropped rather than
        // forcing eviction of everything else for a one-shot value.
        if incoming > inner.bytes_capacity {
            return;
        }
        while inner.bytes_used + incoming > inner.bytes_capacity {
            let Some(victim) = inner.order.pop_front() else {
                break;
            };
            if let Some(bytes) = inner.map.remove(&victim) {
                inner.bytes_used = inner.bytes_used.saturating_sub(bytes.len());
            }
        }
        inner.bytes_used += incoming;
        inner.map.insert(key, data);
        inner.order.push_back(key);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::RangeCache;

    #[test]
    fn stores_and_reads_cached_ranges() {
        let cache = RangeCache::new(1024);
        let key = (1, 0, 9);

        assert!(cache.get(key).is_none());

        cache.put(key, Bytes::from_static(b"1234567890"));

        assert_eq!(cache.get(key).unwrap().len(), 10);
    }

    #[test]
    fn evicts_lru_when_over_byte_budget() {
        let cache = RangeCache::new(20);

        cache.put((1, 0, 9), Bytes::from_static(b"1234567890"));
        cache.put((1, 10, 19), Bytes::from_static(b"abcdefghij"));
        // (1, 0, 9) is LRU; this push should evict it.
        cache.put((1, 20, 29), Bytes::from_static(b"!@#$%^&*()"));

        assert!(cache.get((1, 0, 9)).is_none());
        assert!(cache.get((1, 10, 19)).is_some());
        assert!(cache.get((1, 20, 29)).is_some());
    }

    #[test]
    fn disabled_capacity_skips_inserts() {
        let cache = RangeCache::new(0);
        cache.put((1, 0, 9), Bytes::from_static(b"1234567890"));
        assert!(cache.get((1, 0, 9)).is_none());
    }

    #[test]
    fn skips_entries_larger_than_capacity() {
        let cache = RangeCache::new(8);
        cache.put((1, 0, 9), Bytes::from_static(b"1234567890"));
        assert!(cache.get((1, 0, 9)).is_none());
    }
}
