use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;

/// Cache keyed by (ino, start_byte, end_byte).
pub type RangeCache = Mutex<LruCache<(u64, u64, u64), Bytes>>;

pub fn cache_get(cache: &RangeCache, key: (u64, u64, u64)) -> Option<Bytes> {
    cache.lock().get(&key).cloned()
}

pub fn cache_put(cache: &RangeCache, key: (u64, u64, u64), data: Bytes) {
    cache.lock().put(key, data);
}
