//! Byte-bounded LRU cache for decoded NNTP segments.
//!
//! Plex reads ranges in ~64 KB chunks, so 10+ consecutive reads tend to
//! fall in the same yEnc-decoded segment. Without caching, the streamer
//! re-fetches and re-decodes the same segment on every read. This cache
//! holds decoded segment bytes keyed by message-id; the streamer routes
//! all reads through it.
//!
//! Bounded by total bytes, not entry count — segments are big (~700 KB
//! decoded) and budgets are easier to reason about in bytes. Defaults to
//! 256 MB, overridable via `RIVEN_USENET_CACHE_BYTES`.

use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

pub struct SegmentCache {
    state: Mutex<State>,
    max_bytes: u64,
}

struct State {
    /// Unbounded entry count — we evict on byte budget.
    lru: LruCache<String, Arc<Vec<u8>>>,
    current_bytes: u64,
}

impl SegmentCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            state: Mutex::new(State {
                lru: LruCache::unbounded(),
                current_bytes: 0,
            }),
            max_bytes,
        }
    }

    pub fn get(&self, message_id: &str) -> Option<Arc<Vec<u8>>> {
        let mut state = self.state.lock();
        state.lru.get(message_id).cloned()
    }

    pub fn put(&self, message_id: String, data: Arc<Vec<u8>>) {
        let mut state = self.state.lock();
        let new_bytes = data.len() as u64;
        if let Some(prev) = state.lru.put(message_id, data) {
            state.current_bytes = state.current_bytes.saturating_sub(prev.len() as u64);
        }
        state.current_bytes = state.current_bytes.saturating_add(new_bytes);

        while state.current_bytes > self.max_bytes {
            let Some((_, popped)) = state.lru.pop_lru() else {
                state.current_bytes = 0;
                break;
            };
            state.current_bytes = state.current_bytes.saturating_sub(popped.len() as u64);
        }
    }

    #[cfg(test)]
    pub fn current_bytes(&self) -> u64 {
        self.state.lock().current_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_when_over_budget() {
        let cache = SegmentCache::new(100);
        cache.put("a".into(), Arc::new(vec![0u8; 60]));
        cache.put("b".into(), Arc::new(vec![0u8; 60]));
        // 60 + 60 = 120 > 100 → evict LRU ("a").
        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert_eq!(cache.current_bytes(), 60);
    }

    #[test]
    fn get_promotes_to_mru() {
        let cache = SegmentCache::new(100);
        cache.put("a".into(), Arc::new(vec![0u8; 40]));
        cache.put("b".into(), Arc::new(vec![0u8; 40]));
        let _ = cache.get("a"); // a → MRU
        cache.put("c".into(), Arc::new(vec![0u8; 40])); // 120 > 100 → evict LRU = b
        assert!(cache.get("a").is_some());
        assert!(cache.get("b").is_none());
        assert!(cache.get("c").is_some());
    }

    #[test]
    fn replacement_updates_byte_accounting() {
        let cache = SegmentCache::new(1000);
        cache.put("a".into(), Arc::new(vec![0u8; 500]));
        assert_eq!(cache.current_bytes(), 500);
        cache.put("a".into(), Arc::new(vec![0u8; 300]));
        assert_eq!(cache.current_bytes(), 300);
    }
}
