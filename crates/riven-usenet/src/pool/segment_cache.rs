//! LRU cache of decoded segment bodies, shared by every reader.
//!
//! Values are `bytes::Bytes`, so a cache hit slices out a range with no copy.
//! Nothing about which provider served a segment is stored: a hit is not
//! traffic, and crediting one to a provider would inflate its usage figures.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;

/// Held back from the process memory limit for everything that is not this
/// cache — metas, in-flight decodes, the VFS read-ahead buffers.
const RESERVED_HEADROOM_BYTES: u64 = 150 * 1024 * 1024;
/// Used when no memory budget is configured at all.
const FALLBACK_ENTRIES: usize = 128;

#[derive(Debug, Clone, Copy)]
pub enum Budget {
    Bytes(u64),
    Entries(usize),
}

impl Budget {
    /// Derive the cache budget from configuration: an explicit byte budget
    /// wins, otherwise a process memory limit minus reserved headroom,
    /// otherwise a fixed entry count.
    pub fn from_env() -> Self {
        if let Some(bytes) = env_positive("RIVEN_USENET_CACHE_BYTES") {
            return Budget::Bytes(bytes);
        }
        if let Some(limit_mb) = env_positive("RIVEN_MEMORY_LIMIT_MB") {
            let limit = limit_mb.saturating_mul(1024 * 1024);
            if limit > RESERVED_HEADROOM_BYTES {
                return Budget::Bytes(limit - RESERVED_HEADROOM_BYTES);
            }
        }
        Budget::Entries(FALLBACK_ENTRIES)
    }
}

fn env_positive(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

pub struct SegmentCache {
    state: Mutex<State>,
    budget: Budget,
    hits: AtomicU64,
    misses: AtomicU64,
}

struct State {
    /// Entry count is unbounded here; eviction is driven by whichever budget
    /// the cache was built with.
    lru: LruCache<Arc<str>, Bytes>,
    current_bytes: u64,
}

impl SegmentCache {
    pub fn new(budget: Budget) -> Self {
        Self {
            state: Mutex::new(State {
                lru: LruCache::unbounded(),
                current_bytes: 0,
            }),
            budget,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, message_id: &str) -> Option<Bytes> {
        let mut state = self.state.lock();
        match state.lru.get(message_id).cloned() {
            Some(bytes) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(bytes)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn put(&self, message_id: Arc<str>, data: Bytes) {
        let mut state = self.state.lock();
        let added = data.len() as u64;
        if let Some(previous) = state.lru.put(message_id, data) {
            state.current_bytes = state.current_bytes.saturating_sub(previous.len() as u64);
        }
        state.current_bytes = state.current_bytes.saturating_add(added);
        self.evict(&mut state);
    }

    fn evict(&self, state: &mut State) {
        match self.budget {
            Budget::Bytes(max) => {
                while state.current_bytes > max && state.lru.len() > 1 {
                    let Some((_, popped)) = state.lru.pop_lru() else {
                        break;
                    };
                    state.current_bytes = state.current_bytes.saturating_sub(popped.len() as u64);
                }
            }
            Budget::Entries(max) => {
                while state.lru.len() > max.max(1) {
                    let Some((_, popped)) = state.lru.pop_lru() else {
                        break;
                    };
                    state.current_bytes = state.current_bytes.saturating_sub(popped.len() as u64);
                }
            }
        }
    }

    pub fn current_bytes(&self) -> u64 {
        self.state.lock().current_bytes
    }

    /// Configured byte ceiling, or 0 when the cache is bounded by entry count.
    pub fn max_bytes(&self) -> u64 {
        match self.budget {
            Budget::Bytes(max) => max,
            Budget::Entries(_) => 0,
        }
    }

    pub fn entry_count(&self) -> usize {
        self.state.lock().lru.len()
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_when_over_byte_budget() {
        let cache = SegmentCache::new(Budget::Bytes(100));
        cache.put("a".into(), Bytes::from(vec![0u8; 60]));
        cache.put("b".into(), Bytes::from(vec![0u8; 60]));
        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert_eq!(cache.current_bytes(), 60);
    }

    #[test]
    fn evicts_when_over_entry_budget() {
        let cache = SegmentCache::new(Budget::Entries(2));
        cache.put("a".into(), Bytes::from_static(b"1"));
        cache.put("b".into(), Bytes::from_static(b"2"));
        cache.put("c".into(), Bytes::from_static(b"3"));
        assert_eq!(cache.entry_count(), 2);
        assert!(cache.get("a").is_none());
        assert!(cache.get("c").is_some());
    }

    #[test]
    fn get_promotes_to_mru() {
        let cache = SegmentCache::new(Budget::Bytes(100));
        cache.put("a".into(), Bytes::from(vec![0u8; 40]));
        cache.put("b".into(), Bytes::from(vec![0u8; 40]));
        let _ = cache.get("a");
        cache.put("c".into(), Bytes::from(vec![0u8; 40]));
        assert!(cache.get("a").is_some());
        assert!(cache.get("b").is_none());
    }

    #[test]
    fn replacement_updates_byte_accounting() {
        let cache = SegmentCache::new(Budget::Bytes(1000));
        cache.put("a".into(), Bytes::from(vec![0u8; 500]));
        assert_eq!(cache.current_bytes(), 500);
        cache.put("a".into(), Bytes::from(vec![0u8; 300]));
        assert_eq!(cache.current_bytes(), 300);
    }
}
