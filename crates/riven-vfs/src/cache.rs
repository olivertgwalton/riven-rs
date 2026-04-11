use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use lru::LruCache;
use parking_lot::{Condvar, Mutex};

pub type CacheKey = (u64, u64, u64);

const DEFAULT_SHARD_COUNT: usize = 64;

#[derive(Default)]
struct InFlightState {
    fetching: bool,
    result: Option<Result<Bytes, ()>>,
}

struct InFlightFetch {
    state: Mutex<InFlightState>,
    ready: Condvar,
}

impl InFlightFetch {
    fn new() -> Self {
        Self {
            state: Mutex::new(InFlightState::default()),
            ready: Condvar::new(),
        }
    }
}

/// Shared byte-range cache keyed by `(ino, start_byte, end_byte)`.
///
/// The cache is sharded to reduce lock contention across concurrent reads and
/// also deduplicates in-flight fetches for the same range.
pub struct RangeCache {
    shards: Box<[Mutex<LruCache<CacheKey, Bytes>>]>,
    in_flight: DashMap<CacheKey, Arc<InFlightFetch>>,
}

impl RangeCache {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let shard_count = capacity.min(DEFAULT_SHARD_COUNT).max(1);
        let shard_capacity = capacity.div_ceil(shard_count).max(1);
        let shards = (0..shard_count)
            .map(|_| Mutex::new(LruCache::new(NonZeroUsize::new(shard_capacity).unwrap())))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            shards,
            in_flight: DashMap::new(),
        }
    }

    pub fn get(&self, key: CacheKey) -> Option<Bytes> {
        self.shard(key).lock().get(&key).cloned()
    }

    pub fn put(&self, key: CacheKey, data: Bytes) {
        self.shard(key).lock().put(key, data);
    }

    pub fn get_or_fetch<F>(&self, key: CacheKey, fetch: F) -> Result<Bytes, ()>
    where
        F: FnOnce() -> Result<Bytes, ()>,
    {
        if let Some(data) = self.get(key) {
            return Ok(data);
        }

        let flight = self
            .in_flight
            .entry(key)
            .or_insert_with(|| Arc::new(InFlightFetch::new()))
            .clone();

        let mut state = flight.state.lock();
        if !state.fetching && state.result.is_none() {
            state.fetching = true;
            drop(state);

            let result = fetch();
            if let Ok(data) = &result {
                self.put(key, data.clone());
            }

            let mut state = flight.state.lock();
            state.fetching = false;
            state.result = Some(result.clone());
            flight.ready.notify_all();
            drop(state);
            self.in_flight.remove(&key);
            return result;
        }

        while state.fetching {
            flight.ready.wait(&mut state);
        }

        if let Some(result) = state.result.clone() {
            return result;
        }

        drop(state);
        self.get(key).ok_or(())
    }

    fn shard(&self, key: CacheKey) -> &Mutex<LruCache<CacheKey, Bytes>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let index = (hasher.finish() as usize) % self.shards.len();
        &self.shards[index]
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use bytes::Bytes;

    use super::RangeCache;

    #[test]
    fn get_or_fetch_only_runs_one_fetcher_for_same_key() {
        let cache = Arc::new(RangeCache::new(16));
        let runs = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let runs = Arc::clone(&runs);
            handles.push(thread::spawn(move || {
                cache
                    .get_or_fetch((1, 0, 9), || {
                        runs.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(std::time::Duration::from_millis(10));
                        Ok(Bytes::from_static(b"1234567890"))
                    })
                    .unwrap()
            }));
        }

        for handle in handles {
            assert_eq!(handle.join().unwrap().len(), 10);
        }

        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}
