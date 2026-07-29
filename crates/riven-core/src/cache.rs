//! One byte-bounded LRU and one memory budget for every streaming cache.
//!
//! [`ByteLru`] is the mechanism, [`Pool`] the policy: sizes live in one place
//! so the split between caches is visible, and so a process memory limit
//! scales all of them rather than whichever one read the variable.

use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

use lru::LruCache;
use parking_lot::Mutex;

const MIB: u64 = 1024 * 1024;
/// Share of the process memory limit the caches may claim between them; the
/// rest is in-flight decodes and the runtime.
const LIMIT_DIVISOR: u64 = 2;
/// No pool shrinks past this fraction of its default. A cache scaled to nothing
/// refetches everything, which costs more than the memory it saves.
const FLOOR_DIVISOR: u64 = 8;

/// One cache's identity and size. A pool is a row of data rather than an enum
/// variant, so everything about it sits on one line instead of being spread
/// across parallel matches.
#[derive(Debug, Clone, Copy)]
pub struct Pool {
    pub label: &'static str,
    env: &'static str,
    default_bytes: u64,
}

/// Decoded read-ahead units: every open file, both origins. Largest, because it
/// is the only cache that decides whether a read reaches the network.
pub const READ_AHEAD: Pool = Pool::new("read-ahead", "RIVEN_READ_AHEAD_CACHE_BYTES", 384 * MIB);
/// Deserialized `NzbMeta`. Every usenet read consults it before it can plan
/// anything, and a miss costs a database round trip plus a deserialize.
pub const NZB_META: Pool = Pool::new("nzb-meta", "RIVEN_USENET_META_CACHE_BYTES", 256 * MIB);
/// Raw NZB documents, as fetched from the indexer.
pub const NZB_BODY: Pool = Pool::new("nzb-body", "RIVEN_USENET_NZB_CACHE_BYTES", 64 * MIB);
/// Decoded article bodies: staging between a warm fetch landing and the walk
/// that warmed it consuming the bytes, so bounded by in-flight work.
///
/// Derived from that in-flight work rather than picked, because picking a flat
/// figure is what broke it: 64 MiB held 93 of the ~700 KiB articles it was
/// written for, but only 17 of a 3.84 MB post's — fewer than the 16 riven runs
/// at once. One in-flight generation then evicted the whole cache, so an
/// article that landed after its reader gave up was gone before that reader
/// came back for it, and every reopened range refetched from the wire.
pub const SEGMENT: Pool = Pool::new(
    "segment",
    "RIVEN_USENET_CACHE_BYTES",
    MAX_ARTICLE_BYTES * ARTICLE_MAX_IN_FLIGHT as u64 * STAGING_GENERATIONS,
);

/// Article fetches riven runs at once. Lives here, beside the cache it sizes,
/// because the two cannot be chosen independently: a staging cache smaller than
/// one in-flight generation evicts articles before their reader arrives.
/// `riven-vfs` reads this as its own in-flight cap.
pub const ARTICLE_MAX_IN_FLIGHT: usize = 16;
/// Largest article size to budget for. Posters choose the segment size; 3.84 MB
/// is the largest seen in practice, and sizing for it costs memory on a
/// small-segment post but never under-sizes on a large-segment one.
const MAX_ARTICLE_BYTES: u64 = 4 * MIB;
/// In-flight generations the staging cache holds: one on the wire, one landed
/// and not yet consumed, one whose reader went away and will be back.
const STAGING_GENERATIONS: u64 = 3;

const POOLS: [Pool; 4] = [READ_AHEAD, NZB_META, NZB_BODY, SEGMENT];

impl Pool {
    const fn new(label: &'static str, env: &'static str, default_bytes: u64) -> Self {
        Self {
            label,
            env,
            default_bytes,
        }
    }

    /// Bytes this pool may hold.
    pub fn budget(self) -> u64 {
        budget_for(self, env_u64(self.env), env_u64("RIVEN_MEMORY_LIMIT_MB"))
    }
}

fn total_default_bytes() -> u64 {
    POOLS.iter().map(|pool| pool.default_bytes).sum()
}

/// An override wins outright; otherwise every pool is scaled by the same
/// fraction when the limit cannot fit all the defaults, so their relative sizes
/// hold. Split from the environment so it can be tested.
fn budget_for(pool: Pool, override_bytes: Option<u64>, limit_mb: Option<u64>) -> u64 {
    if let Some(bytes) = override_bytes {
        return bytes;
    }
    let Some(limit_mb) = limit_mb else {
        return pool.default_bytes;
    };
    let allowed = limit_mb.saturating_mul(MIB) / LIMIT_DIVISOR;
    let total = total_default_bytes();
    if allowed >= total {
        return pool.default_bytes;
    }
    let scaled = (u128::from(pool.default_bytes) * u128::from(allowed) / u128::from(total)) as u64;
    scaled.max(pool.default_bytes / FLOOR_DIVISOR)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub bytes_used: u64,
    pub bytes_max: u64,
    pub entries: u64,
}

impl CacheStats {
    /// 0.0–1.0, and 0.0 before the first lookup.
    pub fn hit_rate(&self) -> f64 {
        let n = self.hits + self.misses;
        if n == 0 {
            0.0
        } else {
            self.hits as f64 / n as f64
        }
    }
}

/// An LRU bounded by the total weight of its values rather than their count.
///
/// Weight is given at [`ByteLru::put`] and stored beside the value, so eviction
/// never re-measures — which matters when sizing a value is expensive.
///
/// One lock, deliberately: sharding by key hash measured 35–40% *slower* at 8
/// and 16 threads and no faster at one, because the critical section is a single
/// hashmap probe. It sustains ~40 Mops/s against a streaming demand of ~0.2, so
/// the ceiling to watch is how many lookups callers make, not the lock.
pub struct ByteLru<K: Hash + Eq, V> {
    state: Mutex<Inner<K, V>>,
    max_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

struct Inner<K: Hash + Eq, V> {
    /// Unbounded by count; `bytes` against `max_bytes` drives eviction.
    lru: LruCache<K, (V, u64)>,
    bytes: u64,
}

impl<K: Hash + Eq, V: Clone> ByteLru<K, V> {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            state: Mutex::new(Inner {
                lru: LruCache::unbounded(),
                bytes: 0,
            }),
            max_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn with_budget(pool: Pool) -> Self {
        Self::new(pool.budget())
    }

    /// Fetch, promoting to most-recently-used and counting the lookup.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let found = self.touch(key);
        self.record(found.is_some());
        found
    }

    /// Promote without counting, for callers that measure hit rate at a coarser
    /// grain than one lookup — see [`ByteLru::record`].
    pub fn touch<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.state
            .lock()
            .lru
            .get(key)
            .map(|(value, _)| value.clone())
    }

    /// Held? Without promoting or counting: a scheduling probe must not keep
    /// alive an entry no reader has wanted.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.state.lock().lru.peek(key).is_some()
    }

    /// Evicts until the budget is met, always keeping one entry so an oversized
    /// value still reaches the caller that just fetched it.
    pub fn put(&self, key: K, value: V, weight: u64) {
        let mut state = self.state.lock();
        if let Some((_, previous)) = state.lru.put(key, (value, weight)) {
            state.bytes = state.bytes.saturating_sub(previous);
        }
        state.bytes = state.bytes.saturating_add(weight);
        while state.bytes > self.max_bytes && state.lru.len() > 1 {
            let Some((_, (_, evicted))) = state.lru.pop_lru() else {
                break;
            };
            state.bytes = state.bytes.saturating_sub(evicted);
        }
    }

    /// Count a hit or miss the caller measured — see [`ByteLru::touch`].
    pub fn record(&self, hit: bool) {
        let counter = if hit { &self.hits } else { &self.misses };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> CacheStats {
        let state = self.state.lock();
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bytes_used: state.bytes,
            bytes_max: self.max_bytes,
            entries: state.lru.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lru(max_bytes: u64) -> ByteLru<String, Vec<u8>> {
        ByteLru::new(max_bytes)
    }

    fn put(cache: &ByteLru<String, Vec<u8>>, key: &str, len: usize) {
        cache.put(key.to_string(), vec![0u8; len], len as u64);
    }

    #[test]
    fn evicts_least_recently_used_until_within_budget() {
        let cache = lru(100);
        put(&cache, "a", 60);
        put(&cache, "b", 60);
        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert_eq!(cache.stats().bytes_used, 60);
    }

    #[test]
    fn a_read_promotes_but_a_probe_does_not() {
        let cache = lru(100);
        put(&cache, "a", 40);
        put(&cache, "b", 40);
        // `contains` must not save "a" from being the next eviction...
        assert!(cache.contains("a"));
        put(&cache, "c", 40);
        assert!(!cache.contains("a"));

        // ...whereas reading it does.
        let cache = lru(100);
        put(&cache, "a", 40);
        put(&cache, "b", 40);
        assert!(cache.get("a").is_some());
        put(&cache, "c", 40);
        assert!(cache.contains("a"));
        assert!(!cache.contains("b"));
    }

    #[test]
    fn replacing_a_key_replaces_its_weight() {
        let cache = lru(1000);
        put(&cache, "a", 500);
        assert_eq!(cache.stats().bytes_used, 500);
        put(&cache, "a", 300);
        assert_eq!(cache.stats().bytes_used, 300);
        assert_eq!(cache.stats().entries, 1);
    }

    #[test]
    fn an_oversized_value_is_still_served_once() {
        let cache = lru(10);
        put(&cache, "huge", 999);
        assert!(
            cache.get("huge").is_some(),
            "the caller that just fetched it must be able to read it"
        );
    }

    #[test]
    fn counts_lookups_but_only_when_asked_to() {
        let cache = lru(1000);
        put(&cache, "a", 10);
        cache.get("a");
        cache.get("missing");
        cache.touch("a");
        cache.contains("a");
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses), (1, 1));
        assert_eq!(stats.hit_rate(), 0.5);

        cache.record(true);
        assert_eq!(cache.stats().hits, 2);
    }

    #[test]
    fn an_override_wins_and_a_limit_scales_every_pool_alike() {
        assert_eq!(
            budget_for(SEGMENT, Some(123), Some(64)),
            123,
            "override wins"
        );
        let roomy = Some(2 * total_default_bytes() / MIB);
        for pool in POOLS {
            assert_eq!(budget_for(pool, None, None), pool.default_bytes);
            assert_eq!(budget_for(pool, None, roomy), pool.default_bytes);
        }

        // A tight limit scales every pool by one fraction, so the split holds
        // and the total stays inside the share the caches may claim.
        let tight = Some(512);
        let total: u64 = POOLS.iter().map(|p| budget_for(*p, None, tight)).sum();
        assert!(budget_for(READ_AHEAD, None, tight) < READ_AHEAD.default_bytes);
        assert_eq!(
            budget_for(READ_AHEAD, None, tight) / budget_for(SEGMENT, None, tight),
            READ_AHEAD.default_bytes / SEGMENT.default_bytes
        );
        assert!(total <= 512 * MIB / LIMIT_DIVISOR);
    }

    /// The regression this sizing exists for. A staging cache that cannot hold
    /// one whole in-flight generation is evicted by that generation alone, so an
    /// article landing after its reader gave up is gone before the reader comes
    /// back — and every reopened range refetches from the wire. At a flat 64 MiB
    /// this failed for any article over 4 MiB / 16.
    #[test]
    fn the_segment_cache_holds_a_whole_in_flight_generation() {
        let generation = MAX_ARTICLE_BYTES * ARTICLE_MAX_IN_FLIGHT as u64;
        assert!(
            SEGMENT.default_bytes >= generation,
            "segment cache {} cannot hold one generation of {generation}",
            SEGMENT.default_bytes
        );
        // And still holds a generation on a host tight enough to scale every
        // pool down to its floor.
        let tight = Some(512);
        assert!(budget_for(SEGMENT, None, tight) >= SEGMENT.default_bytes / FLOOR_DIVISOR);
    }

    #[test]
    fn no_pool_scales_below_its_floor() {
        for pool in POOLS {
            assert_eq!(
                budget_for(pool, None, Some(1)),
                pool.default_bytes / FLOOR_DIVISOR
            );
        }
    }
}
