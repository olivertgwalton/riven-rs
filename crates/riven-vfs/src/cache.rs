use lru::LruCache;
use parking_lot::Mutex;

use crate::chunks::Chunk;

pub type ChunkCache = Mutex<LruCache<(u64, u64, u64), Vec<u8>>>;

/// Returns `true` if every chunk in `needed` is present in the cache.
pub fn all_chunks_cached(cache: &ChunkCache, needed: &[Chunk], ino: u64) -> bool {
    let guard = cache.lock();
    needed.iter().all(|c| guard.contains(&c.cache_key(ino)))
}

/// Copy bytes for a contiguous set of chunks out of the cache into a buffer.
///
/// `start` and `end` are the inclusive byte offsets of the requested read
/// within the file — used to slice each chunk to the requested window.
pub fn read_from_cache(
    cache: &ChunkCache,
    needed: &[Chunk],
    ino: u64,
    start: u64,
    end: u64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity((end - start + 1) as usize);
    let mut guard = cache.lock();
    for chunk in needed {
        if let Some(data) = guard.get(&chunk.cache_key(ino)) {
            let chunk_offset = start.saturating_sub(chunk.start) as usize;
            let chunk_end = ((end - chunk.start + 1) as usize).min(data.len());
            if chunk_offset < data.len() {
                buf.extend_from_slice(&data[chunk_offset..chunk_end.min(data.len())]);
            }
        }
    }
    buf
}

/// Insert one entry into the cache.
pub fn cache_put(cache: &ChunkCache, key: (u64, u64, u64), data: Vec<u8>) {
    cache.lock().put(key, data);
}

/// Retrieve a single entry from the cache by key.
pub fn cache_get(cache: &ChunkCache, key: (u64, u64, u64)) -> Option<Vec<u8>> {
    cache.lock().get(&key).cloned()
}

/// Store a set of aligned chunks fetched from a contiguous HTTP response.
///
/// `fetch_start` is the byte offset at which the HTTP response begins;
/// each chunk's position within the response is derived from that base.
pub fn store_fetched_chunks(
    cache: &ChunkCache,
    ino: u64,
    needed: &[Chunk],
    data: &[u8],
    fetch_start: u64,
) {
    let mut guard = cache.lock();
    for chunk in needed {
        let chunk_start_in_data = (chunk.start - fetch_start) as usize;
        let chunk_end_in_data = ((chunk.end - fetch_start + 1) as usize).min(data.len());
        if chunk_start_in_data < data.len() {
            let chunk_data = data[chunk_start_in_data..chunk_end_in_data].to_vec();
            guard.put(chunk.cache_key(ino), chunk_data);
        }
    }
}
