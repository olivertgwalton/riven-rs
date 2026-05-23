//! Process-wide recyclable byte-buffer pool.
//!
//! Both sides of a segment fetch churn through ~700 KB-1 MB `Vec<u8>`
//! allocations: the encoded NNTP article body (input to the yEnc decoder)
//! and the decoded segment (output, handed to the cache as `Bytes`). musl's
//! allocator returns large freed allocations to the kernel via `mmap` +
//! `madvise`, so each fresh allocation paid per-page first-touch faults
//! during 4K HDR streaming. Recycling the allocations through a free list
//! keeps the same hot pages in play.
//!
//! This module is the single implementation behind both the encoded-body
//! and decoded-segment pools — previously two near-identical copies. The
//! two remain *separate instances* (distinct working sets, no cross-pool
//! mutex contention), but share one tested implementation.

use parking_lot::Mutex;

/// A bounded free list of reusable `Vec<u8>` buffers. Construct as a
/// `static` (the constructor is `const`); take a buffer with [`take`], and
/// either return it explicitly with [`give`] or — preferred — hold it in a
/// [`PooledBuf`] whose `Drop` returns it automatically.
///
/// [`take`]: BufPool::take
/// [`give`]: BufPool::give
pub(crate) struct BufPool {
    free: Mutex<Vec<Vec<u8>>>,
    /// Don't pool buffers larger than this — keeps an outlier from pinning
    /// a lot of memory.
    max_buf_bytes: usize,
    /// Cap on retained buffers. `max_bufs × max_buf_bytes` bounds the pool's
    /// memory footprint.
    max_bufs: usize,
}

impl BufPool {
    pub(crate) const fn new(max_bufs: usize, max_buf_bytes: usize) -> Self {
        Self {
            free: Mutex::new(Vec::new()),
            max_buf_bytes,
            max_bufs,
        }
    }

    /// Take a cleared buffer with at least `min_capacity` capacity, reusing
    /// a pooled allocation when one is available.
    pub(crate) fn take(&self, min_capacity: usize) -> Vec<u8> {
        let reused = self.free.lock().pop();
        let mut buf = reused.unwrap_or_default();
        buf.clear();
        if buf.capacity() < min_capacity {
            buf.reserve(min_capacity);
        }
        buf
    }

    /// Return a buffer to the pool (cleared). Dropped on the floor if it's
    /// oversized or the pool is already full.
    pub(crate) fn give(&self, mut buf: Vec<u8>) {
        if buf.capacity() == 0 || buf.capacity() > self.max_buf_bytes {
            return;
        }
        buf.clear();
        let mut free = self.free.lock();
        if free.len() < self.max_bufs {
            free.push(buf);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.free.lock().len()
    }
}

/// A buffer borrowed from a [`BufPool`] that returns itself on drop.
/// Derefs to `[u8]` and implements `AsRef<[u8]>`, so it works directly as
/// the owner in `bytes::Bytes::from_owner` and as a `&[u8]` argument.
pub(crate) struct PooledBuf {
    buf: Vec<u8>,
    pool: &'static BufPool,
}

impl PooledBuf {
    pub(crate) fn take(pool: &'static BufPool, min_capacity: usize) -> Self {
        Self {
            buf: pool.take(min_capacity),
            pool,
        }
    }

    /// Mutable access to fill the buffer (e.g. reading an article body into
    /// it). The buffer is empty on entry.
    pub(crate) fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }
}

impl std::ops::Deref for PooledBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buf
    }
}

impl AsRef<[u8]> for PooledBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.buf);
        self.pool.give(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Separate static pools per test — tests run in parallel and would
    // otherwise race on a shared free list.
    static RECYCLE_POOL: BufPool = BufPool::new(2, 1024);
    static CAP_POOL: BufPool = BufPool::new(2, 1024);

    #[test]
    fn recycles_allocation() {
        let mut b = PooledBuf::take(&RECYCLE_POOL, 256);
        let cap = b.buf.capacity();
        let ptr = b.buf.as_ptr() as usize;
        b.as_mut_vec().extend_from_slice(b"hello");
        drop(b);
        let reused = PooledBuf::take(&RECYCLE_POOL, 0);
        assert_eq!(reused.buf.capacity(), cap, "capacity preserved");
        assert_eq!(reused.buf.as_ptr() as usize, ptr, "same allocation");
        assert!(reused.is_empty(), "cleared on return");
    }

    #[test]
    fn drops_oversized_and_respects_cap() {
        // Oversized: not pooled.
        CAP_POOL.give(Vec::with_capacity(4096));
        assert_eq!(CAP_POOL.len(), 0);
        // Cap of 2 retained.
        CAP_POOL.give(Vec::with_capacity(512));
        CAP_POOL.give(Vec::with_capacity(512));
        CAP_POOL.give(Vec::with_capacity(512));
        assert_eq!(CAP_POOL.len(), 2);
    }
}
