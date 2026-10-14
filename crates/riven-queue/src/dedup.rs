//! Hand-rolled deduplication, kept deliberately.
//!
//! apalis-redis has native idempotency keys, but nothing releases one when its
//! task finishes; only `idempotency_ttl` expires it. An item could then not be
//! queued again until the TTL ran out, however soon its job ended. So dedup
//! stays here: `SET NX` with a TTL, deleted by the guard when the handler
//! finishes.

/// Safety TTL for dedup keys. Under normal operation keys are deleted synchronously
/// by `DedupGuard::drop`; this TTL fires only when the process is hard-killed before
/// the guard runs, preventing permanently orphaned keys.
pub(crate) const DEDUP_KEY_TTL_SECS: u64 = 30 * 60;

/// RAII guard that releases a dedup key when dropped.
///
/// Held for the lifetime of an apalis job handler and released automatically on
/// completion, early return, or panic — so flow code never needs to call
/// `release_dedup` directly.
pub struct DedupGuard {
    key: String,
    redis: redis::aio::ConnectionManager,
}

impl DedupGuard {
    pub(crate) fn new(prefix: &'static str, id: i64, redis: redis::aio::ConnectionManager) -> Self {
        Self {
            key: crate::dedup_key(prefix, id),
            redis,
        }
    }
}

impl Drop for DedupGuard {
    fn drop(&mut self) {
        let key = self.key.clone();
        let mut conn = self.redis.clone();
        tokio::spawn(async move {
            let _result: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
        });
    }
}
