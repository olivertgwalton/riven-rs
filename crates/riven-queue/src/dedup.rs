/// Safety TTL for dedup keys. Under normal operation keys are deleted synchronously
/// by `DedupGuard::drop`; this TTL fires only when the process is hard-killed before
/// the guard runs, preventing permanently orphaned keys.
pub(crate) const DEDUP_KEY_TTL_SECS: u64 = 30 * 60; // 30 minutes

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
    pub(crate) fn new(
        prefix: &'static str,
        id: i64,
        redis: redis::aio::ConnectionManager,
    ) -> Self {
        Self {
            key: format!("riven:dedup:{prefix}:{id}"),
            redis,
        }
    }
}

impl Drop for DedupGuard {
    fn drop(&mut self) {
        let key = self.key.clone();
        let mut conn = self.redis.clone();
        // Fire-and-forget: spawning a task is the only way to do async work in Drop.
        // If the runtime is already shutting down the task is silently dropped, but
        // the safety TTL on the key will clean it up within 30 minutes.
        tokio::spawn(async move {
            let _: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
        });
    }
}
