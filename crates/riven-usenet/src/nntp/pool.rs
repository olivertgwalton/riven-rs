use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::priority_semaphore::{OwnedPermit, PrioritizedSemaphore, Priority};
use super::{NntpConnection, NntpError, NntpProvider};

/// Drop an idle connection that has been sitting in the pool longer than
/// this. Aggressive on purpose: several commercial NNTP providers silently
/// close idle TLS sockets after ~30s without sending `205`, so the next
/// `BODY` fails mid-stream.
const IDLE_TIMEOUT: Duration = Duration::from_secs(20);
/// If a pooled connection has been idle this long but not yet expired,
/// ping it with `DATE` before reuse to confirm it's still alive.
const STALE_THRESHOLD: Duration = Duration::from_secs(10);
/// Deadline for the pre-reuse `DATE` ping. Long enough to absorb a
/// network blip, short enough that a dead socket doesn't add measurable
/// latency to first-byte.
const PING_TIMEOUT: Duration = Duration::from_millis(1500);
/// How often the background reaper sweeps each provider's idle stack.
const REAPER_INTERVAL: Duration = Duration::from_secs(5);
/// Cap on connections opened eagerly per provider at startup, so the
/// streamer doesn't consume the entire provider connection allowance
/// before other consumers (ingest, scrape) can dial.
const PREWARM_CAP: usize = 8;
const MAX_DOWNLOAD_CONNECTIONS: usize = 40;
/// Fixed per-ingest NNTP fan-out budget. A single ingest never grabs more than
/// this many connections, so many ingests run concurrently under the shared
/// semaphore instead of one monopolising the whole pool.
/// Modelled on altmount's `MaxImportConnections` (default 5).
pub const INGEST_CONNECTIONS: usize = 6;
/// Consecutive transient/connection failures before a provider is muted by
/// its circuit breaker.
const BREAKER_FAILURE_THRESHOLD: u32 = 3;
/// First cooldown after tripping. Doubled on each subsequent re-trip until
/// `BREAKER_MAX_COOLDOWN`.
const BREAKER_INITIAL_COOLDOWN: Duration = Duration::from_secs(60);
/// Cap on the exponential backoff so a permanently-broken provider doesn't
/// vanish forever — a probe still runs every 5 min to check recovery.
const BREAKER_MAX_COOLDOWN: Duration = Duration::from_secs(5 * 60);

/// Per-provider circuit breaker. Records consecutive transient failures and
/// suppresses provider use for a cooldown window once `FAILURE_THRESHOLD` is
/// crossed. Successful ops reset the failure counter; an op completing while
/// the breaker is tripped (a single probe `try_each` allows when every
/// provider is tripped) either resets the breaker or re-trips it with a
/// doubled cooldown.
#[derive(Default)]
struct CircuitBreaker {
    /// Consecutive failure count; reset to 0 on success.
    consecutive_failures: AtomicU64,
    /// Next attempt allowed at, as `Instant::elapsed_since` reference epoch.
    /// 0 = not tripped. We can't store `Instant` atomically; store millis
    /// from a process-local epoch instead.
    tripped_until_ms: AtomicU64,
    /// Current cooldown duration in millis. Doubled on re-trip.
    current_cooldown_ms: AtomicU64,
}

impl CircuitBreaker {
    fn new() -> Self {
        Self {
            consecutive_failures: AtomicU64::new(0),
            tripped_until_ms: AtomicU64::new(0),
            current_cooldown_ms: AtomicU64::new(BREAKER_INITIAL_COOLDOWN.as_millis() as u64),
        }
    }

    fn now_ms() -> u64 {
        use std::sync::OnceLock;
        static EPOCH: OnceLock<Instant> = OnceLock::new();
        let epoch = EPOCH.get_or_init(Instant::now);
        epoch.elapsed().as_millis() as u64
    }

    /// True if the breaker is currently muting this provider.
    fn is_tripped(&self) -> bool {
        let until = self.tripped_until_ms.load(Ordering::Relaxed);
        until != 0 && Self::now_ms() < until
    }

    /// Seconds until the breaker re-allows this provider (0 if not tripped).
    fn cooldown_remaining_secs(&self) -> u64 {
        let until = self.tripped_until_ms.load(Ordering::Relaxed);
        let now = Self::now_ms();
        if until > now { (until - now) / 1000 } else { 0 }
    }

    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.tripped_until_ms.store(0, Ordering::Relaxed);
        self.current_cooldown_ms.store(
            BREAKER_INITIAL_COOLDOWN.as_millis() as u64,
            Ordering::Relaxed,
        );
    }

    fn record_failure(&self, host: &str) {
        let was_tripped = self.is_tripped();
        let count = self
            .consecutive_failures
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1) as u32;
        if was_tripped || count >= BREAKER_FAILURE_THRESHOLD {
            let cooldown = if was_tripped {
                let doubled = self
                    .current_cooldown_ms
                    .load(Ordering::Relaxed)
                    .saturating_mul(2);
                doubled.min(BREAKER_MAX_COOLDOWN.as_millis() as u64)
            } else {
                self.current_cooldown_ms.load(Ordering::Relaxed)
            };
            self.current_cooldown_ms.store(cooldown, Ordering::Relaxed);
            self.tripped_until_ms
                .store(Self::now_ms() + cooldown, Ordering::Relaxed);
            tracing::warn!(
                host,
                cooldown_secs = cooldown / 1000,
                consecutive_failures = count,
                "NNTP provider circuit breaker tripped"
            );
        }
    }
}

struct Idle {
    conn: NntpConnection,
    /// Held while the conn sits idle so the semaphore reflects total
    /// open sockets, not in-flight ones. Dropping the permit (expiry,
    /// ping failure, op error) frees the slot for a new dial.
    permit: OwnedPermit,
    last_used: Instant,
}

struct ProviderSlot {
    provider: NntpProvider,
    permits: Arc<PrioritizedSemaphore>,
    /// LIFO: hottest conn at the back. Reaper sweeps the front (oldest)
    /// and can short-circuit at the first non-expired entry.
    idle: Arc<Mutex<VecDeque<Idle>>>,
    /// Callers that found the idle pool empty and every permit already
    /// spoken for. `release()` hands a freed connection directly to the
    /// front of `waiters_high` (else `waiters_low`) instead of parking it
    /// in `idle` — nothing else wakes a queued waiter promptly, since a
    /// plain semaphore permit release only wakes the semaphore's own
    /// (otherwise-unused) internal queue. High is always drained before
    /// Low: streaming reads should never queue behind background ingest.
    waiters_high: Arc<Mutex<VecDeque<oneshot::Sender<Idle>>>>,
    waiters_low: Arc<Mutex<VecDeque<oneshot::Sender<Idle>>>>,
    breaker: Arc<CircuitBreaker>,
    /// Wire bytes (encoded article bodies) downloaded from this provider this
    /// process, and the number of article bodies served. Session counters; a
    /// flusher persists deltas to the DB for lifetime totals + usage trends.
    bytes_downloaded: AtomicU64,
    articles_downloaded: AtomicU64,
}

struct Checkout {
    conn: NntpConnection,
    permit: OwnedPermit,
    slot_idx: usize,
}

/// Read-only health snapshot of one provider slot, for the API's
/// provider-health view. Carries no credentials.
#[derive(Debug, Clone)]
pub struct ProviderHealth {
    pub host: String,
    pub port: u16,
    pub priority: i32,
    pub is_backup: bool,
    /// Connection ceiling (the user's `max_connections`).
    pub max_connections: u32,
    /// Open sockets right now (idle + in-flight).
    pub open_connections: u32,
    /// Open sockets sitting idle in the pool.
    pub idle_connections: u32,
    /// Open sockets currently servicing a fetch.
    pub active_connections: u32,
    /// Circuit breaker is muting this provider.
    pub breaker_tripped: bool,
    /// Seconds until the breaker re-allows the provider (0 if not tripped).
    pub cooldown_seconds_remaining: u64,
    /// Consecutive transient failures recorded since the last success.
    pub consecutive_failures: u64,
}

/// Per-provider session download counters (since process start). A flusher
/// persists the deltas to the DB for lifetime totals and daily usage trends.
#[derive(Debug, Clone)]
pub struct ProviderTraffic {
    pub host: String,
    pub bytes_downloaded: u64,
    pub articles_downloaded: u64,
}

pub struct NntpPool {
    /// Primaries (by priority asc), then backups (by priority asc).
    slots: Vec<ProviderSlot>,
}

impl NntpPool {
    pub fn new_multi(mut providers: Vec<NntpProvider>) -> Arc<Self> {
        providers.sort_by(|a, b| {
            a.is_backup
                .cmp(&b.is_backup)
                .then(a.priority.cmp(&b.priority))
        });
        let slots = providers
            .into_iter()
            .map(|p| {
                let permits = PrioritizedSemaphore::new(p.config.max_connections.max(1) as usize);
                ProviderSlot {
                    provider: p,
                    permits,
                    idle: Arc::new(Mutex::new(VecDeque::new())),
                    waiters_high: Arc::new(Mutex::new(VecDeque::new())),
                    waiters_low: Arc::new(Mutex::new(VecDeque::new())),
                    breaker: Arc::new(CircuitBreaker::new()),
                    bytes_downloaded: AtomicU64::new(0),
                    articles_downloaded: AtomicU64::new(0),
                }
            })
            .collect();
        let pool = Arc::new(Self { slots });
        Self::spawn_reaper(Arc::downgrade(&pool));
        pool
    }

    /// Per-provider health snapshot in pool order (primaries first, then
    /// backups). Cheap and lock-light — used by the API's provider-health view.
    pub fn health(&self) -> Vec<ProviderHealth> {
        self.slots
            .iter()
            .map(|slot| {
                let max = slot.provider.config.max_connections;
                let available = slot.permits.available_permits() as u32;
                let open = max.saturating_sub(available);
                let idle = slot.idle.lock().len() as u32;
                let active = open.saturating_sub(idle);
                ProviderHealth {
                    host: slot.provider.config.host.clone(),
                    port: slot.provider.config.port,
                    priority: slot.provider.priority,
                    is_backup: slot.provider.is_backup,
                    max_connections: max,
                    open_connections: open,
                    idle_connections: idle,
                    active_connections: active,
                    breaker_tripped: slot.breaker.is_tripped(),
                    cooldown_seconds_remaining: slot.breaker.cooldown_remaining_secs(),
                    consecutive_failures: slot.breaker.consecutive_failures.load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    /// Weak ref so the task exits cleanly when the pool is dropped.
    fn spawn_reaper(weak: Weak<Self>) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(REAPER_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let Some(pool) = weak.upgrade() else { return };
                for slot in &pool.slots {
                    Self::reap(slot);
                }
            }
        });
    }

    fn reap(slot: &ProviderSlot) {
        let mut guard = slot.idle.lock();
        while let Some(front) = guard.front() {
            if front.last_used.elapsed() > IDLE_TIMEOUT {
                guard.pop_front();
            } else {
                break;
            }
        }
    }

    /// Pre-establish up to `PREWARM_CAP` authenticated connections per
    /// provider so the first stream request finds hot sockets in the pool.
    /// Failures are logged and skipped; callers dial on demand instead.
    pub async fn prewarm(&self) {
        for slot in &self.slots {
            super::connection::warm_dns(&slot.provider.config.host, slot.provider.config.port)
                .await;
            let target = (slot.provider.config.max_connections as usize).min(PREWARM_CAP);
            let mut handles = Vec::with_capacity(target);
            for _ in 0..target {
                let cfg = slot.provider.config.clone();
                let permits = slot.permits.clone();
                let idle = slot.idle.clone();
                let host = cfg.host.clone();
                handles.push(tokio::spawn(async move {
                    let permit = permits.acquire_owned(Priority::Low).await;
                    match NntpConnection::connect(&cfg).await {
                        Ok(conn) => {
                            idle.lock().push_back(Idle {
                                conn,
                                permit,
                                last_used: Instant::now(),
                            });
                        }
                        Err(e) => {
                            drop(permit);
                            tracing::debug!(host = %host, error = %e, "NNTP prewarm dial failed");
                        }
                    }
                }));
            }
            for h in handles {
                drop(h.await);
            }
            let warmed = slot.idle.lock().len();
            tracing::info!(
                host = %slot.provider.config.host,
                warmed,
                target,
                "NNTP pool prewarmed"
            );
        }
    }

    /// How long a waiter blocks on a direct hand-off before looping back to
    /// re-check the idle pool and permit availability. A hand-off from
    /// `release()` normally resolves this almost instantly; the timeout is
    /// only a safety net for the paths that free a permit without a live
    /// connection to hand off (idle-timeout reaper, transient-error drop),
    /// which wake the semaphore's own (otherwise-unused) queue rather than
    /// `waiters_high`/`waiters_low`.
    const WAITER_RETRY_INTERVAL: Duration = Duration::from_secs(2);

    async fn acquire(&self, slot_idx: usize, priority: Priority) -> Result<Checkout, NntpError> {
        let slot = &self.slots[slot_idx];

        loop {
            // Try the idle pool first, without acquiring a new permit: an
            // idle connection already holds its own permit (see
            // `Idle::permit`), so reusing it doesn't change the
            // total-open-sockets count and needs no fresh permit.
            loop {
                let candidate = slot.idle.lock().pop_back();
                let Some(idle) = candidate else { break };
                let age = idle.last_used.elapsed();
                if age > IDLE_TIMEOUT {
                    drop(idle);
                    continue;
                }
                if age > STALE_THRESHOLD {
                    let mut conn = idle.conn;
                    let idle_permit = idle.permit;
                    let ping_started = std::time::Instant::now();
                    let ping_result = tokio::time::timeout(PING_TIMEOUT, conn.date()).await;
                    let ping_ms = ping_started.elapsed().as_millis();
                    if ping_ms > 50 {
                        tracing::debug!(host = %slot.provider.config.host, ping_ms, "NNTP acquire: stale ping");
                    }
                    match ping_result {
                        Ok(Ok(())) => {
                            return Ok(Checkout {
                                conn,
                                permit: idle_permit,
                                slot_idx,
                            });
                        }
                        Ok(Err(e)) => {
                            tracing::debug!(host = %slot.provider.config.host, error = %e, "NNTP idle ping failed");
                            drop(conn);
                            drop(idle_permit);
                            continue;
                        }
                        Err(_) => {
                            tracing::debug!(host = %slot.provider.config.host, "NNTP idle ping timed out");
                            drop(conn);
                            drop(idle_permit);
                            continue;
                        }
                    }
                }
                return Ok(Checkout {
                    conn: idle.conn,
                    permit: idle.permit,
                    slot_idx,
                });
            }

            // No usable idle connection. If the pool hasn't reached
            // max_connections total open sockets yet, dial a fresh one.
            if let Some(permit) = slot.permits.try_acquire_owned() {
                let connect_started = std::time::Instant::now();
                let conn = NntpConnection::connect(&slot.provider.config).await?;
                let connect_ms = connect_started.elapsed().as_millis();
                if connect_ms > 50 {
                    tracing::debug!(host = %slot.provider.config.host, connect_ms, "NNTP acquire: fresh dial");
                }
                return Ok(Checkout {
                    conn,
                    permit,
                    slot_idx,
                });
            }

            // Every socket is either actively in use or sitting in idle
            // (which we just drained). Register for a direct hand-off
            // instead of blocking on the semaphore: `release()` sends a
            // freed connection straight to the front of this queue, so a
            // high-priority streaming read never queues behind whatever the
            // reaper's 20s IDLE_TIMEOUT happens to be doing.
            let acquire_started = std::time::Instant::now();
            let (tx, rx) = oneshot::channel();
            match priority {
                Priority::High => slot.waiters_high.lock().push_back(tx),
                Priority::Low => slot.waiters_low.lock().push_back(tx),
            }
            match tokio::time::timeout(Self::WAITER_RETRY_INTERVAL, rx).await {
                Ok(Ok(idle)) => {
                    let semaphore_wait_ms = acquire_started.elapsed().as_millis();
                    if semaphore_wait_ms > 50 {
                        tracing::debug!(
                            host = %slot.provider.config.host,
                            semaphore_wait_ms,
                            "NNTP acquire: handoff wait"
                        );
                    }
                    return Ok(Checkout {
                        conn: idle.conn,
                        permit: idle.permit,
                        slot_idx,
                    });
                }
                // Sender dropped (shouldn't normally happen) or the
                // interval elapsed with no hand-off yet — loop back and
                // retry idle/permit/waiter from the top. The now-orphaned
                // `tx` (timeout case) is harmless: whichever `release()`
                // pops it later finds the receiver gone and moves on to
                // the next waiter or `idle`.
                Ok(Err(_)) | Err(_) => continue,
            }
        }
    }

    /// Caller must only release a conn whose last op left the wire in a
    /// clean state (Ok or ArticleNotFound). Other errors → drop the conn.
    fn release(&self, checkout: Checkout) {
        let Checkout {
            conn,
            permit,
            slot_idx,
        } = checkout;
        let slot = &self.slots[slot_idx];
        let mut idle = Idle {
            conn,
            permit,
            last_used: Instant::now(),
        };

        // Hand off directly to a queued waiter (High before Low) instead of
        // parking in `idle`: nothing else wakes a waiter promptly, since a
        // released connection never touches the semaphore's own permit
        // count (see `Idle::permit`). `send` returns the value back on
        // `Err` if the receiver was dropped (its `acquire()` call timed
        // out and looped back already) — try the next waiter in that case.
        loop {
            let next = {
                let mut hi = slot.waiters_high.lock();
                let from_high = hi.pop_front();
                if from_high.is_some() {
                    from_high
                } else {
                    drop(hi);
                    slot.waiters_low.lock().pop_front()
                }
            };
            let Some(tx) = next else { break };
            match tx.send(idle) {
                Ok(()) => return,
                Err(returned_idle) => {
                    idle = returned_idle;
                    continue;
                }
            }
        }

        slot.idle.lock().push_back(idle);
    }

    /// Runs `op` against providers in health/priority order, returning the
    /// value and the index of the slot that served it (so callers can attribute
    /// per-provider traffic).
    async fn try_each<F, Fut, T>(&self, priority: Priority, op: F) -> Result<(T, usize), NntpError>
    where
        F: Fn(NntpConnection) -> Fut,
        Fut: std::future::Future<Output = (NntpConnection, Result<T, NntpError>)>,
    {
        let mut not_found = false;
        let mut last_err: Option<NntpError> = None;

        let mut order: Vec<usize> = Vec::with_capacity(self.slots.len());
        let mut tripped: Vec<usize> = Vec::new();
        for (idx, slot) in self.slots.iter().enumerate() {
            if slot.breaker.is_tripped() {
                tripped.push(idx);
            } else {
                order.push(idx);
            }
        }
        order.extend(tripped);

        for slot_idx in order {
            let host = self.slots[slot_idx].provider.config.host.clone();
            let is_backup = self.slots[slot_idx].provider.is_backup;
            let breaker = self.slots[slot_idx].breaker.clone();
            let checkout = match self.acquire(slot_idx, priority).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(
                        host = %host,
                        backup = is_backup,
                        error = %e,
                        "NNTP acquire failed; trying next provider"
                    );
                    breaker.record_failure(&host);
                    last_err = Some(e);
                    continue;
                }
            };
            let Checkout {
                conn,
                permit,
                slot_idx,
            } = checkout;
            let (conn, result) = op(conn).await;
            match result {
                Ok(v) => {
                    breaker.record_success();
                    self.release(Checkout {
                        conn,
                        permit,
                        slot_idx,
                    });
                    return Ok((v, slot_idx));
                }
                Err(NntpError::ArticleNotFound(s)) => {
                    self.release(Checkout {
                        conn,
                        permit,
                        slot_idx,
                    });
                    last_err = Some(NntpError::ArticleNotFound(s));
                    not_found = true;
                    continue;
                }
                Err(e) => {
                    drop(conn);
                    drop(permit);
                    tracing::debug!(
                        host = %host,
                        backup = is_backup,
                        error = %e,
                        "NNTP op failed; trying next provider"
                    );
                    breaker.record_failure(&host);
                    last_err = Some(e);
                    continue;
                }
            }
        }

        if not_found {
            return Err(NntpError::ArticleNotFound(
                "article not found on any provider".to_string(),
            ));
        }
        Err(last_err.unwrap_or(NntpError::Protocol("no providers configured")))
    }

    pub(crate) async fn fetch_body(
        &self,
        message_id: &str,
        priority: Priority,
    ) -> Result<crate::bufpool::PooledBuf, NntpError> {
        let mid = message_id.to_string();
        let (buf, slot_idx) = self
            .try_each(priority, |mut conn| {
                let mid = mid.clone();
                async move {
                    let r = conn.fetch_body(&mid).await;
                    (conn, r)
                }
            })
            .await?;
        let slot = &self.slots[slot_idx];
        slot.bytes_downloaded
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        slot.articles_downloaded.fetch_add(1, Ordering::Relaxed);
        Ok(buf)
    }

    /// Per-provider session traffic counters (encoded bytes + article bodies
    /// downloaded since process start), in pool order.
    pub fn traffic_snapshot(&self) -> Vec<ProviderTraffic> {
        self.slots
            .iter()
            .map(|slot| ProviderTraffic {
                host: slot.provider.config.host.clone(),
                bytes_downloaded: slot.bytes_downloaded.load(Ordering::Relaxed),
                articles_downloaded: slot.articles_downloaded.load(Ordering::Relaxed),
            })
            .collect()
    }

    pub fn total_capacity(&self) -> usize {
        let cap: usize = self
            .slots
            .iter()
            .filter(|s| !s.provider.is_backup)
            .map(|s| s.provider.config.max_connections.max(1) as usize)
            .sum();
        cap.max(1)
    }

    pub fn download_concurrency(&self) -> usize {
        self.total_capacity().min(MAX_DOWNLOAD_CONNECTIONS)
    }

    /// Fixed per-ingest fan-out cap (see [`INGEST_CONNECTIONS`]), bounded by the
    /// pool's own capacity so a tiny account never asks for more than it has.
    /// One ingest can't take the whole pool — letting `pool ÷ INGEST_CONNECTIONS`
    /// ingests run at once.
    pub fn ingest_concurrency(&self) -> usize {
        self.total_capacity().clamp(1, INGEST_CONNECTIONS)
    }

    pub async fn stat(&self, message_id: &str, priority: Priority) -> Result<bool, NntpError> {
        let mid = message_id.to_string();
        let (exists, _slot_idx) = self
            .try_each(priority, |mut conn| {
                let mid = mid.clone();
                async move {
                    let r = conn.stat(&mid).await;
                    (conn, r)
                }
            })
            .await?;
        Ok(exists)
    }
}
