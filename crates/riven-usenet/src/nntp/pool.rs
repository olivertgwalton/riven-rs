use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

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
const MAX_DOWNLOAD_CONNECTIONS: usize = 15;
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
        // Process-local monotonic ms. `Instant` can't be coerced to a stable
        // u64 across the process directly, so anchor on the first call.
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
        // Trip the breaker on the failure that crosses the threshold and
        // double the cooldown when a re-trip happens after a probe.
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
            // Warm the DNS cache once (low concurrency) before fanning out the
            // concurrent dials below, so they all hit the cache instead of
            // hammering the resolver simultaneously.
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

    async fn acquire(&self, slot_idx: usize, priority: Priority) -> Result<Checkout, NntpError> {
        let slot = &self.slots[slot_idx];
        let permit = slot.permits.acquire_owned(priority).await;

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
                match tokio::time::timeout(PING_TIMEOUT, conn.date()).await {
                    Ok(Ok(())) => {
                        // Reuse the popped permit; drop our newly-acquired
                        // one so total semaphore count stays consistent.
                        drop(permit);
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
            drop(permit);
            return Ok(Checkout {
                conn: idle.conn,
                permit: idle.permit,
                slot_idx,
            });
        }

        let conn = NntpConnection::connect(&slot.provider.config).await?;
        Ok(Checkout {
            conn,
            permit,
            slot_idx,
        })
    }

    /// Caller must only release a conn whose last op left the wire in a
    /// clean state (Ok or ArticleNotFound). Other errors → drop the conn.
    fn release(&self, checkout: Checkout) {
        let Checkout {
            conn,
            permit,
            slot_idx,
        } = checkout;
        self.slots[slot_idx].idle.lock().push_back(Idle {
            conn,
            permit,
            last_used: Instant::now(),
        });
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

        // Build the order: providers whose breaker is *not* tripped come
        // first, in the existing priority order. Tripped providers come last
        // so they still get a probe attempt when every healthy provider has
        // been exhausted; that tail probe gives the breaker a chance to reset.
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
                    // Missing articles are a normal outcome — not a provider
                    // health signal. Leave the breaker state alone.
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
                    // Wire state unclear — close rather than reuse.
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
        // Attribute the downloaded (encoded) wire bytes + this article to the
        // provider that served it.
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
        // STAT is a check, not a download, so it doesn't count toward traffic.
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
