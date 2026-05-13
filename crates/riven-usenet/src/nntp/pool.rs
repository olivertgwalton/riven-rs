use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{NntpConnection, NntpError, NntpProvider, NntpServerConfig};

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

struct Idle {
    conn: NntpConnection,
    /// Held while the conn sits idle so the semaphore reflects total
    /// open sockets, not in-flight ones. Dropping the permit (expiry,
    /// ping failure, op error) frees the slot for a new dial.
    permit: OwnedSemaphorePermit,
    last_used: Instant,
}

struct ProviderSlot {
    provider: NntpProvider,
    permits: Arc<Semaphore>,
    /// LIFO: hottest conn at the back. Reaper sweeps the front (oldest)
    /// and can short-circuit at the first non-expired entry.
    idle: Arc<Mutex<VecDeque<Idle>>>,
}

struct Checkout {
    conn: NntpConnection,
    permit: OwnedSemaphorePermit,
    slot_idx: usize,
}

/// Connection pool spanning one or more NNTP providers with failover.
///
/// Each provider has its own semaphore bound to `max_connections` and a
/// LIFO stack of idle authenticated connections. Operations try providers
/// in priority order; on `ArticleNotFound` from every primary, backups
/// are consulted. Transient errors fall through to the next provider.
pub struct NntpPool {
    /// Primaries (by priority asc), then backups (by priority asc).
    slots: Vec<ProviderSlot>,
}

impl NntpPool {
    /// Build from a single legacy `NntpServerConfig` (back-compat).
    pub fn new(cfg: NntpServerConfig) -> Arc<Self> {
        Self::new_multi(vec![NntpProvider {
            config: cfg,
            priority: 0,
            is_backup: false,
        }])
    }

    pub fn new_multi(mut providers: Vec<NntpProvider>) -> Arc<Self> {
        providers.sort_by(|a, b| {
            a.is_backup
                .cmp(&b.is_backup)
                .then(a.priority.cmp(&b.priority))
        });
        let slots = providers
            .into_iter()
            .map(|p| {
                let permits = Arc::new(Semaphore::new(p.config.max_connections.max(1) as usize));
                ProviderSlot {
                    provider: p,
                    permits,
                    idle: Arc::new(Mutex::new(VecDeque::new())),
                }
            })
            .collect();
        let pool = Arc::new(Self { slots });
        Self::spawn_reaper(Arc::downgrade(&pool));
        pool
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
            let target = (slot.provider.config.max_connections as usize).min(PREWARM_CAP);
            let mut handles = Vec::with_capacity(target);
            for _ in 0..target {
                let cfg = slot.provider.config.clone();
                let permits = slot.permits.clone();
                let idle = slot.idle.clone();
                let host = cfg.host.clone();
                handles.push(tokio::spawn(async move {
                    let Ok(permit) = permits.acquire_owned().await else {
                        return;
                    };
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

    /// Legacy accessor — first configured provider's config. Use
    /// `providers()` for full enumeration.
    pub fn config(&self) -> &NntpServerConfig {
        &self.slots[0].provider.config
    }

    pub fn providers(&self) -> impl Iterator<Item = &NntpProvider> {
        self.slots.iter().map(|s| &s.provider)
    }

    async fn acquire(&self, slot_idx: usize) -> Result<Checkout, NntpError> {
        let slot = &self.slots[slot_idx];
        let permit = slot
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_closed| NntpError::Protocol("pool closed"))?;

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

    async fn try_each<F, Fut, T>(&self, op: F) -> Result<T, NntpError>
    where
        F: Fn(NntpConnection) -> Fut,
        Fut: std::future::Future<Output = (NntpConnection, Result<T, NntpError>)>,
    {
        let mut not_found = false;
        let mut last_err: Option<NntpError> = None;

        for slot_idx in 0..self.slots.len() {
            let host = self.slots[slot_idx].provider.config.host.clone();
            let is_backup = self.slots[slot_idx].provider.is_backup;
            let checkout = match self.acquire(slot_idx).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(
                        host = %host,
                        backup = is_backup,
                        error = %e,
                        "NNTP acquire failed; trying next provider"
                    );
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
                    self.release(Checkout {
                        conn,
                        permit,
                        slot_idx,
                    });
                    return Ok(v);
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
                    // Wire state unclear — close rather than reuse.
                    drop(conn);
                    drop(permit);
                    tracing::debug!(
                        host = %host,
                        backup = is_backup,
                        error = %e,
                        "NNTP op failed; trying next provider"
                    );
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

    pub async fn fetch_body(&self, message_id: &str) -> Result<Vec<u8>, NntpError> {
        let mid = message_id.to_string();
        self.try_each(|mut conn| {
            let mid = mid.clone();
            async move {
                let r = conn.fetch_body(&mid).await;
                (conn, r)
            }
        })
        .await
    }

    pub async fn stat(&self, message_id: &str) -> Result<bool, NntpError> {
        let mid = message_id.to_string();
        self.try_each(|mut conn| {
            let mid = mid.clone();
            async move {
                let r = conn.stat(&mid).await;
                (conn, r)
            }
        })
        .await
    }
}
