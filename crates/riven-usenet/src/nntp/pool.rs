//! NNTP connection pool: per-provider slot actors with priority lanes.
//!
//! Modeled on javi11/nntppool + altmount's budget, simplified:
//!
//! - Each provider owns `max_connections` *slot actors* — small tasks that
//!   each hold at most one live NNTP connection. Work arrives as jobs on
//!   three priority lanes; an idle slot pops the highest-priority job under
//!   a single mutex, so there is no waiter/hand-off machinery and no reaper.
//! - Lanes: `Hot` (a read the player is blocked on) > `Stream` (read-ahead
//!   fill, head/tail precache) > `Bulk` (ingest, health, repair, backfill).
//!   Priority is absolute at pop time; additionally Bulk admission shrinks
//!   while streams are active (altmount's import budget), so bulk work can
//!   never occupy the sockets a live stream is about to need.
//! - Slots with a live connection park "warm" and keep it alive with a
//!   periodic `DATE` keepalive (providers silently drop idle TLS sockets
//!   after ~30s). Slots without a connection park "cold" and only dial when
//!   warm capacity is saturated — job submission wakes warm slots first, so
//!   the read path almost never pays a TLS handshake.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::sync::oneshot;

use super::{NntpConnection, NntpError, NntpProvider};

/// Which queue a job enters. Priority is strictly `Hot > Stream > Bulk`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lane {
    /// A fetch a player is actively blocked on.
    Hot,
    /// Read-ahead / precache for a live stream: latency-tolerant but
    /// throughput-critical. Never throttled — a stream fills its window
    /// across the whole pool.
    Stream,
    /// Ingest, availability sweeps, PAR2 verify, repair, backfill.
    Bulk,
}

/// Send a `DATE` keepalive on a warm parked connection this often. Must stay
/// below the ~30s silent idle-drop several commercial providers apply.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(25);
/// Deadline for the keepalive probe round trip.
const PING_TIMEOUT: Duration = Duration::from_millis(1500);
/// A warm connection unused this long is closed (slot goes cold) — except
/// for the first `WARM_FLOOR` slots, which stay warm indefinitely so a
/// resuming stream never cold-dials.
const IDLE_CLOSE: Duration = Duration::from_secs(120);
/// Number of slots per provider that never idle-close their connection.
const WARM_FLOOR: usize = 4;
/// Consecutive transient/connection failures before a provider is muted by
/// its circuit breaker.
const BREAKER_FAILURE_THRESHOLD: u32 = 3;
/// First cooldown after tripping. Doubled on each subsequent re-trip until
/// `BREAKER_MAX_COOLDOWN`.
const BREAKER_INITIAL_COOLDOWN: Duration = Duration::from_secs(60);
/// Cap on the exponential backoff so a permanently-broken provider doesn't
/// vanish forever — a probe still runs every 5 min to check recovery.
const BREAKER_MAX_COOLDOWN: Duration = Duration::from_secs(5 * 60);
/// Backoff between dial retries after the provider reports its own account
/// connection limit ("502 too many connections"). Not a failure of the
/// provider — just wait and retry.
const TOO_MANY_CONNECTIONS_BACKOFF: Duration = Duration::from_millis(500);
/// Bound on those retries so a misconfigured `max_connections` still
/// eventually surfaces as an error instead of looping forever.
const TOO_MANY_CONNECTIONS_MAX_RETRIES: u32 = 20;
/// How many connections each active stream reserves out of Bulk's admission
/// (altmount's `streamHeadroom`). Bulk expands to the full pool when no
/// stream is playing and shrinks automatically while one is.
const STREAM_HEADROOM: usize = 2;

/// Per-provider circuit breaker. Records consecutive transient failures and
/// suppresses provider use for a cooldown window once the threshold is
/// crossed. Successful ops reset it; an op completing while tripped (the
/// single probe allowed when every provider is tripped) either resets or
/// re-trips with a doubled cooldown.
#[derive(Default)]
struct CircuitBreaker {
    consecutive_failures: AtomicU64,
    /// Next attempt allowed at, in millis from a process-local epoch
    /// (0 = not tripped) — `Instant` can't be stored atomically.
    tripped_until_ms: AtomicU64,
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
        EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
    }

    fn is_tripped(&self) -> bool {
        let until = self.tripped_until_ms.load(Ordering::Relaxed);
        until != 0 && Self::now_ms() < until
    }

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

enum JobKind {
    Body(String),
    Stat(String),
}

enum JobOutput {
    Body(crate::bufpool::PooledBuf),
    Stat(bool),
}

struct Job {
    kind: JobKind,
    lane: Lane,
    reply: oneshot::Sender<Result<JobOutput, NntpError>>,
}

/// One provider's scheduler state. A single mutex covers queues, budget
/// accounting, and slot parking counters so every scheduling decision reads
/// one consistent snapshot — there is no cross-lock ordering to get wrong.
struct Sched {
    hot: VecDeque<Job>,
    stream: VecDeque<Job>,
    bulk: VecDeque<Job>,
    /// Bulk jobs currently claimed by a slot (executing or dialing).
    bulk_inflight: usize,
    /// Slots parked holding a live connection.
    warm_parked: usize,
    /// Slots parked without a connection.
    cold_parked: usize,
    /// Live connections held by slots (executing or parked warm).
    open: usize,
    /// Slots currently executing a job.
    active: usize,
    shutdown: bool,
}

impl Sched {
    fn new() -> Self {
        Self {
            hot: VecDeque::new(),
            stream: VecDeque::new(),
            bulk: VecDeque::new(),
            bulk_inflight: 0,
            warm_parked: 0,
            cold_parked: 0,
            open: 0,
            active: 0,
            shutdown: false,
        }
    }

    /// Pop the highest-priority eligible job. Bulk admission shrinks by
    /// `STREAM_HEADROOM` connections per active stream so playback always
    /// finds free sockets (altmount's import budget).
    fn pop(&mut self, capacity: usize, active_streams: usize) -> Option<Job> {
        if let Some(job) = self.hot.pop_front() {
            return Some(job);
        }
        if let Some(job) = self.stream.pop_front() {
            return Some(job);
        }
        let reserve = (active_streams * STREAM_HEADROOM).min(capacity.saturating_sub(1));
        let bulk_cap = capacity.saturating_sub(reserve).max(1);
        if self.bulk_inflight < bulk_cap
            && let Some(job) = self.bulk.pop_front()
        {
            self.bulk_inflight += 1;
            return Some(job);
        }
        None
    }

    fn queue_mut(&mut self, lane: Lane) -> &mut VecDeque<Job> {
        match lane {
            Lane::Hot => &mut self.hot,
            Lane::Stream => &mut self.stream,
            Lane::Bulk => &mut self.bulk,
        }
    }
}

struct ProviderRt {
    provider: NntpProvider,
    sched: Mutex<Sched>,
    /// Wakes one warm-parked slot (has a live connection).
    warm_notify: Notify,
    /// Wakes one cold-parked slot (must dial first).
    cold_notify: Notify,
    breaker: CircuitBreaker,
    /// Wire bytes (encoded article bodies) and article bodies served this
    /// process; a flusher persists deltas for lifetime totals.
    bytes_downloaded: AtomicU64,
    articles_downloaded: AtomicU64,
}

impl ProviderRt {
    fn capacity(&self) -> usize {
        self.provider.config.max_connections.max(1) as usize
    }

    /// Enqueue a job and wake a slot: warm first (no dial cost), cold only
    /// when every warm slot is busy — this is what makes the pool elastic
    /// without ever cold-dialing while warm capacity is free.
    fn submit(&self, job: Job) {
        {
            let mut s = self.sched.lock();
            if s.shutdown {
                let _ = job
                    .reply
                    .send(Err(NntpError::Protocol("nntp pool shut down")));
                return;
            }
            s.queue_mut(job.lane).push_back(job);
            if s.warm_parked > 0 {
                self.warm_notify.notify_one();
                return;
            }
        }
        self.cold_notify.notify_one();
    }

    /// Wake parked slots after an admission change (bulk budget grew).
    fn wake_for_bulk(&self) {
        let s = self.sched.lock();
        if s.bulk.is_empty() {
            return;
        }
        if s.warm_parked > 0 {
            self.warm_notify.notify_one();
        } else if s.cold_parked > 0 {
            self.cold_notify.notify_one();
        }
    }
}

/// Read-only health snapshot of one provider, for the API's provider-health
/// view. Carries no credentials.
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
    /// Open sockets sitting idle (parked warm).
    pub idle_connections: u32,
    /// Open sockets currently servicing a request.
    pub active_connections: u32,
    /// Circuit breaker is muting this provider.
    pub breaker_tripped: bool,
    /// Seconds until the breaker re-allows the provider (0 if not tripped).
    pub cooldown_seconds_remaining: u64,
    /// Consecutive transient failures recorded since the last success.
    pub consecutive_failures: u64,
}

/// Per-provider session download counters (since process start).
#[derive(Debug, Clone)]
pub struct ProviderTraffic {
    pub host: String,
    pub bytes_downloaded: u64,
    pub articles_downloaded: u64,
}

pub struct NntpPool {
    /// Primaries (by priority asc), then backups (by priority asc).
    providers: Vec<Arc<ProviderRt>>,
    /// Open playback handles — drives the Bulk admission reserve. Shared
    /// with slot actors as a bare counter (never the pool itself) so tasks
    /// don't keep the pool alive and `Drop`-driven shutdown can run.
    active_streams: Arc<AtomicUsize>,
}

/// A workload-bound view of the pool. Callers choose a lane once when they
/// create a reader or background job; individual requests can't assign their
/// own priority. Dispatch policy stays inside the pool.
#[derive(Clone)]
pub struct NntpClient {
    pool: Arc<NntpPool>,
    lane: Lane,
}

impl NntpClient {
    pub(crate) async fn fetch_body(
        &self,
        message_id: &str,
    ) -> Result<crate::bufpool::PooledBuf, NntpError> {
        match self
            .pool
            .try_each(self.lane, || JobKind::Body(message_id.to_string()))
            .await?
        {
            (JobOutput::Body(buf), idx) => {
                let p = &self.pool.providers[idx];
                p.bytes_downloaded
                    .fetch_add(buf.len() as u64, Ordering::Relaxed);
                p.articles_downloaded.fetch_add(1, Ordering::Relaxed);
                Ok(buf)
            }
            (JobOutput::Stat(_), _) => Err(NntpError::Protocol("BODY answered as STAT")),
        }
    }

    pub async fn stat(&self, message_id: &str) -> Result<bool, NntpError> {
        match self
            .pool
            .try_each(self.lane, || JobKind::Stat(message_id.to_string()))
            .await?
        {
            (JobOutput::Stat(exists), _) => Ok(exists),
            (JobOutput::Body(_), _) => Err(NntpError::Protocol("STAT answered as BODY")),
        }
    }

    pub fn capacity(&self) -> usize {
        self.pool.total_capacity()
    }

}

impl NntpPool {
    pub fn new_multi(mut providers: Vec<NntpProvider>) -> Arc<Self> {
        providers.sort_by(|a, b| {
            a.is_backup
                .cmp(&b.is_backup)
                .then(a.priority.cmp(&b.priority))
        });
        let providers: Vec<Arc<ProviderRt>> = providers
            .into_iter()
            .map(|p| {
                Arc::new(ProviderRt {
                    provider: p,
                    sched: Mutex::new(Sched::new()),
                    warm_notify: Notify::new(),
                    cold_notify: Notify::new(),
                    breaker: CircuitBreaker::new(),
                    bytes_downloaded: AtomicU64::new(0),
                    articles_downloaded: AtomicU64::new(0),
                })
            })
            .collect();
        let active_streams = Arc::new(AtomicUsize::new(0));
        let pool = Arc::new(Self {
            providers,
            active_streams: active_streams.clone(),
        });
        for rt in &pool.providers {
            for slot_idx in 0..rt.capacity() {
                tokio::spawn(run_slot(rt.clone(), active_streams.clone(), slot_idx));
            }
        }
        pool
    }

    /// Client for reads a player is blocked on: absolute priority.
    pub fn playback_client(self: &Arc<Self>) -> NntpClient {
        NntpClient {
            pool: self.clone(),
            lane: Lane::Hot,
        }
    }

    /// Client for a live stream's read-ahead fill and head/tail precache.
    /// Yields only to blocked playback reads, never to bulk work.
    pub fn stream_client(self: &Arc<Self>) -> NntpClient {
        NntpClient {
            pool: self.clone(),
            lane: Lane::Stream,
        }
    }

    /// Client for import, health, precache, and repair jobs. Admission
    /// shrinks automatically while streams are active.
    pub fn bulk_client(self: &Arc<Self>) -> NntpClient {
        NntpClient {
            pool: self.clone(),
            lane: Lane::Bulk,
        }
    }

    pub fn stream_started(&self) {
        self.active_streams.fetch_add(1, Ordering::AcqRel);
    }

    pub fn stream_ended(&self) {
        let _ = self
            .active_streams
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
        // The bulk budget just grew; wake parked slots to drain any backlog.
        for rt in &self.providers {
            rt.wake_for_bulk();
        }
    }

    /// Per-provider health snapshot in pool order.
    pub fn health(&self) -> Vec<ProviderHealth> {
        self.providers
            .iter()
            .map(|rt| {
                let s = rt.sched.lock();
                ProviderHealth {
                    host: rt.provider.config.host.clone(),
                    port: rt.provider.config.port,
                    priority: rt.provider.priority,
                    is_backup: rt.provider.is_backup,
                    max_connections: rt.provider.config.max_connections,
                    open_connections: s.open as u32,
                    idle_connections: s.warm_parked as u32,
                    active_connections: s.active as u32,
                    breaker_tripped: rt.breaker.is_tripped(),
                    cooldown_seconds_remaining: rt.breaker.cooldown_remaining_secs(),
                    consecutive_failures: rt.breaker.consecutive_failures.load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    /// Per-provider session traffic counters, in pool order.
    pub fn traffic_snapshot(&self) -> Vec<ProviderTraffic> {
        self.providers
            .iter()
            .map(|rt| ProviderTraffic {
                host: rt.provider.config.host.clone(),
                bytes_downloaded: rt.bytes_downloaded.load(Ordering::Relaxed),
                articles_downloaded: rt.articles_downloaded.load(Ordering::Relaxed),
            })
            .collect()
    }

    pub fn total_capacity(&self) -> usize {
        self.providers
            .iter()
            .filter(|rt| !rt.provider.is_backup)
            .map(|rt| rt.capacity())
            .sum::<usize>()
            .max(1)
    }

    pub fn download_concurrency(&self) -> usize {
        self.total_capacity()
    }

    /// Run one job against providers in health/priority order: healthy
    /// providers first, tripped ones as a last-resort probe. `ArticleNotFound`
    /// moves on to the next provider; transport errors record a breaker
    /// failure and move on.
    async fn try_each(
        &self,
        lane: Lane,
        make_kind: impl Fn() -> JobKind,
    ) -> Result<(JobOutput, usize), NntpError> {
        let mut order: Vec<usize> = Vec::with_capacity(self.providers.len());
        let mut tripped: Vec<usize> = Vec::new();
        for (idx, rt) in self.providers.iter().enumerate() {
            if rt.breaker.is_tripped() {
                tripped.push(idx);
            } else {
                order.push(idx);
            }
        }
        order.extend(tripped);

        let mut not_found = false;
        let mut last_err: Option<NntpError> = None;
        for idx in order {
            let rt = &self.providers[idx];
            let (tx, rx) = oneshot::channel();
            rt.submit(Job {
                kind: make_kind(),
                lane,
                reply: tx,
            });
            match rx.await {
                Ok(Ok(out)) => {
                    rt.breaker.record_success();
                    return Ok((out, idx));
                }
                Ok(Err(NntpError::ArticleNotFound(s))) => {
                    not_found = true;
                    last_err = Some(NntpError::ArticleNotFound(s));
                }
                Ok(Err(e)) => {
                    tracing::debug!(
                        host = %rt.provider.config.host,
                        backup = rt.provider.is_backup,
                        error = %e,
                        "NNTP op failed; trying next provider"
                    );
                    rt.breaker.record_failure(&rt.provider.config.host);
                    last_err = Some(e);
                }
                Err(_) => {
                    last_err = Some(NntpError::Protocol("nntp pool shut down"));
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
}

impl Drop for NntpPool {
    fn drop(&mut self) {
        for rt in &self.providers {
            let pending: Vec<Job> = {
                let mut s = rt.sched.lock();
                s.shutdown = true;
                let mut pending: Vec<Job> = s.hot.drain(..).collect();
                pending.extend(s.stream.drain(..));
                pending.extend(s.bulk.drain(..));
                pending
            };
            for job in pending {
                let _ = job
                    .reply
                    .send(Err(NntpError::Protocol("nntp pool shut down")));
            }
            rt.warm_notify.notify_waiters();
            rt.cold_notify.notify_waiters();
        }
    }
}

/// Dial with backoff on the provider's own "too many connections" answer —
/// that's the account at capacity, not the provider down, so it must never
/// trip the breaker or fail the job outright.
async fn dial(rt: &ProviderRt) -> Result<NntpConnection, NntpError> {
    let mut retries: u32 = 0;
    loop {
        let started = Instant::now();
        match NntpConnection::connect(&rt.provider.config).await {
            Ok(conn) => {
                let connect_ms = started.elapsed().as_millis();
                if connect_ms > 50 {
                    tracing::debug!(
                        host = %rt.provider.config.host,
                        connect_ms,
                        "NNTP slot dialed"
                    );
                }
                return Ok(conn);
            }
            Err(NntpError::TooManyConnections(status)) => {
                retries += 1;
                if retries > TOO_MANY_CONNECTIONS_MAX_RETRIES {
                    return Err(NntpError::TooManyConnections(status));
                }
                tracing::debug!(
                    host = %rt.provider.config.host,
                    status,
                    attempt = retries,
                    "NNTP provider at its own connection limit; backing off"
                );
                tokio::time::sleep(TOO_MANY_CONNECTIONS_BACKOFF).await;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn execute(conn: &mut NntpConnection, kind: &JobKind) -> Result<JobOutput, NntpError> {
    match kind {
        JobKind::Body(mid) => conn.fetch_body(mid).await.map(JobOutput::Body),
        JobKind::Stat(mid) => conn.stat(mid).await.map(JobOutput::Stat),
    }
}

/// One slot actor: owns at most one live connection for its provider and
/// serves jobs until pool shutdown. All scheduling state transitions happen
/// under the provider's single sched mutex.
async fn run_slot(rt: Arc<ProviderRt>, active_streams: Arc<AtomicUsize>, slot_idx: usize) {
    let mut conn: Option<NntpConnection> = None;
    let mut last_used = Instant::now();

    loop {
        // Claim the highest-priority eligible job, or decide to park.
        let (job, backlog) = {
            let mut s = rt.sched.lock();
            if s.shutdown {
                if conn.is_some() {
                    s.open -= 1;
                }
                return;
            }
            let job = s.pop(rt.capacity(), active_streams.load(Ordering::Acquire));
            if job.is_some() {
                s.active += 1;
            }
            let backlog = !(s.hot.is_empty() && s.stream.is_empty());
            (job, backlog)
        };

        // `Notify` stores at most one wake permit, so a submission burst can
        // wake fewer slots than it queued jobs. Chain the wake: any slot that
        // claims work while more is queued wakes the next parked slot, so a
        // backlog fans out across slots instead of draining serially.
        if job.is_some() && backlog {
            let s = rt.sched.lock();
            if s.warm_parked > 0 {
                rt.warm_notify.notify_one();
            } else if s.cold_parked > 0 {
                rt.cold_notify.notify_one();
            }
        }

        let Some(job) = job else {
            park(&rt, &mut conn, slot_idx, last_used).await;
            continue;
        };

        // The requester may have gone away (obsolete read-ahead window,
        // cancelled caller) — don't spend a socket on it.
        if job.reply.is_closed() {
            finish_job(&rt, job.lane);
            continue;
        }

        // Ensure a live connection.
        if conn.is_none() {
            match dial(&rt).await {
                Ok(c) => {
                    conn = Some(c);
                    rt.sched.lock().open += 1;
                }
                Err(e) => {
                    let lane = job.lane;
                    let _ = job.reply.send(Err(e));
                    finish_job(&rt, lane);
                    // Don't spin every cold slot against an unreachable
                    // provider: park; the next submission re-wakes us.
                    continue;
                }
            }
        }

        let c = conn.as_mut().expect("connection ensured above");
        let result = execute(c, &job.kind).await;
        last_used = Instant::now();

        // A transport-level failure poisons the connection: drop it so the
        // next job dials fresh. `ArticleNotFound` leaves the wire clean.
        let drop_conn = !matches!(result, Ok(_) | Err(NntpError::ArticleNotFound(_)));
        if drop_conn && let Some(mut dead) = conn.take() {
            dead.quit().await;
            rt.sched.lock().open -= 1;
        }

        let lane = job.lane;
        let _ = job.reply.send(result);
        finish_job(&rt, lane);
    }
}

/// Post-job bookkeeping: clear the active flag, release bulk admission, and
/// wake another slot if freed budget makes a queued bulk job eligible.
fn finish_job(rt: &ProviderRt, lane: Lane) {
    let wake_bulk = {
        let mut s = rt.sched.lock();
        s.active -= 1;
        if lane == Lane::Bulk {
            s.bulk_inflight -= 1;
        }
        lane == Lane::Bulk && !s.bulk.is_empty()
    };
    if wake_bulk {
        rt.wake_for_bulk();
    }
}

/// Park this slot until new work arrives. Warm slots keep their connection
/// alive with `DATE` keepalives and idle-close beyond the warm floor; cold
/// slots simply sleep until warm capacity is saturated and they're woken.
async fn park(
    rt: &ProviderRt,
    conn: &mut Option<NntpConnection>,
    slot_idx: usize,
    last_used: Instant,
) {
    if conn.is_some() {
        let notified = rt.warm_notify.notified();
        rt.sched.lock().warm_parked += 1;
        let woken = tokio::time::timeout(KEEPALIVE_INTERVAL, notified)
            .await
            .is_ok();
        rt.sched.lock().warm_parked -= 1;
        if woken {
            return;
        }
        // Keepalive tick. Past the warm floor, long-idle connections are
        // returned to the OS; the floor keeps a base set permanently hot.
        if slot_idx >= WARM_FLOOR && last_used.elapsed() >= IDLE_CLOSE {
            if let Some(mut c) = conn.take() {
                c.quit().await;
                rt.sched.lock().open -= 1;
            }
            return;
        }
        // Note: the keepalive deliberately does NOT refresh `last_used` —
        // only real jobs do. Otherwise the ping itself would keep resetting
        // the idle clock and idle-close could never fire.
        let c = conn.as_mut().expect("warm slot holds a connection");
        let alive = matches!(
            tokio::time::timeout(PING_TIMEOUT, c.date()).await,
            Ok(Ok(()))
        );
        if !alive {
            drop(conn.take());
            rt.sched.lock().open -= 1;
        }
    } else {
        let notified = rt.cold_notify.notified();
        rt.sched.lock().cold_parked += 1;
        // The timeout is a shutdown/lost-wake safety net: a cold slot woken
        // by nothing simply re-checks the queues and re-parks.
        let _ = tokio::time::timeout(Duration::from_secs(60), notified).await;
        rt.sched.lock().cold_parked -= 1;
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;
    use crate::nntp::{NntpProvider, NntpServerConfig};

    /// Loopback listener speaking enough NNTP for pool tests: `200` greeting,
    /// `111` to DATE, `223` to STAT (exists), `430` to BODY (not found).
    async fn spawn_fake_nntp_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
                    let (r, mut w) = socket.into_split();
                    if w.write_all(b"200 fake nntp ready\r\n").await.is_err() {
                        return;
                    }
                    let mut lines = BufReader::new(r).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let reply: &[u8] = if line.starts_with("DATE") {
                            b"111 20260101000000\r\n"
                        } else if line.starts_with("STAT") {
                            b"223 0 <exists>\r\n"
                        } else if line.starts_with("BODY") {
                            b"430 no such article\r\n"
                        } else if line.starts_with("QUIT") {
                            let _ = w.write_all(b"205 bye\r\n").await;
                            return;
                        } else {
                            b"500 what\r\n"
                        };
                        if w.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, handle)
    }

    fn test_provider(addr: std::net::SocketAddr, max_connections: u32) -> NntpProvider {
        NntpProvider {
            config: NntpServerConfig {
                host: addr.ip().to_string(),
                port: addr.port(),
                user: None,
                pass: None,
                use_tls: false,
                max_connections,
                timeout: Duration::from_secs(5),
            },
            priority: 0,
            is_backup: false,
        }
    }

    fn dummy_job(lane: Lane) -> (Job, oneshot::Receiver<Result<JobOutput, NntpError>>) {
        let (tx, rx) = oneshot::channel();
        (
            Job {
                kind: JobKind::Stat("x@test".into()),
                lane,
                reply: tx,
            },
            rx,
        )
    }

    #[test]
    fn pop_serves_hot_before_stream_before_bulk() {
        let mut s = Sched::new();
        let (bulk, _b) = dummy_job(Lane::Bulk);
        let (stream, _s) = dummy_job(Lane::Stream);
        let (hot, _h) = dummy_job(Lane::Hot);
        s.bulk.push_back(bulk);
        s.stream.push_back(stream);
        s.hot.push_back(hot);

        assert_eq!(s.pop(4, 0).unwrap().lane, Lane::Hot);
        assert_eq!(s.pop(4, 0).unwrap().lane, Lane::Stream);
        assert_eq!(s.pop(4, 0).unwrap().lane, Lane::Bulk);
        assert!(s.pop(4, 0).is_none());
    }

    #[test]
    fn bulk_admission_shrinks_while_streams_active() {
        let mut s = Sched::new();
        for _ in 0..4 {
            let (job, rx) = dummy_job(Lane::Bulk);
            std::mem::forget(rx);
            s.bulk.push_back(job);
        }
        // capacity 4, one stream → reserve 2 → bulk cap 2.
        assert!(s.pop(4, 1).is_some());
        assert!(s.pop(4, 1).is_some());
        assert!(s.pop(4, 1).is_none(), "third bulk job must wait");
        // Stream gone → cap restored.
        assert!(s.pop(4, 0).is_some());
        // Hot is never budget-gated.
        let (hot, _h) = dummy_job(Lane::Hot);
        s.hot.push_back(hot);
        assert_eq!(s.pop(4, 1).unwrap().lane, Lane::Hot);
    }

    #[test]
    fn bulk_reserve_never_exceeds_capacity() {
        let mut s = Sched::new();
        let (job, rx) = dummy_job(Lane::Bulk);
        std::mem::forget(rx);
        s.bulk.push_back(job);
        // capacity 1, many streams: cap clamps to 1, job still eligible.
        assert!(s.pop(1, 50).is_some());
    }

    #[tokio::test]
    async fn stat_round_trips_through_slot_actor() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = NntpPool::new_multi(vec![test_provider(addr, 2)]);
        let client = pool.bulk_client();
        let exists = tokio::time::timeout(Duration::from_secs(2), client.stat("abc@test"))
            .await
            .expect("stat must not hang")
            .unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn body_not_found_maps_to_article_not_found() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = NntpPool::new_multi(vec![test_provider(addr, 1)]);
        let client = pool.playback_client();
        let result = tokio::time::timeout(Duration::from_secs(2), client.fetch_body("abc@test"))
            .await
            .expect("fetch must not hang");
        assert!(matches!(result, Err(NntpError::ArticleNotFound(_))));
    }

    #[tokio::test]
    async fn many_concurrent_stats_share_bounded_slots() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = NntpPool::new_multi(vec![test_provider(addr, 3)]);
        let run = async {
            let mut handles = Vec::new();
            for _ in 0..64 {
                let client = pool.bulk_client();
                handles.push(tokio::spawn(async move { client.stat("x@test").await }));
            }
            for h in handles {
                assert!(h.await.unwrap().unwrap());
            }
        };
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("burst of stats must drain through 3 slots without stalling");
    }

    #[tokio::test]
    async fn connections_stay_warm_between_jobs() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = NntpPool::new_multi(vec![test_provider(addr, 2)]);
        let client = pool.stream_client();
        client.stat("a@test").await.unwrap();
        client.stat("b@test").await.unwrap();
        // The reply arrives before the slot's post-job bookkeeping, so poll
        // briefly rather than assert on an instantaneous snapshot.
        for _ in 0..100 {
            let health = &pool.health()[0];
            if health.open_connections >= 1 && health.active_connections == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let health = &pool.health()[0];
        panic!(
            "connections did not settle warm: open={} active={}",
            health.open_connections, health.active_connections
        );
    }
}
