//! Per-provider connection pool.
//!
//! One pool per configured provider. A semaphore enforces the provider's
//! `max_connections`; connections are created lazily, reused while warm, and
//! reaped once they have been idle long enough that the provider would drop
//! them anyway.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use super::client::{NntpClient, Traffic};
use super::{NntpError, NntpProvider, NntpServerConfig};

/// How often the reaper sweeps the idle queue.
const REAP_INTERVAL: Duration = Duration::from_secs(15);
/// Idle connections older than this are closed. Commercial providers silently
/// drop idle sockets at around this age, so holding them longer only means
/// discovering the drop on the next command instead of here.
const MAX_IDLE: Duration = Duration::from_secs(30);
/// Waits longer than this mean the account limit is the bottleneck, which is
/// worth seeing in logs.
const SLOW_ACQUIRE: Duration = Duration::from_millis(250);
/// Consecutive `430`s before a provider is tried after its healthier peers.
const NOT_FOUND_DEMOTE_THRESHOLD: u32 = 3;
/// Consecutive successes that clear a demotion.
const SUCCESS_PROMOTE_THRESHOLD: u32 = 10;

pub struct ClientPool {
    config: Arc<NntpServerConfig>,
    priority: i32,
    is_backup: bool,
    slots: Arc<Semaphore>,
    idle: Mutex<Vec<NntpClient>>,
    traffic: Arc<Traffic>,
    open: AtomicUsize,
    leased: AtomicUsize,
    consecutive_not_found: AtomicU32,
    consecutive_success: AtomicU32,
}

impl ClientPool {
    pub fn new(provider: NntpProvider) -> Arc<Self> {
        let pool = Self::unreaped(provider);
        spawn_reaper(Arc::downgrade(&pool));
        pool
    }

    /// A pool with no background reaper, so a test can age the idle queue past
    /// [`MAX_IDLE`] and still observe what [`ClientPool::acquire`] does with it.
    /// Advancing a paused clock fires the reaper's timer, which would otherwise
    /// clean up first and hide the checkout path entirely.
    fn unreaped(provider: NntpProvider) -> Arc<Self> {
        let capacity = provider.config.max_connections.max(1) as usize;
        Arc::new(Self {
            config: Arc::new(provider.config),
            priority: provider.priority,
            is_backup: provider.is_backup,
            slots: Arc::new(Semaphore::new(capacity)),
            idle: Mutex::new(Vec::new()),
            traffic: Arc::new(Traffic::default()),
            open: AtomicUsize::new(0),
            leased: AtomicUsize::new(0),
            consecutive_not_found: AtomicU32::new(0),
            consecutive_success: AtomicU32::new(0),
        })
    }

    pub fn host(&self) -> &str {
        &self.config.host
    }

    pub fn priority(&self) -> i32 {
        self.priority
    }

    pub fn is_backup(&self) -> bool {
        self.is_backup
    }

    pub fn capacity(&self) -> usize {
        self.config.max_connections.max(1) as usize
    }

    pub fn traffic(&self) -> &Traffic {
        &self.traffic
    }

    /// True while this provider is being skipped past healthier ones because
    /// it keeps answering `430` for articles others can serve.
    pub fn is_demoted(&self) -> bool {
        self.consecutive_not_found.load(Ordering::Relaxed) >= NOT_FOUND_DEMOTE_THRESHOLD
    }

    pub fn consecutive_not_found(&self) -> u32 {
        self.consecutive_not_found.load(Ordering::Relaxed)
    }

    pub fn record_not_found(&self) {
        self.consecutive_success.store(0, Ordering::Relaxed);
        let count = self
            .consecutive_not_found
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if count == NOT_FOUND_DEMOTE_THRESHOLD {
            tracing::info!(
                host = %self.config.host,
                consecutive_not_found = count,
                "nntp provider demoted behind healthier providers"
            );
        }
    }

    pub fn record_success(&self) {
        let count = self
            .consecutive_success
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if count >= SUCCESS_PROMOTE_THRESHOLD {
            self.consecutive_success.store(0, Ordering::Relaxed);
            if self.consecutive_not_found.swap(0, Ordering::Relaxed) >= NOT_FOUND_DEMOTE_THRESHOLD {
                tracing::info!(host = %self.config.host, "nntp provider promoted after sustained success");
            }
        }
    }

    /// Borrow a connection: an idle one if there is one, a freshly dialed one
    /// if the provider's limit leaves room, otherwise wait for whichever
    /// happens first.
    pub async fn acquire(self: &Arc<Self>) -> Result<Lease, NntpError> {
        let started = Instant::now();
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_closed| NntpError::Protocol("nntp pool closed"))?;
        let waited = started.elapsed();
        if waited >= SLOW_ACQUIRE {
            tracing::debug!(
                host = %self.config.host,
                wait_ms = waited.as_millis(),
                capacity = self.capacity(),
                "waited for an nntp connection slot"
            );
        }

        // Age is checked *here*, not only in [`ClientPool::reap`]. The reaper
        // runs on an interval, so trusting it alone hands out connections up to
        // `REAP_INTERVAL` past `MAX_IDLE` — and a socket the provider has
        // already dropped costs a reconnect plus a whole re-fetch at best. At
        // worst the FIN never arrives, the `BODY` write goes into a half-open
        // socket, and the read waits on TCP retransmission for minutes: three
        // articles measured at 182-189 s against a provider whose own p99 for
        // the same articles is 1.7 s. Discarding on the way out costs one
        // comparison. Walk the whole idle stack, because every entry below a
        // stale one is older still.
        let mut stale = 0usize;
        let reused = loop {
            let Some(client) = self.idle.lock().pop() else {
                break None;
            };
            if Instant::now().duration_since(client.last_used()) < MAX_IDLE {
                break Some(client);
            }
            drop(client);
            stale += 1;
        };
        if stale > 0 {
            self.open.fetch_sub(stale, Ordering::Relaxed);
            tracing::debug!(
                host = %self.config.host,
                stale,
                "closed idle nntp connections the provider had likely dropped"
            );
        }
        if let Some(mut client) = reused {
            client.touch();
            self.leased.fetch_add(1, Ordering::Relaxed);
            return Ok(Lease {
                client: Some(client),
                pool: self.clone(),
                permit: Some(permit),
            });
        }

        let client = NntpClient::connect(self.config.clone(), self.traffic.clone()).await?;
        self.open.fetch_add(1, Ordering::Relaxed);
        self.leased.fetch_add(1, Ordering::Relaxed);
        Ok(Lease {
            client: Some(client),
            pool: self.clone(),
            permit: Some(permit),
        })
    }

    fn release(&self, client: NntpClient) {
        if client.is_poisoned() {
            self.discard();
            return;
        }
        self.leased.fetch_sub(1, Ordering::Relaxed);
        self.idle.lock().push(client);
    }

    fn discard(&self) {
        self.leased.fetch_sub(1, Ordering::Relaxed);
        self.open.fetch_sub(1, Ordering::Relaxed);
    }

    fn reap(&self) {
        let now = Instant::now();
        let mut idle = self.idle.lock();
        let before = idle.len();
        idle.retain(|client| now.duration_since(client.last_used()) < MAX_IDLE);
        let closed = before - idle.len();
        drop(idle);
        if closed > 0 {
            self.open.fetch_sub(closed, Ordering::Relaxed);
        }
    }

    pub fn health(&self) -> ProviderHealth {
        let idle = self.idle.lock().len();
        ProviderHealth {
            host: self.config.host.clone(),
            port: self.config.port,
            priority: self.priority,
            is_backup: self.is_backup,
            max_connections: self.config.max_connections,
            open_connections: self.open.load(Ordering::Relaxed) as u32,
            idle_connections: idle as u32,
            active_connections: self.leased.load(Ordering::Relaxed) as u32,
            demoted: self.is_demoted(),
            consecutive_not_found: self.consecutive_not_found(),
        }
    }
}

fn spawn_reaper(pool: Weak<ClientPool>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(REAP_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let Some(pool) = pool.upgrade() else {
                return;
            };
            pool.reap();
        }
    });
}

/// A borrowed connection. Dropping it returns the connection to the idle
/// queue, or closes it when its wire state is uncertain.
pub struct Lease {
    client: Option<NntpClient>,
    pool: Arc<ClientPool>,
    permit: Option<OwnedSemaphorePermit>,
}

impl std::ops::Deref for Lease {
    type Target = NntpClient;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("lease holds a client")
    }
}

impl std::ops::DerefMut for Lease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("lease holds a client")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.pool.release(client);
        }
        drop(self.permit.take());
    }
}

/// Read-only health snapshot of one provider, for the API's provider view.
/// Carries no credentials.
#[derive(Debug, Clone)]
pub struct ProviderHealth {
    pub host: String,
    pub port: u16,
    pub priority: i32,
    pub is_backup: bool,
    pub max_connections: u32,
    pub open_connections: u32,
    pub idle_connections: u32,
    pub active_connections: u32,
    pub demoted: bool,
    pub consecutive_not_found: u32,
}

/// Per-provider session download counters (since process start).
#[derive(Debug, Clone)]
pub struct ProviderTraffic {
    pub host: String,
    pub bytes_downloaded: u64,
    pub articles_downloaded: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nntp::tests::spawn_fake_nntp_server;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

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

    #[tokio::test]
    async fn idle_connections_are_reused() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = ClientPool::new(test_provider(addr, 4));

        {
            let mut lease = pool.acquire().await.unwrap();
            assert!(lease.stat("a@test").await.unwrap());
        }
        assert_eq!(pool.health().open_connections, 1);
        assert_eq!(pool.health().idle_connections, 1);

        {
            let mut lease = pool.acquire().await.unwrap();
            assert!(lease.stat("b@test").await.unwrap());
        }
        assert_eq!(
            pool.health().open_connections,
            1,
            "second acquire must reuse the idle connection, not dial"
        );
    }

    #[tokio::test]
    async fn concurrent_borrowers_never_exceed_capacity() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = ClientPool::new(test_provider(addr, 3));

        let mut handles = Vec::new();
        for _ in 0..24 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let mut lease = pool.acquire().await.unwrap();
                lease.stat("x@test").await.unwrap()
            }));
        }
        for handle in handles {
            assert!(handle.await.unwrap());
        }
        assert!(
            pool.health().open_connections <= 3,
            "opened {} connections for a 3-connection provider",
            pool.health().open_connections
        );
    }

    #[tokio::test]
    async fn cancelled_command_discards_connection_with_unread_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (command_seen_tx, command_seen_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            stream.get_mut().write_all(b"200 ready\r\n").await.unwrap();
            stream.get_mut().flush().await.unwrap();
            let mut command = String::new();
            stream.read_line(&mut command).await.unwrap();
            assert!(command.starts_with("BODY "));
            assert!(command_seen_tx.send(()).is_ok());
            std::future::pending::<()>().await;
        });

        let pool = ClientPool::new(test_provider(addr, 1));
        let task_pool = pool.clone();
        let command = tokio::spawn(async move {
            let mut lease = task_pool.acquire().await.unwrap();
            lease.body("cancel@test").await
        });
        command_seen_rx.await.unwrap();
        command.abort();
        drop(command.await);
        tokio::task::yield_now().await;

        let health = pool.health();
        assert_eq!(health.idle_connections, 0);
        assert_eq!(health.open_connections, 0);
        server.abort();
    }

    #[tokio::test]
    async fn reaper_closes_stale_idle_connections() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = ClientPool::new(test_provider(addr, 2));
        {
            let mut lease = pool.acquire().await.unwrap();
            assert!(lease.stat("a@test").await.unwrap());
        }
        assert_eq!(pool.health().idle_connections, 1);

        tokio::time::pause();
        tokio::time::advance(MAX_IDLE + Duration::from_secs(1)).await;
        pool.reap();
        assert_eq!(pool.health().idle_connections, 0);
        assert_eq!(pool.health().open_connections, 0);
    }

    /// The reaper is an interval sweep, so it is never the thing that decides
    /// whether *this* connection is safe to use. Between sweeps a socket the
    /// provider dropped would otherwise be handed straight to a `BODY`, and a
    /// half-open one takes the read down for minutes.
    #[tokio::test]
    async fn a_stale_idle_connection_is_never_handed_out() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        // No reaper: this test is about what `acquire` does between sweeps.
        let pool = ClientPool::unreaped(test_provider(addr, 4));

        // Two idle connections, so reuse and replacement are distinguishable:
        // reusing one leaves the other open, replacing closes both.
        {
            let mut first = pool.acquire().await.unwrap();
            let mut second = pool.acquire().await.unwrap();
            assert!(first.stat("a@test").await.unwrap());
            assert!(second.stat("b@test").await.unwrap());
        }
        assert_eq!(pool.health().open_connections, 2);
        assert_eq!(pool.health().idle_connections, 2);

        // Past the idle limit with no sweep in between — exactly the window
        // `REAP_INTERVAL` leaves open, and where the provider has already
        // dropped its side.
        tokio::time::pause();
        tokio::time::advance(MAX_IDLE + Duration::from_secs(1)).await;
        tokio::time::resume();

        let mut lease = pool.acquire().await.unwrap();
        assert!(lease.stat("c@test").await.unwrap());
        let health = pool.health();
        assert_eq!(
            health.open_connections, 1,
            "both stale connections must be closed and one dialled fresh, \
             not popped and reused"
        );
        assert_eq!(health.idle_connections, 0);
    }

    #[tokio::test]
    async fn demotion_follows_not_found_history() {
        let pool = ClientPool::new(test_provider(
            "127.0.0.1:1".parse::<std::net::SocketAddr>().unwrap(),
            1,
        ));
        assert!(!pool.is_demoted());
        for _ in 0..NOT_FOUND_DEMOTE_THRESHOLD {
            pool.record_not_found();
        }
        assert!(pool.is_demoted());
        for _ in 0..SUCCESS_PROMOTE_THRESHOLD {
            pool.record_success();
        }
        assert!(!pool.is_demoted());
    }
}
