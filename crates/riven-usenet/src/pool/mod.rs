//! Multi-provider segment pool.
//!
//! Sits above the per-provider [`ClientPool`]s and turns "give me this
//! message-id" into decoded bytes: permanent-missing check, decoded cache,
//! single-flight coalescing, then providers in priority order until one
//! answers.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use riven_core::cache::{ByteLru, SEGMENT};
use tokio::sync::oneshot;

use crate::nntp::{ClientPool, NntpError, NntpProvider, ProviderHealth, ProviderTraffic};
use crate::state::{FetchEntry, InFlight};
use crate::yenc;

mod missing;

pub use missing::MissingCache;

/// Decoded article bodies, keyed by message-id. **Staging, not retention:** the
/// VFS read-ahead cache above holds the same bytes at the same granularity, so
/// re-reads never reach here. What is left is holding an article between a warm
/// fetch landing and the walk that warmed it consuming the bytes.
///
/// Values are `Bytes`, so a hit slices a range with no copy. Which provider
/// served a segment is not stored: a hit is not traffic, and crediting one
/// would inflate that provider's usage figures.
pub type SegmentCache = ByteLru<Arc<str>, Bytes>;

/// Cap on the decoded-size memo. One `(message_id, u64)` entry per segment
/// ever fetched, ~80 bytes each.
const DECODED_SIZES_ENTRIES: usize = 500_000;

pub struct SegmentPool {
    /// Primaries first (by priority), then backups. Demoted providers are
    /// re-ordered per request rather than here.
    providers: Vec<Arc<ClientPool>>,
    cache: SegmentCache,
    missing: MissingCache,
    inflight: InFlight,
    decoded_sizes: DecodedSizes,
    metrics: FetchMetrics,
}

impl SegmentPool {
    pub fn new(mut providers: Vec<NntpProvider>) -> Arc<Self> {
        providers.sort_by(|a, b| {
            a.is_backup
                .cmp(&b.is_backup)
                .then(a.priority.cmp(&b.priority))
        });
        Arc::new(Self {
            providers: providers.into_iter().map(ClientPool::new).collect(),
            cache: SegmentCache::with_budget(SEGMENT),
            missing: MissingCache::default(),
            inflight: InFlight::default(),
            decoded_sizes: DecodedSizes::new(DECODED_SIZES_ENTRIES),
            metrics: FetchMetrics::default(),
        })
    }

    pub fn providers(&self) -> &[Arc<ClientPool>] {
        &self.providers
    }

    pub fn cache(&self) -> &SegmentCache {
        &self.cache
    }

    pub fn missing(&self) -> &MissingCache {
        &self.missing
    }

    pub fn metrics(&self) -> &FetchMetrics {
        &self.metrics
    }

    /// Decoded length of a segment we have fetched before, without refetching
    /// it. Needed to seek into the middle of a RAR volume.
    pub fn decoded_size(&self, message_id: &str) -> Option<u64> {
        self.decoded_sizes.get(message_id)
    }

    pub fn in_flight(&self) -> usize {
        self.inflight.len()
    }

    /// Sum of every primary provider's connection limit — the ceiling on how
    /// much work can be in flight against the wire at once.
    pub fn total_connections(&self) -> usize {
        self.providers
            .iter()
            .filter(|p| !p.is_backup())
            .map(|p| p.capacity())
            .sum::<usize>()
            .max(1)
    }

    pub fn health(&self) -> Vec<ProviderHealth> {
        self.providers.iter().map(|p| p.health()).collect()
    }

    pub fn traffic(&self) -> Vec<ProviderTraffic> {
        self.providers
            .iter()
            .map(|p| ProviderTraffic {
                host: p.host().to_string(),
                bytes_downloaded: p.traffic().bytes_read.load(Ordering::Relaxed),
                articles_downloaded: p.traffic().articles_read.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Providers in the order they should be tried: primaries before backups,
    /// and within each class, providers that keep answering `430` last.
    fn attempt_order(&self) -> Vec<&Arc<ClientPool>> {
        let mut order: Vec<&Arc<ClientPool>> = self.providers.iter().collect();
        order.sort_by_key(|p| (p.is_backup(), p.is_demoted(), p.priority()));
        order
    }

    /// Fetch and decode a segment. Coalesces concurrent callers for the same
    /// message-id onto one wire fetch.
    pub async fn fetch_segment(self: &Arc<Self>, message_id: &str) -> Result<Bytes, NntpError> {
        loop {
            if let Some(hit) = self.cache.get(message_id) {
                return Ok(hit);
            }
            if self.missing.contains(message_id) {
                return Err(NntpError::ArticleNotFound(
                    "previously confirmed missing on every provider".into(),
                ));
            }
            match self.inflight.enter_or_wait(message_id) {
                FetchEntry::Wait(slot) => {
                    slot.wait().await;
                }
                FetchEntry::Owner(slot, key) => {
                    let pool = self.clone();
                    let (result_tx, result_rx) = oneshot::channel();
                    tokio::spawn(async move {
                        let result = pool.fetch_sequential(&key).await;
                        pool.inflight.finish(&key, &slot);
                        drop(result_tx.send(result));
                    });
                    return result_rx
                        .await
                        .unwrap_or(Err(NntpError::Protocol("segment fetch task stopped")));
                }
            }
        }
    }

    /// Walk providers in order, stopping at the first one that serves the
    /// article. A provider that answers `430` is excluded and the next is
    /// tried; if every provider says `430` the id is permanently missing.
    async fn fetch_sequential(&self, message_id: &Arc<str>) -> Result<Bytes, NntpError> {
        let mut all_not_found = true;
        let mut attempted = false;
        let mut last_err: Option<NntpError> = None;

        for provider in self.attempt_order() {
            attempted = true;
            match self.fetch_from(provider, message_id).await {
                Ok(bytes) => {
                    provider.record_success();
                    return Ok(bytes);
                }
                Err(NntpError::ArticleNotFound(status)) => {
                    provider.record_not_found();
                    last_err = Some(NntpError::ArticleNotFound(status));
                }
                Err(error) => {
                    tracing::debug!(
                        host = provider.host(),
                        message_id = %message_id,
                        %error,
                        "nntp provider failed; excluding and trying the next"
                    );
                    all_not_found = false;
                    last_err = Some(error);
                }
            }
        }

        self.metrics.record_failed();
        if attempted && all_not_found {
            tracing::warn!(message_id = %message_id, "article missing on every provider");
            self.missing.insert(message_id);
        }
        Err(last_err.unwrap_or(NntpError::Protocol("no providers configured")))
    }

    async fn fetch_from(
        &self,
        provider: &Arc<ClientPool>,
        message_id: &Arc<str>,
    ) -> Result<Bytes, NntpError> {
        // Split so a slow article can be attributed. Raw NNTP against this
        // provider fetches a 722 KiB article in ~87 ms p50, so anything near a
        // second is riven's own cost and this says which part of it.
        let started = std::time::Instant::now();
        let mut lease = provider.acquire().await?;
        let acquire_us = started.elapsed().as_micros() as u64;

        let wire = std::time::Instant::now();
        let body = lease.body(message_id).await?;
        drop(lease);
        let body_us = wire.elapsed().as_micros() as u64;

        let blocking = std::time::Instant::now();
        let decoded = match tokio::task::spawn_blocking(move || yenc::decode(&body)).await {
            Ok(Ok((decoded, _info))) => decoded,
            Ok(Err(error)) => {
                tracing::warn!(message_id = %message_id, %error, "yenc decode failed");
                return Err(NntpError::Protocol("yenc decode failed"));
            }
            Err(error) => {
                tracing::warn!(message_id = %message_id, %error, "yenc decode task panicked");
                return Err(NntpError::Protocol("yenc decode task panicked"));
            }
        };

        let decode_us = blocking.elapsed().as_micros() as u64;
        let total_us = started.elapsed().as_micros() as u64;
        if total_us >= 300_000 {
            tracing::debug!(
                host = %provider.host(),
                bytes = decoded.len(),
                acquire_ms = acquire_us / 1000,
                body_ms = body_us / 1000,
                // Includes waiting for a blocking-pool thread, not just the
                // decode: saturation there would show up here and nowhere else.
                decode_ms = decode_us / 1000,
                total_ms = total_us / 1000,
                "slow article fetch"
            );
        }

        self.metrics.record_ok(decoded.len() as u64);
        self.decoded_sizes
            .put(message_id.clone(), decoded.len() as u64);
        self.cache
            .put(message_id.clone(), decoded.clone(), decoded.len() as u64);
        Ok(decoded)
    }

    /// STAT a batch over **one** connection from the healthiest provider, with
    /// every command written before any reply is read. A sweep of `n` articles
    /// then costs one connection and one round trip instead of `n` of each.
    ///
    /// Ids that provider reports missing are re-checked across every provider,
    /// since "missing" is only true when they all agree.
    pub async fn stat_batch(&self, message_ids: &[String]) -> Result<Vec<bool>, NntpError> {
        let Some(provider) = self.attempt_order().into_iter().next() else {
            return Err(NntpError::Protocol("no providers configured"));
        };
        let mut present = {
            let mut lease = provider.acquire().await?;
            lease.stat_many(message_ids).await?
        };
        for (index, found) in present.iter_mut().enumerate() {
            if !*found {
                *found = self.stat_segment(&message_ids[index]).await?;
            }
        }
        Ok(present)
    }

    /// `STAT` a message-id, trying providers in order. `Ok(false)` only once
    /// every provider agrees the article is gone.
    pub async fn stat_segment(&self, message_id: &str) -> Result<bool, NntpError> {
        if self.missing.contains(message_id) {
            return Ok(false);
        }
        let mut last_err: Option<NntpError> = None;
        let mut all_reported = true;
        for provider in self.attempt_order() {
            let mut lease = match provider.acquire().await {
                Ok(lease) => lease,
                Err(error) => {
                    all_reported = false;
                    last_err = Some(error);
                    continue;
                }
            };
            match lease.stat(message_id).await {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(error) => {
                    all_reported = false;
                    last_err = Some(error);
                }
            }
        }
        if all_reported {
            return Ok(false);
        }
        Err(last_err.unwrap_or(NntpError::Protocol("no providers configured")))
    }
}

/// Memoized decoded sizes keyed by message-id. An evicted entry only costs a
/// fallback to walking segments, never correctness.
struct DecodedSizes {
    inner: Mutex<LruCache<Arc<str>, u64>>,
}

impl DecodedSizes {
    fn new(max_entries: usize) -> Self {
        let capacity = NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
        }
    }

    fn get(&self, message_id: &str) -> Option<u64> {
        self.inner.lock().get(message_id).copied()
    }

    fn put(&self, message_id: Arc<str>, size: u64) {
        self.inner.lock().put(message_id, size);
    }
}

/// Cumulative counters for fetches that actually hit the wire.
#[derive(Default)]
pub struct FetchMetrics {
    ok: AtomicU64,
    failed: AtomicU64,
    bytes_decoded: AtomicU64,
}

impl FetchMetrics {
    fn record_ok(&self, decoded_bytes: u64) {
        self.ok.fetch_add(1, Ordering::Relaxed);
        self.bytes_decoded
            .fetch_add(decoded_bytes, Ordering::Relaxed);
    }

    fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ok(&self) -> u64 {
        self.ok.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    pub fn bytes_decoded(&self) -> u64 {
        self.bytes_decoded.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::nntp::NntpServerConfig;
    use crate::nntp::tests::{FAKE_SEGMENT_PAYLOAD, spawn_fake_nntp_server};

    fn provider(addr: std::net::SocketAddr, max_connections: u32, priority: i32) -> NntpProvider {
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
            priority,
            is_backup: false,
        }
    }

    #[tokio::test]
    async fn fetches_and_decodes_a_segment() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 2, 0)]);
        let bytes = pool.fetch_segment("a@test").await.unwrap();
        assert_eq!(bytes.as_ref(), FAKE_SEGMENT_PAYLOAD);
        assert_eq!(pool.decoded_size("a@test"), Some(bytes.len() as u64));
    }

    #[tokio::test]
    async fn second_fetch_is_served_from_cache() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 2, 0)]);
        pool.fetch_segment("a@test").await.unwrap();
        pool.fetch_segment("a@test").await.unwrap();
        assert_eq!(pool.metrics().ok(), 1, "cache hit must not re-fetch");
        assert_eq!(pool.cache().stats().hits, 1);
    }

    #[tokio::test]
    async fn concurrent_fetches_coalesce_to_one_wire_request() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 8, 0)]);

        let mut handles = Vec::new();
        for _ in 0..16 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                pool.fetch_segment("same@test").await
            }));
        }
        for handle in handles {
            assert_eq!(
                handle.await.unwrap().unwrap().as_ref(),
                FAKE_SEGMENT_PAYLOAD
            );
        }
        assert_eq!(pool.metrics().ok(), 1);
    }

    #[tokio::test]
    async fn cancelled_reader_leaves_fetch_for_later_reader() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 1, 0)]);
        let held_connection = pool.providers()[0].acquire().await.unwrap();

        let first_pool = pool.clone();
        let first = tokio::spawn(async move { first_pool.fetch_segment("same@test").await });
        while pool.in_flight() == 0 {
            tokio::task::yield_now().await;
        }
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        drop(held_connection);
        let bytes = pool.fetch_segment("same@test").await.unwrap();
        assert_eq!(bytes.as_ref(), FAKE_SEGMENT_PAYLOAD);
        assert_eq!(
            pool.metrics().ok(),
            1,
            "wire fetch must survive cancellation"
        );
    }

    #[tokio::test]
    async fn missing_on_every_provider_is_cached_permanently() {
        let (addr, _server) = spawn_missing_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 2, 0)]);

        assert!(matches!(
            pool.fetch_segment("gone@test").await,
            Err(NntpError::ArticleNotFound(_))
        ));
        assert!(pool.missing().contains("gone@test"));
        assert!(matches!(
            pool.fetch_segment("gone@test").await,
            Err(NntpError::ArticleNotFound(_))
        ));
        assert_eq!(
            pool.providers()[0].consecutive_not_found(),
            1,
            "the second call must short-circuit before touching the provider"
        );
    }

    #[tokio::test]
    async fn falls_over_to_the_next_provider_on_not_found() {
        let (missing_addr, _a) = spawn_missing_server().await;
        let (ok_addr, _b) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(missing_addr, 2, 0), provider(ok_addr, 2, 1)]);

        let bytes = pool.fetch_segment("a@test").await.unwrap();
        assert_eq!(bytes.as_ref(), FAKE_SEGMENT_PAYLOAD);
        assert_eq!(pool.providers()[0].consecutive_not_found(), 1);
        assert!(!pool.missing().contains("a@test"));
    }

    /// Loopback listener that answers `430` to everything article-shaped.
    async fn spawn_missing_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let (read_half, mut write_half) = socket.into_split();
                    if write_half.write_all(b"200 fake\r\n").await.is_err() {
                        return;
                    }
                    let mut lines = BufReader::new(read_half).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let reply: &[u8] = if line.starts_with("BODY") || line.starts_with("STAT") {
                            b"430 no such article\r\n"
                        } else if line.starts_with("QUIT") {
                            return;
                        } else {
                            b"111 20260101000000\r\n"
                        };
                        if write_half.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, handle)
    }
}
