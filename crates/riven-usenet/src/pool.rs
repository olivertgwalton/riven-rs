//! Multi-provider segment pool.
//!
//! Sits above the per-provider [`ClientPool`]s and turns "give me this
//! message-id" into decoded bytes: permanent-missing check, decoded cache,
//! single-flight coalescing, then providers in priority order until one
//! answers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use riven_core::cache::{ByteLru, SEGMENT, SEGMENT_SIZES};
use tokio::sync::oneshot;
use tokio::time::Instant;

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

/// Articles written to one connection before its replies are read.
///
/// This is a **memory** bound, not a throughput one. A pipelined batch holds
/// every reply it has read until the batch completes, so the depth times the
/// article size is resident at once — 16 MiB at the 4 MiB worst-case article,
/// ~3 MiB on a typical post. Deeper buys almost nothing: 4 already removes
/// three quarters of the round trips, and the curve flattens from there.
const PIPELINE_DEPTH: usize = 4;

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
            decoded_sizes: DecodedSizes::new(),
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

    /// Live figures for the decoded-size memo, for the health query.
    pub fn segment_sizes_stats(&self) -> riven_core::cache::CacheStats {
        self.decoded_sizes.stats()
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
    ///
    /// A provider that stops answering is treated the same way — its budget
    /// expires (see [`ClientPool::article_budget`]) and the walk moves on. That
    /// failover, not a longer wait, is what rescues a stalled article: the
    /// stall observed in practice was a socket the provider had silently
    /// stopped serving, which no amount of patience on that socket recovers.
    async fn fetch_sequential(&self, message_id: &Arc<str>) -> Result<Bytes, NntpError> {
        let mut all_not_found = true;
        let mut attempted = false;
        let mut last_err: Option<NntpError> = None;

        for provider in self.attempt_order() {
            attempted = true;
            let started = Instant::now();
            match self.fetch_from(provider, message_id).await {
                Ok(bytes) => {
                    provider.record_success(started.elapsed());
                    return Ok(bytes);
                }
                Err(NntpError::ArticleNotFound(status)) => {
                    provider.record_not_found();
                    last_err = Some(NntpError::ArticleNotFound(status));
                }
                Err(error) => {
                    let stalled = matches!(error, NntpError::Timeout);
                    provider.record_failure();
                    if stalled {
                        tracing::warn!(
                            host = provider.host(),
                            message_id = %message_id,
                            waited_ms = started.elapsed().as_millis(),
                            budget_ms = provider.article_budget().as_millis(),
                            "nntp provider stopped answering; failing over to the next"
                        );
                    } else {
                        tracing::debug!(
                            host = provider.host(),
                            message_id = %message_id,
                            %error,
                            "nntp provider failed; excluding and trying the next"
                        );
                    }
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
        let mut lease = provider.acquire().await?;
        let body = lease.body(message_id, provider.article_budget()).await?;
        drop(lease);
        self.decode_and_cache(message_id, body).await
    }

    /// yEnc-decode one fetched body off the runtime and publish it: metrics,
    /// the decoded-size memo, and the staging cache.
    async fn decode_and_cache(
        &self,
        message_id: &Arc<str>,
        body: Vec<u8>,
    ) -> Result<Bytes, NntpError> {
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

        self.metrics.record_ok(decoded.len() as u64);
        self.decoded_sizes
            .put(message_id.clone(), decoded.len() as u64);
        self.cache
            .put(message_id.clone(), decoded.clone(), decoded.len() as u64);
        Ok(decoded)
    }

    /// Fetch a batch of articles, pipelining the wire work over **one**
    /// connection instead of taking one per article. Results are positional.
    ///
    /// For batch consumers — ingest probes, backfill, the PAR2 blob walk —
    /// which know every id up front and care about their connection footprint,
    /// because the slots they hold are the ones playback is competing for. The
    /// streaming path deliberately does not use this: it wants `n` articles
    /// arriving in parallel, which is the opposite trade.
    ///
    /// `fallback_concurrency` bounds only the articles priming did not resolve.
    pub async fn fetch_batch(
        self: &Arc<Self>,
        message_ids: &[String],
        fallback_concurrency: usize,
    ) -> Vec<Result<Bytes, NntpError>> {
        self.prime_batch(message_ids).await;

        // Everything primed is a cache hit that never reaches the wire. What
        // priming missed goes through the per-article path, which is where
        // failover, single-flight and the permanent-missing rule live — so
        // pipelining stays a pure optimisation and cannot change an outcome.
        // Each future owns its id and its own handle on the pool. Borrowing
        // them across `buffer_unordered` instead leaves the returned futures
        // with anonymous lifetimes that defeat `Send` inference at the
        // `LocalByteSource` impls above this.
        let owned: Vec<(usize, String)> = message_ids.iter().cloned().enumerate().collect();
        let mut results: Vec<(usize, Result<Bytes, NntpError>)> = futures::stream::iter(owned)
            .map(|(index, id)| {
                let pool = Arc::clone(self);
                async move { (index, pool.fetch_segment(&id).await) }
            })
            .buffer_unordered(fallback_concurrency.max(1))
            .collect()
            .await;
        results.sort_by_key(|(index, _)| *index);
        results.into_iter().map(|(_, result)| result).collect()
    }

    /// Best-effort: pipeline every id not already staged over one connection
    /// from the healthiest provider and stage whatever comes back.
    ///
    /// Every failure path here is a silent `return` on purpose. This only ever
    /// saves the caller a round trip; [`fetch_batch`](Self::fetch_batch)
    /// re-checks every id afterwards, so a provider that mishandles pipelined
    /// `BODY` costs one wasted attempt and nothing else.
    ///
    /// Two things it deliberately does not do. It does not feed the provider's
    /// latency EWMA — a batch's duration is `n` articles' worth and would make
    /// [`ClientPool::article_budget`] nonsense — nor its failure counters,
    /// which the per-article retry underneath records properly. And it does not
    /// consult the single-flight table, so an id already in flight elsewhere
    /// may be fetched twice; that is wasted bytes on a background path, not a
    /// wrong answer, and checking would need a lock this path has no other
    /// reason to take.
    async fn prime_batch(self: &Arc<Self>, message_ids: &[String]) {
        // `contains`, not `get`: this is a scheduling probe over candidates, and
        // `get` both counts a lookup and promotes what it finds. Counting here
        // booked a second miss for every id that then missed again in
        // `fetch_segment`, which is why the segment cache reported a 33.9 % hit
        // rate under playback against exactly half as many wire fetches as
        // misses — the real figure was 50.7 %. Promoting is wrong for the same
        // reason: a look-ahead candidate no reader has asked for should not
        // outrank a segment one did.
        let wanted: Vec<String> = message_ids
            .iter()
            .filter(|id| !self.cache.contains(id.as_str()) && !self.missing.contains(id))
            .cloned()
            .collect();
        if wanted.is_empty() {
            return;
        }
        let Some(provider) = self.attempt_order().into_iter().next() else {
            return;
        };

        // Chunked here rather than trusted to callers, because the cost of an
        // unbounded batch is paid in memory, not time: `body_many` holds every
        // reply it has read until the batch finishes, so an `n`-article batch
        // is `n` article buffers resident at once. One caller
        // (`fetch_par2_blob`) passes a whole file's segment list, which made
        // that unbounded. Peak is now PIPELINE_DEPTH buffers whatever the
        // caller asks for, and the round-trip saving is barely touched — a
        // chunk of 4 still eliminates three quarters of the round trips.
        for chunk in wanted.chunks(PIPELINE_DEPTH) {
            if !self.prime_chunk(provider, chunk).await {
                return;
            }
        }
    }

    /// One pipelined chunk. `false` means stop — the connection failed, and the
    /// remaining chunks would only repeat the failure.
    async fn prime_chunk(self: &Arc<Self>, provider: &Arc<ClientPool>, chunk: &[String]) -> bool {
        // One article's budget per article in the chunk: the replies arrive
        // back to back, so the chunk is allowed the sum of what its members
        // would each have been allowed alone.
        let budget = provider
            .article_budget()
            .saturating_mul(u32::try_from(chunk.len()).unwrap_or(u32::MAX));

        let Ok(mut lease) = provider.acquire().await else {
            return false;
        };
        let bodies = match lease.body_many(chunk, budget).await {
            Ok(bodies) => bodies,
            Err(error) => {
                tracing::debug!(
                    host = provider.host(),
                    batch = chunk.len(),
                    %error,
                    "pipelined body batch failed; falling back to per-article fetches"
                );
                return false;
            }
        };
        drop(lease);

        for (id, body) in chunk.iter().zip(bodies) {
            // A `430` here says only that *this* provider lacks the article.
            // Whether every provider agrees — the thing that makes an id
            // permanently missing — is not this path's call to make.
            let Ok(body) = body else { continue };
            drop(self.decode_and_cache(&Arc::from(id.as_str()), body).await);
        }
        true
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
    inner: ByteLru<Arc<str>, u64>,
}

impl DecodedSizes {
    fn new() -> Self {
        Self {
            inner: ByteLru::with_budget(SEGMENT_SIZES),
        }
    }

    /// What one entry costs, so the byte budget means something.
    ///
    /// The `lru` crate boxes a node holding key, value and two links, the table
    /// keeps a slot, and the message-id's own bytes sit behind the `Arc`. None
    /// of that is measurable at runtime without walking the allocator, so it is
    /// counted rather than measured — the point is that the budget tracks the
    /// real cost within a small factor, not that it is exact.
    fn weight(message_id: &str) -> u64 {
        /// `Arc` header + `Arc<str>` fat pointer + `u64` + two LRU links +
        /// the hashmap slot, rounded up for allocator overhead.
        const PER_ENTRY_OVERHEAD: u64 = 96;
        PER_ENTRY_OVERHEAD + message_id.len() as u64
    }

    fn get(&self, message_id: &str) -> Option<u64> {
        // `touch`, not `get`: this memo is consulted opportunistically while
        // walking a RAR volume, and its hit rate is not the segment cache's.
        self.inner.touch(message_id)
    }

    fn put(&self, message_id: Arc<str>, size: u64) {
        let weight = Self::weight(&message_id);
        self.inner.put(message_id, size, weight);
    }

    fn stats(&self) -> riven_core::cache::CacheStats {
        self.inner.stats()
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
                article_timeout: Duration::from_millis(200),
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

    /// The stall this crate's article budget exists for: a provider that
    /// accepts `BODY`, answers `222`, and then never sends the body. No
    /// timeout on the socket recovers it — only giving up and asking someone
    /// else does. Before the budget, this fetch blocked for up to
    /// `300 s x 3 attempts` and took the playback read with it.
    ///
    /// The test providers configure a 200 ms `article_timeout`, so the budget
    /// elapses in real time without the test waiting out the 15 s default.
    #[tokio::test]
    async fn a_provider_that_stops_answering_fails_over_to_the_next() {
        let (silent_addr, _silent) = spawn_silent_body_server().await;
        let (ok_addr, _ok) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(silent_addr, 2, 0), provider(ok_addr, 2, 1)]);

        let bytes = pool.fetch_segment("a@test").await.unwrap();
        assert_eq!(bytes.as_ref(), FAKE_SEGMENT_PAYLOAD);
        assert_eq!(
            pool.providers()[0].consecutive_failure(),
            1,
            "the stall must count against the provider, not the article"
        );
        assert!(
            !pool.missing().contains("a@test"),
            "a stall is not evidence the article is gone"
        );
    }

    /// The point of [`SegmentPool::fetch_batch`]: a batch consumer holds one
    /// connection, not one per article. Those slots are what the streaming path
    /// is competing for, so this is the property worth pinning — not the
    /// round-trip count, which is the same saving expressed less usefully.
    #[tokio::test]
    async fn a_batch_fetch_pipelines_instead_of_taking_a_connection_per_article() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 8, 0)]);
        let ids: Vec<String> = (0..8).map(|i| format!("seg-{i}@test")).collect();

        let fetched = pool.fetch_batch(&ids, 4).await;
        assert_eq!(fetched.len(), 8);
        for result in &fetched {
            assert_eq!(result.as_ref().unwrap().as_ref(), FAKE_SEGMENT_PAYLOAD);
        }
        assert_eq!(
            pool.health()[0].open_connections,
            1,
            "an 8-article batch must not open a connection per article"
        );
    }

    /// The batch path probes the cache to decide what to fetch. That probe must
    /// not be counted, because the same ids are looked up again for real on the
    /// way through `fetch_segment`: counting both booked two misses per article
    /// and halved the reported hit rate. Under playback that showed as a 33.9 %
    /// segment hit rate against exactly half as many wire fetches as misses.
    ///
    /// Pinned on the number of *accounted lookups* rather than on how they are
    /// classified: what the probe must not do is add a lookup at all, and that
    /// stays true however the batch path later decides hit or miss.
    #[tokio::test]
    async fn a_batch_probe_does_not_count_as_a_cache_lookup() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 8, 0)]);
        let ids: Vec<String> = (0..4).map(|i| format!("seg-{i}@test")).collect();

        let fetched = pool.fetch_batch(&ids, 4).await;
        assert_eq!(fetched.len(), 4);

        let stats = pool.cache().stats();
        assert_eq!(
            stats.hits + stats.misses,
            4,
            "one accounted lookup per article; the probe must not add its own: {stats:?}"
        );

        // A second pass over the same ids is all cache, and must add exactly one
        // accounted lookup per article again.
        let before = pool.cache().stats();
        for id in &ids {
            assert!(pool.fetch_segment(id).await.is_ok());
        }
        let after = pool.cache().stats();
        assert_eq!(after.misses, before.misses, "a cached id must not miss");
        assert_eq!(after.hits, before.hits + 4, "one hit per fetch");
    }

    /// A `430` mid-pipeline is a status line with no body behind it. Reading it
    /// as though it had one would consume the *next* article's body as this
    /// one's and hand back garbage for every article after it — so this asserts
    /// the survivors decode, not merely that the call returned.
    #[tokio::test]
    async fn a_missing_article_mid_batch_leaves_the_rest_of_the_batch_readable() {
        let (addr, _server) = spawn_selective_body_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 4, 0)]);
        let ids: Vec<String> = vec![
            "a@test".into(),
            "dead@test".into(),
            "c@test".into(),
            "d@test".into(),
        ];

        let fetched = pool.fetch_batch(&ids, 2).await;
        assert_eq!(fetched.len(), 4);
        assert!(
            fetched[1].is_err(),
            "the missing article must report missing"
        );
        for index in [0, 2, 3] {
            assert_eq!(
                fetched[index].as_ref().unwrap().as_ref(),
                FAKE_SEGMENT_PAYLOAD,
                "article {index} decoded wrong; the pipeline lost sync on the 430"
            );
        }
    }

    /// A pipelined batch holds every reply it has read until the batch ends, so
    /// batch size is a memory multiplier. `fetch_par2_blob` passes a whole
    /// file's segment list, which made that unbounded — the depth cap has to
    /// live here rather than in each caller.
    #[tokio::test]
    async fn a_large_batch_is_chunked_rather_than_pipelined_whole() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 8, 0)]);
        let ids: Vec<String> = (0..64).map(|i| format!("big-{i}@test")).collect();

        let fetched = pool.fetch_batch(&ids, 2).await;
        assert_eq!(fetched.len(), 64);
        for result in &fetched {
            assert_eq!(result.as_ref().unwrap().as_ref(), FAKE_SEGMENT_PAYLOAD);
        }
        // A 64-article batch must not put 64 article buffers in memory at once.
        const { assert!(PIPELINE_DEPTH <= 8) };
        assert_eq!(
            pool.health()[0].open_connections,
            1,
            "chunking must reuse the one connection, not open one per chunk"
        );
    }

    /// Results are positional, and the fallback path resolves them out of
    /// order. An index mix-up here would give one article's bytes another's
    /// message-id — silent corruption of whatever the caller keys on them.
    #[tokio::test]
    async fn batch_results_stay_in_the_order_they_were_asked_for() {
        let (addr, _server) = spawn_selective_body_server().await;
        let pool = SegmentPool::new(vec![provider(addr, 4, 0)]);
        let ids: Vec<String> = vec!["dead@test".into(), "b@test".into(), "dead2@test".into()];

        let fetched = pool.fetch_batch(&ids, 3).await;
        assert!(fetched[0].is_err());
        assert!(fetched[1].is_ok());
        assert!(fetched[2].is_err());
    }

    /// Loopback listener that serves a yEnc article to `BODY`, except for
    /// message-ids containing `dead`, which get a `430` and no body.
    async fn spawn_selective_body_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let article = crate::yenc::tests::encode_single(FAKE_SEGMENT_PAYLOAD, "fake.bin");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let article = article.clone();
                tokio::spawn(async move {
                    let (read_half, mut write_half) = socket.into_split();
                    if write_half.write_all(b"200 fake\r\n").await.is_err() {
                        return;
                    }
                    let mut lines = BufReader::new(read_half).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let dead = line.contains("dead");
                        let reply: Vec<u8> = if line.starts_with("QUIT") {
                            return;
                        } else if line.starts_with("BODY") || line.starts_with("STAT") {
                            if dead {
                                b"430 no such article\r\n".to_vec()
                            } else if line.starts_with("STAT") {
                                b"223 0 ok\r\n".to_vec()
                            } else {
                                let mut out = b"222 0 <exists>\r\n".to_vec();
                                out.extend_from_slice(&article);
                                out.extend_from_slice(b"\r\n.\r\n");
                                out
                            }
                        } else {
                            b"111 20260101000000\r\n".to_vec()
                        };
                        if write_half.write_all(&reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, handle)
    }

    /// Loopback listener that greets, accepts `BODY`, replies `222`, and then
    /// sends nothing further — a socket the provider has stopped serving.
    async fn spawn_silent_body_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
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
                        if line.starts_with("BODY") {
                            drop(write_half.write_all(b"222 0 <a@test> body\r\n").await);
                            std::future::pending::<()>().await;
                        }
                        let reply: &[u8] = if line.starts_with("QUIT") {
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
