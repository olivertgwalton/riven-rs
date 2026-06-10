//! High-level streaming engine.
//!
//! Holds the NNTP pool and exposes two operations:
//!   - `ingest(info_hash, nzb_xml)` — parse an NZB, build the right virtual-file
//!     metadata (direct or RAR-contained), and persist segment maps to Redis.
//!   - `read_range(info_hash, file_index, start, end)` — fetch the NNTP
//!     articles covering `[start, end]`, decode yEnc, and return the bytes.
//!
//! Segment offsets are *approximate* for direct sources — we use the encoded
//! `bytes` from the NZB as a stand-in for decoded size (right to within ~2%).
//! Good enough for sequential playback; HLS/DASH and progressive MP4/MKV
//! players tolerate the few-byte boundary slop. RAR-contained sources use
//! exact decoded-byte addressing.

use std::sync::{Arc, Mutex, OnceLock};

use futures::StreamExt;
use futures::stream;
use sqlx::PgPool;

use crate::nntp::{NntpConfig, NntpPool, Priority};
use crate::state::StreamerState;

mod backfill;
mod error;
mod ingest;
mod meta;
mod read_direct;
mod read_rar;
mod store;
#[cfg(test)]
mod tests;

pub use error::StreamerError;
pub use ingest::DEFAULT_AVAILABILITY_SAMPLE_PERCENT;
pub use meta::{NzbMeta, NzbMetaFile, NzbMetaSource, NzbRarPart, NzbRarSlice};

pub(crate) use meta::{concat_slices, segments_overlapping, select_validation_indices};

/// Floor on the prefetch fan-out during ingest (background work).
pub(crate) const PREFETCH_FLOOR: usize = 4;

#[derive(Clone)]
pub struct UsenetStreamer {
    pub(crate) pool: Arc<NntpPool>,
    pub(crate) state: Arc<StreamerState>,
    /// Bounds concurrent NZB ingests. Sized from the NNTP connection budget
    /// (`pool.total_capacity()`) so large accounts drain a scrape backlog fast
    /// while small ones stay conservative — see `ingest_concurrency_for`.
    pub(crate) ingest_sem: Arc<tokio::sync::Semaphore>,
    pub(crate) db: PgPool,
}

impl UsenetStreamer {
    pub fn new(cfg: NntpConfig, db: PgPool) -> Self {
        crate::nntp::init_crypto();
        let pool = NntpPool::new_multi(cfg.providers);
        let ingest_sem = Arc::new(tokio::sync::Semaphore::new(
            crate::state::ingest_concurrency_for(pool.total_capacity()),
        ));
        // Fire-and-forget: open a handful of authenticated NNTP sockets per
        // provider so the first stream request finds hot connections in the
        // pool instead of paying TCP + TLS + AUTHINFO latency.
        {
            let pool = pool.clone();
            tokio::spawn(async move {
                pool.prewarm().await;
            });
        }
        Self {
            pool,
            state: StreamerState::global(),
            ingest_sem,
            db,
        }
    }

    /// Process-wide shared streamer keyed by NNTP config. Both ingest and
    /// playback construct through this so they share one `NntpPool` and the
    /// user's `max_connections` is the true ceiling against the provider.
    /// Settings change → fingerprint flips → cached entry rebuilt
    /// automatically, no restart needed.
    pub fn shared(cfg: NntpConfig, db: PgPool) -> Self {
        let fp = nntp_config_fingerprint(&cfg);
        let mut guard = shared_cell()
            .lock()
            .expect("UsenetStreamer::shared mutex poisoned");
        if let Some((stored_fp, streamer)) = guard.as_ref()
            && *stored_fp == fp
        {
            return streamer.clone();
        }
        let streamer = Self::new(cfg, db);
        *guard = Some((fp, streamer.clone()));
        streamer
    }

    /// The already-constructed shared streamer, if one exists — without
    /// creating (and prewarming) a new pool as a side effect. Read-only
    /// callers like the API's provider-health view use this so a health query
    /// never spins up NNTP connections on its own.
    pub fn existing_shared() -> Option<Self> {
        shared_cell()
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|(_, streamer)| streamer.clone()))
    }

    pub fn pool(&self) -> Arc<NntpPool> {
        self.pool.clone()
    }

    /// Load NZB meta for `info_hash`. Order: in-memory LRU → Postgres.
    /// Postgres is the source of truth; the LRU just absorbs the hot path
    /// during playback. There's no Redis layer and no TTL: as long as the
    /// `usenet_meta` row exists, the file remains streamable.
    pub async fn load_meta(&self, info_hash: &str) -> Result<Arc<NzbMeta>, StreamerError> {
        if let Some(hit) = self.state.meta_cache.get(info_hash) {
            self.maybe_kick_backfill(&hit);
            return Ok(hit);
        }
        let mut meta = store::load(&self.db, info_hash)
            .await?
            .ok_or_else(|| StreamerError::NotIngested(info_hash.to_string()))?;

        // Auto-heal Direct metas whose offset table predates exact-offset
        // rescaling. Those tables hold encoded-byte *estimates* (each segment's
        // slot drifts from its true decoded length), which misaligns the
        // stateless random-access read path and corrupts playback. Detection is
        // a pure check on the stored array — no I/O — so already-exact metas
        // pass straight through. When a table looks approximate we re-run the
        // same rescale a fresh ingest would (cheap: it fetches only the first +
        // last segment) and persist the result, so it's a one-time cost per
        // title that survives restarts and cache eviction.
        let mut healed = false;
        for file in &mut meta.files {
            let approximate = matches!(
                &file.source,
                NzbMetaSource::Direct { offsets, .. } if direct_offsets_look_approximate(offsets)
            );
            if approximate {
                match self.rescale_direct_to_decoded(file).await {
                    Ok(()) => healed = true,
                    Err(error) => tracing::warn!(
                        info_hash,
                        %error,
                        "usenet meta auto-heal: rescale failed; serving stored offsets"
                    ),
                }
            }
        }
        if healed {
            match store::store(&self.db, info_hash, &meta).await {
                Ok(()) => tracing::info!(
                    info_hash,
                    "usenet meta auto-heal: rescaled Direct offsets to exact decoded space"
                ),
                Err(error) => tracing::warn!(
                    info_hash,
                    %error,
                    "usenet meta auto-heal: persist failed; healed in memory only"
                ),
            }
        }

        let arc = Arc::new(meta);
        self.state
            .meta_cache
            .put(info_hash.to_string(), arc.clone());
        self.maybe_kick_backfill(&arc);
        Ok(arc)
    }

    /// STAT-sample a file's segments across providers to gauge availability,
    /// returning counts (not a verdict). Mirrors the ingest probe's sampling
    /// but reports missing/error counts so the health view can show *how*
    /// degraded a title is. Uses `Priority::Low` so it never competes with
    /// live playback.
    pub async fn scan_availability(
        &self,
        info_hash: &str,
        file_index: usize,
        sample_percent: usize,
    ) -> Result<AvailabilityScan, StreamerError> {
        let meta = self.load_meta(info_hash).await?;
        let file = meta
            .files
            .get(file_index)
            .ok_or(StreamerError::BadFileIndex(file_index))?;

        let message_ids: Vec<String> = match &file.source {
            NzbMetaSource::Direct { segments, .. } => {
                segments.iter().map(|s| s.message_id.clone()).collect()
            }
            NzbMetaSource::Rar { parts, .. } => parts
                .iter()
                .flat_map(|p| p.segments.iter())
                .map(|s| s.message_id.clone())
                .collect(),
        };

        let total = message_ids.len();
        if total == 0 {
            return Ok(AvailabilityScan::default());
        }

        // Strategic sample (first-N / last-N / spread middle), or every segment
        // when `sample_percent >= 100`. Full coverage is the only mode that
        // catches a single dead article — set it for the background scanner via
        // the "check all segments" toggle.
        let sample: Vec<String> = select_validation_indices(total, sample_percent)
            .into_iter()
            .map(|i| message_ids[i].clone())
            .collect();
        let n = sample.len();
        if n == 0 {
            return Ok(AvailabilityScan::default());
        }

        let concurrency = self.pool.ingest_concurrency().max(PREFETCH_FLOOR).min(n);
        let pool = self.pool.clone();
        let mut probes = stream::iter(sample)
            .map(move |mid| {
                let pool = pool.clone();
                async move { pool.stat(&mid, Priority::Low).await }
            })
            .buffer_unordered(concurrency);

        let mut scan = AvailabilityScan {
            total_segments: total,
            ..AvailabilityScan::default()
        };
        while let Some(result) = probes.next().await {
            scan.sampled_segments += 1;
            match result {
                Ok(true) => {}
                Ok(false) => scan.missing_segments += 1,
                Err(_) => scan.error_segments += 1,
            }
        }
        Ok(scan)
    }

    /// Full STAT verification of **every** segment across **all** files in a
    /// release — the only check that reliably catches a single dead article,
    /// where sampling almost always misses it. Meant to run once on the
    /// *selected* candidate at download time (not per candidate walked), gated
    /// by the "check all segments" setting.
    ///
    /// Returns `Err(IncompleteRelease)` when the confirmed-missing fraction
    /// exceeds `acceptable_missing_pct` (0.0 = altmount's zero-tolerance
    /// default; the read path has no par2 repair, so any gap in the played
    /// range stalls playback), or when the provider was unreachable for over
    /// half the sweep (can't confirm completeness — don't pass it through).
    pub async fn verify_release_complete(
        &self,
        info_hash: &str,
        acceptable_missing_pct: f64,
    ) -> Result<(), StreamerError> {
        let meta = self.load_meta(info_hash).await?;
        let mut message_ids: Vec<String> = Vec::new();
        for file in &meta.files {
            match &file.source {
                NzbMetaSource::Direct { segments, .. } => {
                    message_ids.extend(segments.iter().map(|s| s.message_id.clone()));
                }
                NzbMetaSource::Rar { parts, .. } => {
                    for p in parts {
                        message_ids.extend(p.segments.iter().map(|s| s.message_id.clone()));
                    }
                }
            }
        }
        // RAR parts can be shared across contained files — de-dup so a segment
        // is STAT'd once.
        message_ids.sort_unstable();
        message_ids.dedup();
        let total = message_ids.len();
        if total == 0 {
            return Ok(());
        }

        let concurrency = self
            .pool
            .ingest_concurrency()
            .max(PREFETCH_FLOOR)
            .min(total);
        let pool = self.pool.clone();
        let mut probes = stream::iter(message_ids)
            .map(move |mid| {
                let pool = pool.clone();
                async move { pool.stat(&mid, Priority::Low).await }
            })
            .buffer_unordered(concurrency);

        let mut missing = 0usize;
        let mut errors = 0usize;
        let mut checked = 0usize;
        while let Some(result) = probes.next().await {
            checked += 1;
            match result {
                Ok(true) => {}
                Ok(false) => missing += 1,
                Err(_) => errors += 1,
            }
        }

        let checked_f = checked.max(1) as f64;
        if (missing as f64 / checked_f) * 100.0 > acceptable_missing_pct {
            return Err(StreamerError::IncompleteRelease { missing, checked });
        }
        if errors as f64 / checked_f > 0.5 {
            return Err(StreamerError::IncompleteRelease {
                missing: errors,
                checked,
            });
        }
        Ok(())
    }
}

/// Result of [`UsenetStreamer::scan_availability`] — raw counts over a sampled
/// subset of a file's segments.
#[derive(Debug, Clone, Default)]
pub struct AvailabilityScan {
    pub total_segments: usize,
    pub sampled_segments: usize,
    pub missing_segments: usize,
    pub error_segments: usize,
}

impl AvailabilityScan {
    /// Health verdict, shared by the background scanner and the on-demand
    /// re-check so they classify identically:
    /// - `unhealthy` — at least one sampled segment was confirmed missing.
    /// - `unknown` — nothing to check, or over half the probes errored (the
    ///   provider was unreachable, so availability couldn't be confirmed).
    /// - `healthy` — every sampled segment was confirmed present.
    pub fn status(&self) -> &'static str {
        if self.total_segments == 0 {
            "unknown"
        } else if self.missing_segments > 0 {
            "unhealthy"
        } else if self.sampled_segments > 0
            && (self.error_segments as f64 / self.sampled_segments as f64) > 0.5
        {
            "unknown"
        } else {
            "healthy"
        }
    }
}

/// Heuristic (no I/O): does a `Direct` offset table look like a pre-exact-offset
/// estimate rather than the uniform-part decoded map the current ingest emits?
///
/// The exact-offset rescale makes every *full* segment the same decoded size
/// (`dec_first`), so all interior steps are identical and only the final
/// (partial) segment differs. The old encoded-byte estimate varied each
/// segment's slot, so its interior steps differ from the first one onward.
/// Comparing the first few full-part steps catches the estimate immediately
/// (S5E3, for instance, drifts at the very second segment) while leaving a
/// genuinely uniform table untouched.
fn direct_offsets_look_approximate(offsets: &[u64]) -> bool {
    // `offsets` has `n_segments + 1` entries. The final step
    // (`offsets[len-2]..offsets[len-1]`) is the partial last segment and is
    // excluded; full-part steps are indices `0..len-2`. Need at least two of
    // them to compare.
    let full_steps = offsets.len().saturating_sub(2);
    if full_steps < 2 {
        return false;
    }
    let step0 = offsets[1] - offsets[0];
    for i in 1..full_steps.min(4) {
        if offsets[i + 1] - offsets[i] != step0 {
            return true;
        }
    }
    false
}

/// Public accessor: registry of currently-streaming items. The VFS
/// `UsenetSession` registers a stream on first read and removes it when the
/// file handle is dropped (via the `LocalByteSource` stream hooks).
pub fn active_streams() -> Arc<crate::state::ActiveStreams> {
    crate::state::global_active_streams()
}

/// Process-wide cache of the shared streamer keyed by NNTP config fingerprint.
/// Lifted to module scope so [`UsenetStreamer::existing_shared`] can peek it
/// without the create-on-miss side effect of [`UsenetStreamer::shared`].
fn shared_cell() -> &'static Mutex<Option<(u64, UsenetStreamer)>> {
    static CELL: OnceLock<Mutex<Option<(u64, UsenetStreamer)>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

/// Point-in-time health of the in-process usenet streaming engine (segment
/// cache, NNTP fetch counters, in-flight work). Counters are cumulative since
/// process start; the API derives rates by sampling deltas.
#[derive(Debug, Clone)]
pub struct StreamingHealth {
    pub cache_bytes_used: u64,
    pub cache_bytes_max: u64,
    pub cache_entries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub fetches_ok: u64,
    pub fetches_failed: u64,
    pub bytes_decoded: u64,
    pub in_flight: u64,
    pub dead_segments: u64,
    pub active_streams: u64,
}

/// Snapshot the process-global streaming state for the API's health view.
pub fn streaming_health() -> StreamingHealth {
    let state = crate::state::StreamerState::global();
    StreamingHealth {
        cache_bytes_used: state.cache.current_bytes(),
        cache_bytes_max: state.cache.max_bytes(),
        cache_entries: state.cache.entry_count() as u64,
        cache_hits: state.cache.hits(),
        cache_misses: state.cache.misses(),
        fetches_ok: state.fetch_metrics.ok(),
        fetches_failed: state.fetch_metrics.failed(),
        bytes_decoded: state.fetch_metrics.bytes_decoded(),
        in_flight: state.in_flight.len() as u64,
        dead_segments: state.fails.len() as u64,
        active_streams: active_streams().count() as u64,
    }
}

fn nntp_config_fingerprint(cfg: &NntpConfig) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    let mut entries: Vec<&crate::nntp::NntpProvider> = cfg.providers.iter().collect();
    entries.sort_by(|a, b| {
        (a.config.host.as_str(), a.config.port).cmp(&(b.config.host.as_str(), b.config.port))
    });
    for p in entries {
        p.config.host.hash(&mut h);
        p.config.port.hash(&mut h);
        p.config.user.hash(&mut h);
        p.config.pass.hash(&mut h);
        p.config.use_tls.hash(&mut h);
        p.config.max_connections.hash(&mut h);
        p.priority.hash(&mut h);
        p.is_backup.hash(&mut h);
    }
    h.finish()
}

#[async_trait::async_trait]
impl riven_core::local_source::LocalByteSource for UsenetStreamer {
    async fn read_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> anyhow::Result<bytes::Bytes> {
        // Inherent `read_range` loads meta + dispatches Direct/RAR.
        Ok(UsenetStreamer::read_range(self, info_hash, file_index, start, end_inclusive).await?)
    }

    async fn prefetch(&self, info_hash: &str, file_index: usize, start: u64, end_inclusive: u64) {
        self.prefetch_range(info_hash, file_index, start, end_inclusive)
            .await;
    }

    fn stream_register(&self, key: &str, info_hash: &str, filename: &str, file_size: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        active_streams().register(
            key.to_string(),
            crate::state::ActiveStream {
                info_hash: info_hash.to_string(),
                filename: filename.to_string(),
                file_size,
                started_at: now,
                last_active: now,
                client: "vfs".to_string(),
            },
        );
    }

    fn stream_touch(&self, key: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        active_streams().touch(key, now);
    }

    fn stream_unregister(&self, key: &str) {
        active_streams().unregister(key);
    }
}
