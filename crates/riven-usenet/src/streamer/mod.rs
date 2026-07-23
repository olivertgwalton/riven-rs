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

use sea_orm::DatabaseConnection;

use crate::nntp::{NntpConfig, NntpPool};
use crate::state::StreamerState;

mod availability;
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
pub use meta::{NzbMeta, NzbMetaFile, NzbMetaSource, NzbRarPart, NzbRarSlice, UNKNOWN_FILE_LABEL};

pub(crate) use availability::{SweepCounts, stat_sweep};
pub(crate) use meta::{concat_slices, segments_overlapping, select_validation_indices};

#[derive(Clone)]
pub struct UsenetStreamer {
    pub(crate) pool: Arc<NntpPool>,
    pub(crate) state: Arc<StreamerState>,
    pub(crate) db: DatabaseConnection,
}

impl UsenetStreamer {
    pub fn new(cfg: NntpConfig, db: DatabaseConnection) -> Self {
        crate::nntp::init_crypto();
        let pool = NntpPool::new_multi(cfg.providers);
        Self {
            pool,
            state: StreamerState::global(),
            db,
        }
    }

    /// Process-wide shared streamer keyed by NNTP config. Both ingest and
    /// playback construct through this so they share one `NntpPool` and the
    /// user's `max_connections` is the true ceiling against the provider.
    /// Settings change → fingerprint flips → cached entry rebuilt
    /// automatically, no restart needed.
    pub fn shared(cfg: NntpConfig, db: DatabaseConnection) -> Self {
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

    /// Filename for a log field, from the in-memory meta cache only. Never
    /// touches Postgres and never waits on an in-flight load: naming a file in
    /// a log line must not add a database round-trip (or a single-flight wait)
    /// to a path that is otherwise cache-only — notably the read-ahead shed
    /// path, which runs *before* the meta load precisely to stay cheap.
    /// Returns the placeholder when the release isn't resident.
    pub fn cached_file_label(&self, info_hash: &str, file_index: usize) -> String {
        self.state.meta_cache.get(info_hash).map_or_else(
            || meta::UNKNOWN_FILE_LABEL.to_string(),
            |meta| meta.file_label(file_index).to_string(),
        )
    }

    /// Keep one operation's fan-out small even when a provider permits many
    /// connections. This mirrors AltMount's explicit per-reader prefetch
    /// window instead of equating provider capacity with work concurrency.
    pub(crate) fn prefetch_concurrency(&self, client_capacity: usize) -> usize {
        client_capacity.min(self.state.max_prefetch()).max(1)
    }

    /// Load NZB meta for `info_hash`. Order: in-memory LRU → Postgres.
    /// Postgres is the source of truth; the LRU just absorbs the hot path
    /// during playback. There's no Redis layer and no TTL: as long as the
    /// `usenet_meta` row exists, the file remains streamable.
    ///
    /// Single-flighted per info_hash. Every episode of a season pack shares
    /// one `usenet_meta` row, so a scanner opening a whole season at once
    /// produced N simultaneous cold loads of the *same* document — N database
    /// round-trips, N parses and N peak-sized deserialize allocations for a
    /// single cache fill. With an 80 MB record and two dozen episodes that
    /// alone is a multi-gigabyte spike (which musl then never returns to the
    /// OS). Now the first caller loads and the rest wait on its promise.
    pub async fn load_meta(&self, info_hash: &str) -> Result<Arc<NzbMeta>, StreamerError> {
        // A waiter whose owner failed retries as owner itself; bounded so a
        // permanently-failing load can't bounce callers around the loop.
        const MAX_LOAD_ATTEMPTS: usize = 4;
        for _ in 0..MAX_LOAD_ATTEMPTS {
            if let Some(hit) = self.state.meta_cache.get(info_hash) {
                self.maybe_kick_backfill(&hit);
                return Ok(hit);
            }
            match self.state.meta_loads.enter_or_wait(info_hash) {
                crate::state::FetchEntry::Wait(slot) => {
                    slot.wait().await;
                }
                crate::state::FetchEntry::Owner(slot, key) => {
                    // Release waiters even if this future is cancelled
                    // mid-load (a FUSE handle closing, say) — otherwise every
                    // other opener of this release hangs forever.
                    let guard = MetaLoadGuard {
                        state: self.state.clone(),
                        slot,
                        key,
                    };
                    let result = self.load_meta_cold(info_hash).await;
                    drop(guard);
                    return result;
                }
            }
        }
        // Owner churn (every attempt lost the race and then found nothing
        // cached). Fall back to loading directly rather than failing a read.
        self.load_meta_cold(info_hash).await
    }

    /// Cold path of [`load_meta`]: Postgres load, offset auto-heal, cache fill.
    /// Only ever entered by one caller at a time per info_hash.
    async fn load_meta_cold(&self, info_hash: &str) -> Result<Arc<NzbMeta>, StreamerError> {
        let mut meta = store::load(&self.db, info_hash)
            .await?
            .ok_or_else(|| StreamerError::NotIngested(info_hash.to_string()))?;

        // Healing one file costs up to two full article downloads, so a
        // release with hundreds of files (a season pack ships one meta for the
        // whole season) would otherwise download hundreds of megabytes inline
        // on a single cache miss — with every other opener of that release
        // blocked behind it on the single-flight promise. Heal a few files per
        // load instead: the result is persisted, so successive loads converge,
        // and an unhealed file still serves correctly from its stored offsets
        // (`read_direct` anchors on the offset table but sizes every slice
        // from the actual decoded length).
        const MAX_AUTOHEAL_FILES_PER_LOAD: usize = 4;
        let mut healed_indices: Vec<usize> = Vec::new();
        let mut healed_names: Vec<String> = Vec::new();
        let mut pending_names: Vec<String> = Vec::new();
        for (file_index, file) in meta.files.iter_mut().enumerate() {
            let approximate = matches!(
                &file.source,
                NzbMetaSource::Direct { offsets, .. } if direct_offsets_look_approximate(offsets)
            );
            if !approximate {
                continue;
            }
            let filename = file.filename.clone();
            if healed_indices.len() >= MAX_AUTOHEAL_FILES_PER_LOAD {
                pending_names.push(filename);
                continue;
            }
            match self.rescale_direct_to_decoded(file).await {
                Ok(()) => {
                    healed_indices.push(file_index);
                    healed_names.push(filename);
                }
                Err(error) => tracing::warn!(
                    info_hash,
                    file = %filename,
                    %error,
                    "usenet meta auto-heal: rescale failed; serving stored offsets"
                ),
            }
        }
        if !pending_names.is_empty() {
            tracing::info!(
                info_hash,
                pending_heals = pending_names.len(),
                files = %summarize_filenames(&pending_names),
                "usenet meta auto-heal: deferred remaining files to a later load"
            );
        }
        if !healed_indices.is_empty() {
            match store::store(&self.db, info_hash, &meta).await {
                Ok(()) => tracing::info!(
                    info_hash,
                    healed = healed_indices.len(),
                    files = %summarize_filenames(&healed_names),
                    "usenet meta auto-heal: rescaled Direct offsets to exact decoded space"
                ),
                Err(error) => tracing::warn!(
                    info_hash,
                    files = %summarize_filenames(&healed_names),
                    %error,
                    "usenet meta auto-heal: persist failed; healed in memory only"
                ),
            }
            // The heal above can shrink `total_size` (an encoded-byte estimate
            // corrected to the exact decoded length). `filesystem_entries.file_size`
            // was set once at grab time from the old estimate and won't pick up
            // this correction on its own, so every library entry serving this
            // file would keep advertising a size the source can't actually back —
            // any tail read past the real end then fails with EIO. Sync it now.
            for file_index in &healed_indices {
                let file_size = meta.files[*file_index].total_size;
                let filename = meta.files[*file_index].filename.as_str();
                match store::sync_file_size(&self.db, info_hash, *file_index, file_size).await {
                    Ok(rows) if rows > 0 => tracing::info!(
                        info_hash,
                        file_index,
                        file = %filename,
                        file_size,
                        rows,
                        "usenet meta auto-heal: synced filesystem_entries.file_size"
                    ),
                    Ok(_) => {}
                    Err(error) => tracing::warn!(
                        info_hash,
                        file_index,
                        file = %filename,
                        %error,
                        "usenet meta auto-heal: failed to sync filesystem_entries.file_size"
                    ),
                }
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
    /// degraded a title is. Uses a workload-bound bulk client so it yields to
    /// live playback without assigning priority per article.
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

        let sample: Vec<String> = select_validation_indices(total, sample_percent)
            .into_iter()
            .map(|i| message_ids[i].clone())
            .collect();
        let n = sample.len();
        if n == 0 {
            return Ok(AvailabilityScan::default());
        }

        let concurrency = self
            .prefetch_concurrency(self.pool.bulk_client().capacity())
            .min(n);
        let client = self.pool.bulk_client();
        let counts = stat_sweep(&client, sample, concurrency, false, &file.filename).await;

        Ok(AvailabilityScan {
            total_segments: total,
            sampled_segments: counts.checked,
            missing_segments: counts.missing,
            error_segments: counts.errors,
        })
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
        message_ids.sort_unstable();
        message_ids.dedup();
        let total = message_ids.len();
        if total == 0 {
            return Ok(());
        }

        let concurrency = self
            .prefetch_concurrency(self.pool.bulk_client().capacity())
            .min(total);
        let SweepCounts {
            missing,
            errors,
            checked,
        } = stat_sweep(
            &self.pool.bulk_client(),
            message_ids,
            concurrency,
            false,
            meta.label(),
        )
        .await;

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

/// Releases the `meta_loads` single-flight slot on scope exit, including when
/// the owning future is cancelled. Without the `Drop`, a cancelled load would
/// leave the slot present and never-completed, hanging every other caller that
/// asked for the same release.
struct MetaLoadGuard {
    state: Arc<StreamerState>,
    slot: Arc<crate::state::PromiseSlot>,
    key: Arc<str>,
}

impl Drop for MetaLoadGuard {
    fn drop(&mut self) {
        self.state.meta_loads.finish(&self.key, &self.slot);
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

/// Render a file list for a log line: a season pack can defer dozens of files,
/// so name the first few and count the rest rather than emitting a multi-KB
/// log field.
fn summarize_filenames(names: &[String]) -> String {
    const MAX_NAMED: usize = 5;
    if names.len() <= MAX_NAMED {
        return names.join(", ");
    }
    format!(
        "{}, +{} more",
        names[..MAX_NAMED].join(", "),
        names.len() - MAX_NAMED
    )
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
        Ok(UsenetStreamer::read_range(self, info_hash, file_index, start, end_inclusive).await?)
    }

    async fn prefetch(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        segments_ahead: usize,
    ) {
        self.prefetch_window(info_hash, file_index, start, segments_ahead)
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
        self.pool.stream_started();
    }

    fn stream_touch(&self, key: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        active_streams().touch(key, now);
    }

    fn stream_unregister(&self, key: &str) {
        active_streams().unregister(key);
        self.pool.stream_ended();
    }
}
