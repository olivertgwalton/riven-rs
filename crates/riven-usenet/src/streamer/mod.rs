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

use sqlx::PgPool;

use crate::nntp::{NntpConfig, NntpPool};
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

pub(crate) use meta::segments_overlapping;

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
        static CELL: OnceLock<Mutex<Option<(u64, UsenetStreamer)>>> = OnceLock::new();
        let cell = CELL.get_or_init(|| Mutex::new(None));
        let fp = nntp_config_fingerprint(&cfg);
        let mut guard = cell.lock().expect("UsenetStreamer::shared mutex poisoned");
        if let Some((stored_fp, streamer)) = guard.as_ref()
            && *stored_fp == fp
        {
            return streamer.clone();
        }
        let streamer = Self::new(cfg, db);
        *guard = Some((fp, streamer.clone()));
        streamer
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
        let meta = store::load(&self.db, info_hash)
            .await?
            .ok_or_else(|| StreamerError::NotIngested(info_hash.to_string()))?;
        let arc = Arc::new(meta);
        self.state.meta_cache.put(info_hash.to_string(), arc.clone());
        self.maybe_kick_backfill(&arc);
        Ok(arc)
    }
}

/// Public accessor: registry of currently-streaming items. The VFS
/// `UsenetSession` registers a stream on first read and removes it when the
/// file handle is dropped (via the `LocalByteSource` stream hooks).
pub fn active_streams() -> Arc<crate::state::ActiveStreams> {
    crate::state::global_active_streams()
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
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
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
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        active_streams().touch(key, now);
    }

    fn stream_unregister(&self, key: &str) {
        active_streams().unregister(key);
    }
}
