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

use redis::AsyncCommands;

use crate::nntp::{NntpConfig, NntpPool};
use crate::state::StreamerState;

mod backfill;
mod error;
mod ingest;
mod meta;
mod read_direct;
mod read_rar;
#[cfg(test)]
mod tests;

pub use error::StreamerError;
pub use meta::{NzbMeta, NzbMetaFile, NzbMetaSource, NzbRarPart, NzbRarSlice};

pub(crate) use meta::{io_error, meta_key, segments_overlapping};

/// Floor on the prefetch fan-out. Most playback paths derive their
/// concurrency from `NntpPool::total_capacity()` so the user's
/// `max_connections` setting is the real ceiling; this floor protects the
/// degenerate case of a single-connection misconfiguration.
pub(crate) const PREFETCH_FLOOR: usize = 4;

#[derive(Clone)]
pub struct UsenetStreamer {
    pub(crate) pool: Arc<NntpPool>,
    pub(crate) state: Arc<StreamerState>,
    pub(crate) redis: redis::aio::ConnectionManager,
}

impl UsenetStreamer {
    pub fn new(cfg: NntpConfig, redis: redis::aio::ConnectionManager) -> Self {
        crate::nntp::init_crypto();
        let pool = NntpPool::new_multi(cfg.providers);
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
            redis,
        }
    }

    pub fn shared(cfg: NntpConfig, redis: redis::aio::ConnectionManager) -> Self {
        static CELL: OnceLock<Mutex<Option<(u64, UsenetStreamer)>>> = OnceLock::new();
        let cell = CELL.get_or_init(|| Mutex::new(None));
        let fp = nntp_config_fingerprint(&cfg);
        let mut guard = cell.lock().expect("UsenetStreamer::shared mutex poisoned");
        if let Some((stored_fp, streamer)) = guard.as_ref()
            && *stored_fp == fp
        {
            return streamer.clone();
        }
        let streamer = Self::new(cfg, redis);
        *guard = Some((fp, streamer.clone()));
        streamer
    }

    pub fn pool(&self) -> Arc<NntpPool> {
        self.pool.clone()
    }

    pub async fn load_meta(&self, info_hash: &str) -> Result<Arc<NzbMeta>, StreamerError> {
        if let Some(hit) = self.state.meta_cache.get(info_hash) {
            self.maybe_kick_backfill(&hit);
            return Ok(hit);
        }
        let mut redis = self.redis.clone();
        let raw: Option<String> = AsyncCommands::get(&mut redis, meta_key(info_hash)).await?;
        let raw = raw.ok_or_else(|| StreamerError::NotIngested(info_hash.to_string()))?;
        let meta: NzbMeta = serde_json::from_str(&raw)
            .map_err(|e| StreamerError::Redis(redis::RedisError::from(io_error(e.to_string()))))?;
        let arc = Arc::new(meta);
        self.state.meta_cache.put(info_hash.to_string(), arc.clone());
        self.maybe_kick_backfill(&arc);
        Ok(arc)
    }
}

/// Public accessor: registry of currently-streaming items. The `/usenet/`
/// handler registers a stream on body-stream start and removes it when the
/// body completes or is dropped.
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
