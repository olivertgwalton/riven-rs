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

use std::sync::Arc;

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

pub(crate) const READ_PREFETCH_WINDOW: usize = 8;

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
