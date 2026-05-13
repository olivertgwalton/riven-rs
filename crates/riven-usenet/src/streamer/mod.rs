//! High-level streaming engine.
//!
//! Holds the NNTP pool and exposes two operations:
//!   - `ingest(info_hash, nzb_xml)` — parse an NZB. If it contains a
//!     stored multi-volume RAR archive, pre-fetch the volume headers to
//!     locate the contained file, then expose that contained file as the
//!     primary virtual file. Otherwise pick the largest media file in the
//!     NZB and expose it directly. Persist segment maps to Redis.
//!   - `read_range(info_hash, file_index, start, end)` — fetch the NNTP
//!     articles covering `[start, end]`, decode yEnc, and return the bytes.
//!     For RAR-contained virtual files, translates virtual byte ranges to
//!     per-volume slices before fetching.
//!
//! Segment offsets are *approximate* — we use the encoded `bytes` from the
//! NZB as a stand-in for decoded size, which is right to within ~2%. This is
//! good enough for sequential playback. Players that issue precise byte-range
//! seeks will get bytes that are close to but not exactly aligned with what
//! they asked for; HLS/DASH-style segment boundaries handle this fine, and
//! progressive MP4 / MKV players are typically tolerant.

use std::sync::Arc;

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::cache::SegmentCache;
use crate::nntp::{NntpPool, NntpServerConfig};
use crate::nzb::NzbSegment;
use crate::state::{ActiveStreams, DecodedSizes, InFlight, MetaCache, PermanentFails};

mod ingest;
mod read;
mod read_rar;
#[cfg(test)]
mod tests;

/// Pinned future used to homogenize the async blocks pushed into a
/// `FuturesOrdered`. Each call site produces a distinct anonymous type;
/// boxing erases the type so they fit in the same collection.
pub(crate) type FetchFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub(crate) const READ_PREFETCH_WINDOW: usize = 8;
/// Default decoded-segment cache budget. Overridable via env var.
/// Size up linearly with expected concurrent stream count: each stream
/// needs roughly 10-20 MB of warm segments to keep the body stream's
/// lookahead populated. Default 256 MB ≈ 12 concurrent streams.
const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct NntpConfig {
    /// One or more NNTP providers ordered by intent. Empty backups are
    /// fine — a single primary is the common case. Order doesn't matter
    /// for ingest; the pool sorts internally by `(is_backup, priority)`.
    pub providers: Vec<crate::nntp::NntpProvider>,
}

impl NntpConfig {
    pub fn single(server: NntpServerConfig) -> Self {
        Self {
            providers: vec![crate::nntp::NntpProvider {
                config: server,
                priority: 0,
                is_backup: false,
            }],
        }
    }

    pub fn primary(&self) -> Option<&NntpServerConfig> {
        self.providers.first().map(|p| &p.config)
    }
}

/// Persisted-to-Redis metadata for an ingested NZB.
#[derive(Clone, Serialize, Deserialize)]
pub struct NzbMeta {
    pub info_hash: String,
    pub files: Vec<NzbMetaFile>,
    /// Password used to decrypt encrypted RAR archives in this NZB. Only
    /// populated when at least one volume's file header reported
    /// encryption. Stored alongside metadata so reads don't need to look
    /// it up from plugin settings (which may have changed) or recompute
    /// PBKDF2 from scratch unnecessarily.
    #[serde(default)]
    pub password: Option<String>,
}

// Manual Debug so accidental `tracing::debug!(?meta)` doesn't print the
// archive password.
impl std::fmt::Debug for NzbMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NzbMeta")
            .field("info_hash", &self.info_hash)
            .field("files", &self.files)
            .field("password", &self.password.as_deref().map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbMetaFile {
    pub filename: String,
    pub total_size: u64,
    pub source: NzbMetaSource,
}

/// How to materialize bytes for a `NzbMetaFile`. Either:
///   - `Direct`: the file IS a top-level NZB file; segments map straight to
///     the byte stream of the produced file.
///   - `Rar`: the file is contained inside a stored multi-volume RAR
///     archive. Bytes are assembled from contiguous slices of one or more
///     `parts`, each of which is a top-level NZB file (a `.rar`/`.rNN`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NzbMetaSource {
    Direct {
        /// Cumulative encoded-byte offsets per segment, length = segments.len()+1.
        /// `offsets[i]` is the start of segment `i`; `offsets[segments.len()]`
        /// is the total file size.
        offsets: Vec<u64>,
        segments: Vec<NzbSegment>,
    },
    Rar {
        parts: Vec<NzbRarPart>,
        /// Contiguous slices that compose the contained file, in order. The
        /// total of `slices[i].length` equals the file's `total_size`.
        slices: Vec<NzbRarSlice>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbRarPart {
    pub filename: String,
    pub total_size: u64,
    pub offsets: Vec<u64>,
    pub segments: Vec<NzbSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbRarSlice {
    pub part_index: usize,
    pub start_in_part: u64,
    pub length: u64,
    /// If present, this slice's data area is AES-256-CBC encrypted with
    /// the parameters here. The `length` field still refers to the
    /// *plaintext* contribution from this volume; the on-volume bytes
    /// occupy `ciphertext_length` (rounded up to 16-byte alignment).
    #[serde(default)]
    pub encryption: Option<crate::rar::RarEncryption>,
    /// Bytes occupied on-volume by this slice's ciphertext. For
    /// unencrypted slices this equals `length`; for encrypted slices it
    /// is `length` rounded up to the next AES block boundary (16 bytes).
    #[serde(default)]
    pub ciphertext_length: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamerError {
    #[error("nzb parse error: {0}")]
    Nzb(#[from] crate::nzb::NzbError),
    #[error("nntp error: {0}")]
    Nntp(#[from] crate::nntp::NntpError),
    #[error("yenc error: {0}")]
    Yenc(#[from] crate::yenc::YencError),
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("metadata not found for info_hash {0}")]
    NotIngested(String),
    #[error("file index {0} out of range")]
    BadFileIndex(usize),
    #[error("range out of bounds")]
    BadRange,
    #[error("no media files in NZB")]
    NoMediaFile,
    #[error("article availability too low: {missing}/{checked} segments missing from provider")]
    IncompleteRelease { missing: usize, checked: usize },
    #[error("archive is encrypted but no password was provided")]
    MissingPassword,
    #[error("crypto error: {0}")]
    Crypto(#[from] crate::crypto::CryptoError),
}

pub(crate) const META_TTL_SECS: i64 = 60 * 60 * 24 * 7;

pub(crate) fn meta_key(info_hash: &str) -> String {
    format!("riven:nzb:meta:{info_hash}")
}

#[derive(Clone)]
pub struct UsenetStreamer {
    pub(crate) pool: Arc<NntpPool>,
    pub(crate) cache: Arc<SegmentCache>,
    pub(crate) meta_cache: Arc<MetaCache>,
    pub(crate) decoded_sizes: Arc<DecodedSizes>,
    pub(crate) fails: Arc<PermanentFails>,
    pub(crate) in_flight: Arc<InFlight>,
    pub(crate) redis: redis::aio::ConnectionManager,
}

/// Process-wide segment cache. Sharing the cache across all `UsenetStreamer`
/// instances means the RAR header bytes that plugin-usenet's ingest fetches
/// stay hot for the read-path streamer's subsequent serves.
fn global_cache() -> Arc<SegmentCache> {
    static CACHE: OnceLock<Arc<SegmentCache>> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            let cache_bytes = std::env::var("RIVEN_USENET_CACHE_BYTES")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(DEFAULT_CACHE_BYTES);
            Arc::new(SegmentCache::new(cache_bytes))
        })
        .clone()
}

fn global_meta_cache() -> Arc<MetaCache> {
    static C: OnceLock<Arc<MetaCache>> = OnceLock::new();
    C.get_or_init(|| Arc::new(MetaCache::default())).clone()
}

fn global_decoded_sizes() -> Arc<DecodedSizes> {
    static C: OnceLock<Arc<DecodedSizes>> = OnceLock::new();
    C.get_or_init(|| Arc::new(DecodedSizes::default())).clone()
}

fn global_permanent_fails() -> Arc<PermanentFails> {
    static C: OnceLock<Arc<PermanentFails>> = OnceLock::new();
    C.get_or_init(|| Arc::new(PermanentFails::default()))
        .clone()
}

fn global_in_flight() -> Arc<InFlight> {
    static C: OnceLock<Arc<InFlight>> = OnceLock::new();
    C.get_or_init(|| Arc::new(InFlight::default())).clone()
}

/// Public accessor: registry of currently-streaming items. The
/// `/usenet/` handler registers a stream on body-stream start and
/// removes it when the body completes or is dropped.
pub fn active_streams() -> Arc<ActiveStreams> {
    static C: OnceLock<Arc<ActiveStreams>> = OnceLock::new();
    C.get_or_init(|| Arc::new(ActiveStreams::default())).clone()
}

impl UsenetStreamer {
    pub fn new(cfg: NntpConfig, redis: redis::aio::ConnectionManager) -> Self {
        crate::nntp::init_crypto();
        Self {
            pool: Arc::new(NntpPool::new_multi(cfg.providers)),
            cache: global_cache(),
            meta_cache: global_meta_cache(),
            decoded_sizes: global_decoded_sizes(),
            fails: global_permanent_fails(),
            in_flight: global_in_flight(),
            redis,
        }
    }

    pub fn nntp_config(&self) -> &NntpServerConfig {
        self.pool.config()
    }

    pub async fn load_meta(&self, info_hash: &str) -> Result<Arc<NzbMeta>, StreamerError> {
        if let Some(hit) = self.meta_cache.get(info_hash) {
            return Ok(hit);
        }
        let mut redis = self.redis.clone();
        let raw: Option<String> =
            redis::AsyncCommands::get(&mut redis, meta_key(info_hash)).await?;
        let raw = raw.ok_or_else(|| StreamerError::NotIngested(info_hash.to_string()))?;
        let meta: NzbMeta = serde_json::from_str(&raw)
            .map_err(|e| StreamerError::Redis(redis::RedisError::from(io_error(e.to_string()))))?;
        let arc = Arc::new(meta);
        self.meta_cache.put(info_hash.to_string(), arc.clone());
        Ok(arc)
    }
}

pub(crate) fn io_error(msg: String) -> std::io::Error {
    std::io::Error::other(msg)
}

/// Indices of segments whose encoded-byte range overlaps `[lo, hi]`.
pub(crate) fn segments_overlapping(
    offsets: &[u64],
    segments: &[NzbSegment],
    lo: u64,
    hi: u64,
) -> Vec<String> {
    if segments.is_empty() {
        return Vec::new();
    }
    let mut first = 0usize;
    let mut last = segments.len() - 1;
    for (i, win) in offsets.windows(2).enumerate() {
        if win[1] > lo {
            first = i;
            break;
        }
    }
    for (i, win) in offsets.windows(2).enumerate() {
        if win[0] > hi {
            last = i.saturating_sub(1);
            break;
        }
        last = i;
    }
    (first..=last)
        .map(|i| segments[i].message_id.clone())
        .collect()
}
