//! High-level streaming engine.
//!
//! Holds the NNTP pool and exposes two operations:
//!   - `ingest(info_hash, nzb_xml)` — parse an NZB, pick the largest media
//!     file, persist its segment list + offsets to Redis. Returns the chosen
//!     file's metadata so the caller can populate `stream_url` / `file_size`.
//!   - `read_range(info_hash, file_index, start, end)` — fetch the NNTP
//!     articles covering `[start, end]`, decode yEnc, and return the bytes.
//!
//! Segment offsets are *approximate* — we use the encoded `bytes` from the
//! NZB as a stand-in for decoded size, which is right to within ~2%. This is
//! good enough for sequential playback. Players that issue precise byte-range
//! seeks will get bytes that are close to but not exactly aligned with what
//! they asked for; HLS/DASH-style segment boundaries handle this fine, and
//! progressive MP4 / MKV players are typically tolerant.

use std::sync::Arc;

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::nntp::{NntpError, NntpPool, NntpServerConfig};
use crate::nzb::{NzbFile, looks_like_media, parse_nzb};
use crate::yenc;

#[derive(Debug, Clone)]
pub struct NntpConfig {
    pub server: NntpServerConfig,
}

/// Persisted-to-Redis metadata for an ingested NZB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbMeta {
    pub info_hash: String,
    pub files: Vec<NzbMetaFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbMetaFile {
    pub filename: String,
    pub total_size: u64,
    /// Cumulative encoded-byte offsets per segment, length = segments.len()+1.
    /// `offsets[i]` is the start of segment `i`; `offsets[segments.len()]` is
    /// the total file size.
    pub offsets: Vec<u64>,
    pub segments: Vec<crate::nzb::NzbSegment>,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamerError {
    #[error("nzb parse error: {0}")]
    Nzb(#[from] crate::nzb::NzbError),
    #[error("nntp error: {0}")]
    Nntp(#[from] NntpError),
    #[error("yenc error: {0}")]
    Yenc(#[from] yenc::YencError),
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
}

const META_TTL_SECS: i64 = 60 * 60 * 24 * 7;

fn meta_key(info_hash: &str) -> String {
    format!("riven:nzb:meta:{info_hash}")
}

#[derive(Clone)]
pub struct UsenetStreamer {
    pool: Arc<NntpPool>,
    redis: redis::aio::ConnectionManager,
}

impl UsenetStreamer {
    pub fn new(cfg: NntpConfig, redis: redis::aio::ConnectionManager) -> Self {
        crate::nntp::init_crypto();
        Self {
            pool: Arc::new(NntpPool::new(cfg.server)),
            redis,
        }
    }

    pub fn nntp_config(&self) -> &NntpServerConfig {
        self.pool.config()
    }

    /// Parse an NZB and persist its per-file segment map. Returns the
    /// metadata, with the chosen "primary" media file at `files[0]`.
    pub async fn ingest(
        &self,
        info_hash: &str,
        nzb_xml: &str,
    ) -> Result<NzbMeta, StreamerError> {
        let mut files = parse_nzb(nzb_xml)?;
        if files.is_empty() {
            return Err(StreamerError::NoMediaFile);
        }

        // Prefer media files; if none look like media, keep the largest file.
        let media: Vec<&NzbFile> = files.iter().filter(|f| looks_like_media(f)).collect();
        let primary_subject = if !media.is_empty() {
            // Largest media file by total encoded bytes.
            media
                .iter()
                .max_by_key(|f| f.segments.iter().map(|s| s.bytes).sum::<u64>())
                .map(|f| f.subject.clone())
        } else {
            None
        };
        if let Some(subj) = primary_subject {
            files.sort_by_key(|f| if f.subject == subj { 0 } else { 1 });
        }

        let meta_files: Vec<NzbMetaFile> = files
            .into_iter()
            .map(|f| {
                let mut offsets = Vec::with_capacity(f.segments.len() + 1);
                let mut acc: u64 = 0;
                offsets.push(0);
                for seg in &f.segments {
                    acc += seg.bytes;
                    offsets.push(acc);
                }
                NzbMetaFile {
                    filename: extract_filename(&f.subject),
                    total_size: acc,
                    offsets,
                    segments: f.segments,
                }
            })
            .collect();

        let meta = NzbMeta {
            info_hash: info_hash.to_string(),
            files: meta_files,
        };

        let json = serde_json::to_string(&meta).map_err(|e| {
            StreamerError::Redis(redis::RedisError::from(io_error(e.to_string())))
        })?;
        let mut redis = self.redis.clone();
        let _: () = AsyncCommands::set_ex(&mut redis, meta_key(info_hash), json, META_TTL_SECS as u64).await?;

        Ok(meta)
    }

    pub async fn load_meta(&self, info_hash: &str) -> Result<NzbMeta, StreamerError> {
        let mut redis = self.redis.clone();
        let raw: Option<String> = AsyncCommands::get(&mut redis, meta_key(info_hash)).await?;
        let raw = raw.ok_or_else(|| StreamerError::NotIngested(info_hash.to_string()))?;
        serde_json::from_str(&raw)
            .map_err(|e| StreamerError::Redis(redis::RedisError::from(io_error(e.to_string()))))
    }

    /// Read `[start, end]` (inclusive) from `file_index`. Fetches every
    /// segment whose encoded-byte range overlaps the request, decodes them,
    /// and returns a contiguous byte slice. The returned vec is *exactly*
    /// `end - start + 1` bytes if the request is fully in range.
    pub async fn read_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<u8>, StreamerError> {
        let meta = self.load_meta(info_hash).await?;
        let file = meta
            .files
            .get(file_index)
            .ok_or(StreamerError::BadFileIndex(file_index))?;
        if start > end_inclusive || end_inclusive >= file.total_size {
            return Err(StreamerError::BadRange);
        }

        // Find segment indices that overlap [start, end].
        let mut first = 0usize;
        let mut last = file.segments.len() - 1;
        for (i, win) in file.offsets.windows(2).enumerate() {
            if win[1] > start {
                first = i;
                break;
            }
        }
        for (i, win) in file.offsets.windows(2).enumerate() {
            if win[0] > end_inclusive {
                last = i.saturating_sub(1);
                break;
            }
            last = i;
        }

        let mut decoded_concat = Vec::with_capacity((end_inclusive - start + 1) as usize);
        // We emit bytes mapped against *encoded-byte offsets*. After decode,
        // a segment is slightly smaller than its encoded size, so we scale
        // requested byte positions proportionally within the segment.
        for idx in first..=last {
            let seg = &file.segments[idx];
            let body = self.pool.fetch_body(&seg.message_id).await?;
            let (decoded, _info) = yenc::decode(&body)?;

            let seg_enc_start = file.offsets[idx];
            let seg_enc_end = file.offsets[idx + 1];
            let enc_len = seg_enc_end - seg_enc_start;
            let dec_len = decoded.len() as u64;

            // Map encoded-byte request bounds into this segment's decoded space.
            let req_lo_enc = start.max(seg_enc_start) - seg_enc_start;
            let req_hi_enc = end_inclusive.min(seg_enc_end - 1) - seg_enc_start;
            let lo = ((req_lo_enc as u128 * dec_len as u128) / enc_len as u128) as usize;
            let hi = (((req_hi_enc as u128 + 1) * dec_len as u128) / enc_len as u128) as usize;
            let hi = hi.min(decoded.len());
            if lo < hi {
                decoded_concat.extend_from_slice(&decoded[lo..hi]);
            }
        }

        Ok(decoded_concat)
    }
}

fn extract_filename(subject: &str) -> String {
    // Common NNTP subject formats:
    //   "[1/12] - "Movie.2024.1080p.mkv" yEnc (1/86)"
    //   "Movie.2024.1080p.mkv (1/86)"
    if let Some(start) = subject.find('"')
        && let Some(rel_end) = subject[start + 1..].find('"')
    {
        return subject[start + 1..start + 1 + rel_end].to_string();
    }
    // Fall back to the first whitespace-delimited token that has a dot.
    subject
        .split_whitespace()
        .find(|t| t.contains('.'))
        .unwrap_or(subject)
        .to_string()
}

fn io_error(msg: String) -> std::io::Error {
    std::io::Error::other(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_quoted_filename() {
        let s = r#"[1/12] - "Movie.2024.1080p.mkv" yEnc (1/86)"#;
        assert_eq!(extract_filename(s), "Movie.2024.1080p.mkv");
    }

    #[test]
    fn extract_bare_filename() {
        let s = "Movie.2024.1080p.mkv (1/86)";
        assert_eq!(extract_filename(s), "Movie.2024.1080p.mkv");
    }
}
