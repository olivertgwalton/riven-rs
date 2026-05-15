//! Persisted-to-Redis NZB metadata and the helpers that read/index it.

use serde::{Deserialize, Serialize};

use crate::nzb::NzbSegment;

pub(crate) const META_TTL_SECS: i64 = 60 * 60 * 24 * 7;

pub(crate) fn meta_key(info_hash: &str) -> String {
    format!("riven:nzb:meta:{info_hash}")
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NzbMeta {
    pub info_hash: String,
    pub files: Vec<NzbMetaFile>,
    /// Password used to decrypt encrypted RAR archives in this NZB. Only
    /// populated when at least one volume's file header reported encryption.
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

/// How to materialize bytes for a `NzbMetaFile`:
///   - `Direct`: segments map straight to the byte stream of the produced file.
///   - `Rar`: bytes are assembled from contiguous slices of one or more
///     top-level NZB files (`.rar`/`.rNN`) that form a stored multi-volume
///     archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NzbMetaSource {
    Direct {
        /// Cumulative encoded-byte offsets per segment, length = segments.len()+1.
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
    /// Uniform decoded byte size of every non-last segment. yEnc posters use
    /// a fixed `=ypart` size, so once known each segment boundary is an O(1)
    /// lookup. `None` means legacy meta — read path falls back to walking.
    #[serde(default)]
    pub decoded_seg_size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbRarSlice {
    pub part_index: usize,
    pub start_in_part: u64,
    pub length: u64,
    /// If present, this slice's data area is AES-256-CBC encrypted with the
    /// parameters here. `length` is the *plaintext* contribution; the on-volume
    /// bytes occupy `ciphertext_length`.
    #[serde(default)]
    pub encryption: Option<crate::rar::RarEncryption>,
    /// Bytes occupied on-volume by this slice's ciphertext. Equals `length`
    /// for unencrypted slices; rounded up to 16-byte alignment for encrypted.
    #[serde(default)]
    pub ciphertext_length: u64,
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
