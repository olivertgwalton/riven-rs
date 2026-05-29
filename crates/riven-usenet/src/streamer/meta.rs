//! Persisted-to-Postgres NZB metadata and the helpers that read/index it.
//!
//! Storage rationale lives next to the [`028_usenet_meta`] migration; the
//! short version is that NNTP message-ids don't expire upstream, so the
//! segment map is permanent address-book data, not a refreshable cache.

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::nzb::NzbSegment;

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
    let n = segments.len();
    // `offsets[i]..offsets[i+1]` is segment `i`'s encoded-byte span (offsets is
    // sorted, length `n + 1`). Binary-search the boundaries instead of the
    // previous two linear `windows(2)` scans.
    //
    // first = smallest `i` with `offsets[i + 1] > lo`. `partition_point`
    // returns the count of offsets `<= lo`, so the first offset index strictly
    // greater than `lo` is that count; the matching window start is one less.
    // Falls back to 0 (the old loop's default) when nothing exceeds `lo`.
    let first = if offsets.last().is_some_and(|&end| end > lo) {
        offsets.partition_point(|&o| o <= lo).saturating_sub(1)
    } else {
        0
    };
    // last = largest `i` reached before `offsets[i] > hi`; defaults to `n - 1`.
    // `partition_point` over the first `n` offsets returns the first index where
    // `offsets[i] > hi` (or `n` when none do), matching the old loop's `i - 1`.
    let last = offsets[..n].partition_point(|&o| o <= hi).saturating_sub(1);
    (first..=last)
        .map(|i| segments[i].message_id.clone())
        .collect()
}

/// Concatenate decoded segment slices into one contiguous `Bytes`. Used by the
/// direct and RAR readers for callers that want a single buffer (HTTP buffered
/// responses, RAR encrypted-slice decrypt). Single slice → zero-copy return;
/// multi-slice → concat into a sized `BytesMut`. The streaming HTTP path uses
/// the slice list directly and skips this.
pub(crate) fn concat_slices(mut slices: Vec<Bytes>, start: u64, end_inclusive: u64) -> Bytes {
    match slices.len() {
        0 => Bytes::new(),
        1 => slices.pop().unwrap_or_default(),
        _ => {
            let mut buf = BytesMut::with_capacity((end_inclusive - start + 1) as usize);
            for s in slices {
                buf.extend_from_slice(&s);
            }
            buf.freeze()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(id: &str) -> NzbSegment {
        NzbSegment {
            bytes: 0,
            number: 0,
            message_id: id.to_string(),
        }
    }

    #[test]
    fn segments_overlapping_picks_touched_segments() {
        // 3 segments: [0,100), [100,250), [250,400).
        let offsets = [0u64, 100, 250, 400];
        let segments = [seg("a"), seg("b"), seg("c")];

        let ids = |lo, hi| segments_overlapping(&offsets, &segments, lo, hi);

        // Inside the first segment.
        assert_eq!(ids(0, 0), vec!["a"]);
        assert_eq!(ids(50, 99), vec!["a"]);
        // Spans the first boundary.
        assert_eq!(ids(50, 150), vec!["a", "b"]);
        // Starts mid-segment-1, ends in segment-2.
        assert_eq!(ids(120, 300), vec!["b", "c"]);
        // Exactly on a boundary start picks the later segment.
        assert_eq!(ids(100, 100), vec!["b"]);
        // Whole file.
        assert_eq!(ids(0, 399), vec!["a", "b", "c"]);
    }

    #[test]
    fn segments_overlapping_empty_is_empty() {
        assert!(segments_overlapping(&[0], &[], 0, 10).is_empty());
    }
}
