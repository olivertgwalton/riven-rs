//! Persisted-to-Postgres NZB metadata and the helpers that read/index it.
//!
//! Storage rationale lives next to the [`028_usenet_meta`] migration; the
//! short version is that NNTP message-ids don't expire upstream, so the
//! segment map is permanent address-book data, not a refreshable cache.

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use super::DEFAULT_AVAILABILITY_SAMPLE_PERCENT;
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
    let first = if offsets.last().is_some_and(|&end| end > lo) {
        offsets.partition_point(|&o| o <= lo).saturating_sub(1)
    } else {
        0
    };
    let last = offsets[..n].partition_point(|&o| o <= hi).saturating_sub(1);
    (first..=last)
        .map(|i| segments[i].message_id.clone())
        .collect()
}

/// Pick which segment indices to STAT-probe for availability. A
/// `sample_percent` of 100 or more returns every index — full verification,
/// the only mode that reliably catches a *single* dead article in a large
/// file. Otherwise it returns a strategic sample (mirroring altmount and the
/// background health
/// scanner): the first `FIRST_N` segments catch DMCA takedowns (which nuke a
/// release's head), the last `LAST_N` catch truncated uploads, and an
/// evenly-spaced middle catches general retention loss. Strictly better
/// coverage than the old uniform stride for the same STAT budget. Returned
/// indices are sorted and de-duplicated.
pub(crate) fn select_validation_indices(total: usize, sample_percent: usize) -> Vec<usize> {
    if total == 0 {
        return Vec::new();
    }
    if sample_percent >= 100 {
        return (0..total).collect();
    }
    const FIRST_N: usize = 3;
    const LAST_N: usize = 2;
    const SAMPLE_MIN: usize = 20;
    const SAMPLE_MAX: usize = 150;

    let pct = if (1..=100).contains(&sample_percent) {
        sample_percent
    } else {
        DEFAULT_AVAILABILITY_SAMPLE_PERCENT
    };
    let n = ((total * pct) / 100)
        .clamp(SAMPLE_MIN, SAMPLE_MAX)
        .min(total);
    if n >= total || total <= FIRST_N + LAST_N {
        return (0..total).collect();
    }

    let mut indices: Vec<usize> = (0..FIRST_N).collect();
    indices.extend((total - LAST_N)..total);

    let middle_start = FIRST_N;
    let middle_end = total - LAST_N;
    let middle_range = middle_end - middle_start;
    let middle_count = n.saturating_sub(FIRST_N + LAST_N);
    for i in 0..middle_count {
        let idx = middle_start + ((2 * i + 1) * middle_range) / (2 * middle_count.max(1));
        if idx < middle_end {
            indices.push(idx);
        }
    }
    indices.sort_unstable();
    indices.dedup();
    indices
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
    fn validation_full_coverage_at_100_percent() {
        let got = select_validation_indices(36_526, 100);
        assert_eq!(got.len(), 36_526);
        assert_eq!(got.first(), Some(&0));
        assert_eq!(got.last(), Some(&36_525));

        let over = select_validation_indices(10, 250);
        assert_eq!(over, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn validation_sample_includes_head_tail_and_is_bounded() {
        let total = 36_526;
        let got = select_validation_indices(total, 5);
        for i in 0..3 {
            assert!(got.contains(&i), "missing head index {i}");
        }
        assert!(got.contains(&(total - 1)));
        assert!(got.contains(&(total - 2)));
        assert!(got.windows(2).all(|w| w[0] < w[1]), "not sorted/unique");
        assert!(got.len() <= 150, "sample exceeded the cap: {}", got.len());
        assert!(got.iter().all(|&i| i < total));
    }

    #[test]
    fn validation_small_file_probes_everything() {
        assert_eq!(select_validation_indices(4, 5), vec![0, 1, 2, 3]);
        assert_eq!(select_validation_indices(0, 5), Vec::<usize>::new());
    }

    #[test]
    fn segments_overlapping_picks_touched_segments() {
        let offsets = [0u64, 100, 250, 400];
        let segments = [seg("a"), seg("b"), seg("c")];

        let ids = |lo, hi| segments_overlapping(&offsets, &segments, lo, hi);

        assert_eq!(ids(0, 0), vec!["a"]);
        assert_eq!(ids(50, 99), vec!["a"]);
        assert_eq!(ids(50, 150), vec!["a", "b"]);
        assert_eq!(ids(120, 300), vec!["b", "c"]);
        assert_eq!(ids(100, 100), vec!["b"]);
        assert_eq!(ids(0, 399), vec!["a", "b", "c"]);
    }

    #[test]
    fn segments_overlapping_empty_is_empty() {
        assert!(segments_overlapping(&[0], &[], 0, 10).is_empty());
    }
}
