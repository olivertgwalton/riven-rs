//! Persisted-to-Postgres NZB metadata and the helpers that read/index it.
//!
//! Storage rationale lives next to the [`028_usenet_meta`] migration; the
//! short version is that NNTP message-ids don't expire upstream, so the
//! segment map is permanent address-book data, not a refreshable cache.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use super::DEFAULT_AVAILABILITY_SAMPLE_PERCENT;

#[derive(Clone)]
pub struct NzbMeta {
    pub info_hash: String,
    pub files: Vec<NzbMetaFile>,
    /// The distinct RAR volume sets this release contains, each shared by every
    /// inner media file that addresses it.
    ///
    /// A season pack posted as one RAR archive yields one file per episode, and
    /// every one of them is served by the *same* volumes. They used to each
    /// carry a full copy: measured on this library, 3 154 MB of stored volume
    /// data against 1 696 MB of distinct volume data — **1 458 MB, 46 %, was
    /// the same segments written again**. On the worst release it was one
    /// 6.7 MB array stored twenty times, 131 MB of a 145 MB document.
    ///
    /// pglz never collapsed it because its match window is a few KB and the
    /// copies sit megabytes apart, so the duplication was invisible to both
    /// compression and to anyone looking at the compressed size.
    pub rar_sets: Vec<Arc<Vec<NzbRarPart>>>,
    /// Password used to decrypt encrypted RAR archives in this NZB. Only
    /// populated when at least one volume's file header reported encryption.
    pub password: Option<String>,
}

fn legacy_version() -> u32 {
    1
}

// ── Wire format ──────────────────────────────────────────────────────────────
// Stored shape, kept separate from the in-memory one so the sharing above is a
// property of the type rather than of every call site that touches it.
//
// Reads accept both shapes: rows written before this carry the volumes inline
// under `parts`, and are folded into shared sets on load — identical arrays
// collapse to one, so an old row costs its deduplicated size in memory the
// moment it is read, without being rewritten.

/// Format version of a stored document.
///
/// Exists so "does this row need rewriting?" is one indexable comparison
/// rather than a scan for the absence of a field. `1` is everything written
/// before deduplication — the value is defaulted on read, so no existing row
/// needs touching for the check to work.
///
/// Old binaries ignore it: serde drops unknown fields, so a `v` they have
/// never heard of costs them nothing.
pub(crate) const META_FORMAT_VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
struct WireMeta {
    /// See [`META_FORMAT_VERSION`].
    #[serde(default = "legacy_version")]
    v: u32,
    info_hash: String,
    files: Vec<WireFile>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rar_sets: Vec<Vec<NzbRarPart>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    password: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct WireFile {
    filename: String,
    total_size: u64,
    source: WireSource,
}

#[derive(Serialize, Deserialize)]
enum WireSource {
    Direct {
        offsets: Vec<u64>,
        segments: crate::segments::SegmentList,
    },
    Rar {
        /// Index into [`WireMeta::rar_sets`]. Absent on rows written before
        /// deduplication, which carry `parts` instead.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        set: Option<usize>,
        /// Legacy inline volumes. Never written any more.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        parts: Vec<NzbRarPart>,
        slices: Vec<NzbRarSlice>,
    },
}

impl Serialize for NzbMeta {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Distinct by allocation: the sets are built once and cloned as `Arc`s,
        // so pointer identity is exactly the sharing being recorded.
        let mut sets: Vec<Arc<Vec<NzbRarPart>>> = Vec::new();
        let index_of = |sets: &mut Vec<Arc<Vec<NzbRarPart>>>, parts: &Arc<Vec<NzbRarPart>>| {
            if let Some(i) = sets.iter().position(|s| Arc::ptr_eq(s, parts)) {
                return i;
            }
            sets.push(Arc::clone(parts));
            sets.len() - 1
        };

        let files: Vec<WireFile> = self
            .files
            .iter()
            .map(|f| WireFile {
                filename: f.filename.clone(),
                total_size: f.total_size,
                source: match &f.source {
                    NzbMetaSource::Direct { offsets, segments } => WireSource::Direct {
                        offsets: offsets.clone(),
                        segments: segments.clone(),
                    },
                    NzbMetaSource::Rar { parts, slices } => WireSource::Rar {
                        set: Some(index_of(&mut sets, parts)),
                        parts: Vec::new(),
                        slices: slices.clone(),
                    },
                },
            })
            .collect();

        WireMeta {
            v: META_FORMAT_VERSION,
            info_hash: self.info_hash.clone(),
            files,
            rar_sets: sets.iter().map(|s| (**s).clone()).collect(),
            password: self.password.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NzbMeta {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = WireMeta::deserialize(deserializer)?;
        let sets: Vec<Arc<Vec<NzbRarPart>>> = wire.rar_sets.into_iter().map(Arc::new).collect();
        // Legacy rows repeat the same volumes per inner file; fold identical
        // ones onto one allocation so an old row costs its deduplicated size in
        // memory even before it is rewritten.
        let mut legacy: Vec<Arc<Vec<NzbRarPart>>> = Vec::new();

        let files = wire
            .files
            .into_iter()
            .map(|f| NzbMetaFile {
                filename: f.filename,
                total_size: f.total_size,
                source: match f.source {
                    WireSource::Direct { offsets, segments } => {
                        NzbMetaSource::Direct { offsets, segments }
                    }
                    WireSource::Rar { set, parts, slices } => {
                        let parts = match set.and_then(|i| sets.get(i)) {
                            Some(shared) => Arc::clone(shared),
                            None => match legacy.iter().find(|s| ***s == parts) {
                                Some(shared) => Arc::clone(shared),
                                None => {
                                    let shared = Arc::new(parts);
                                    legacy.push(Arc::clone(&shared));
                                    shared
                                }
                            },
                        };
                        NzbMetaSource::Rar { parts, slices }
                    }
                },
            })
            .collect();

        let rar_sets = if sets.is_empty() { legacy } else { sets };
        Ok(NzbMeta {
            info_hash: wire.info_hash,
            files,
            rar_sets,
            password: wire.password,
        })
    }
}

/// Placeholder used in log fields when a name genuinely isn't available, so a
/// log line always has the `file`/`release` field present and greppable rather
/// than silently dropping it.
pub const UNKNOWN_FILE_LABEL: &str = "<unknown>";

/// Match against the same extensions the downstream persist step accepts as
/// playable video — see `crates/riven-queue/src/flows/download_item/helpers.rs`
/// `is_video_file`. Kept in sync intentionally: returning a virtual file
/// whose extension the queue ignores wastes an ingest cycle.
pub(crate) fn is_media_filename(name: &str) -> bool {
    let ext = name.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    matches!(
        ext.as_str(),
        "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm"
    )
}

impl NzbMeta {
    /// Name for the release as a whole, for log fields. An `info_hash` is a
    /// SHA-1 of the NZB URL and says nothing about what is playing, so every
    /// log line about a release carries this alongside it. The primary media
    /// file is the closest thing a meta row has to a title — ingest orders it
    /// first, and for a season pack it at least identifies the show.
    pub fn label(&self) -> &str {
        self.files
            .iter()
            .find(|f| is_media_filename(&f.filename))
            .or_else(|| self.files.first())
            .map_or(UNKNOWN_FILE_LABEL, |f| f.filename.as_str())
    }

    /// Name of the file at `file_index`, for log fields.
    pub fn file_label(&self, file_index: usize) -> &str {
        self.files
            .get(file_index)
            .map_or(UNKNOWN_FILE_LABEL, |f| f.filename.as_str())
    }
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

/// What a caller of [`ingest`](crate::UsenetStreamer::ingest) actually needs:
/// the files a release yields, and how big each one is.
///
/// `ingest` used to hand back the whole [`NzbMeta`]. Its one caller reads
/// `filename` and `total_size` and nothing else, so on a re-scrape — the
/// idempotent path — that deserialised the entire stored document to produce a
/// list of names. Measured on this library: 145 MB of JSON parsed for 56 KB of
/// answer across 520 files, a ~2 600× amplification, and then a deep clone of
/// all of it on top.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestedFile {
    pub filename: String,
    pub total_size: u64,
}

impl From<&NzbMetaFile> for IngestedFile {
    fn from(f: &NzbMetaFile) -> Self {
        Self {
            filename: f.filename.clone(),
            total_size: f.total_size,
        }
    }
}

/// One file's segment map plus the release-level bits a read of it needs.
///
/// This is the unit the read path caches, rather than the whole [`NzbMeta`]:
/// a season pack is one row holding every episode, so resolving a read through
/// the release deserialised all of them. `password` rides along because it is
/// stored per release but consumed per read.
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub file: NzbMetaFile,
    pub password: Option<String>,
}

impl std::ops::Deref for FileMeta {
    type Target = NzbMetaFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
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
        segments: crate::segments::SegmentList,
    },
    Rar {
        /// The volumes. Shared with every other inner file of the same RAR set
        /// — see [`NzbMeta::rar_sets`] for what copying it cost.
        #[serde(skip)]
        parts: Arc<Vec<NzbRarPart>>,
        /// Contiguous slices that compose the contained file, in order. The
        /// total of `slices[i].length` equals the file's `total_size`.
        slices: Vec<NzbRarSlice>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NzbRarPart {
    pub filename: String,
    pub total_size: u64,
    pub offsets: Vec<u64>,
    pub segments: crate::segments::SegmentList,
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

#[cfg(test)]
mod dedup_tests {
    use super::*;

    fn part(name: &str) -> NzbRarPart {
        NzbRarPart {
            filename: name.into(),
            total_size: 100,
            offsets: vec![0, 50, 100],
            segments: [
                crate::segments::NzbSegment {
                    bytes: 50,
                    message_id: format!("{name}-a@h"),
                },
                crate::segments::NzbSegment {
                    bytes: 50,
                    message_id: format!("{name}-b@h"),
                },
            ]
            .into_iter()
            .collect(),
            decoded_seg_size: Some(50),
        }
    }

    fn rar_file(name: &str, parts: &Arc<Vec<NzbRarPart>>) -> NzbMetaFile {
        NzbMetaFile {
            filename: name.into(),
            total_size: 100,
            source: NzbMetaSource::Rar {
                parts: Arc::clone(parts),
                slices: Vec::new(),
            },
        }
    }

    /// Two inner files of one archive must serialise the volumes once, not
    /// twice — the whole point. On this library that ratio was 20:1 and worth
    /// 1 458 MB across the table.
    #[test]
    fn one_archive_stores_its_volumes_once() {
        let shared = Arc::new(vec![part("v1"), part("v2")]);
        let meta = NzbMeta {
            info_hash: "nzb-x".into(),
            files: vec![rar_file("ep1.mkv", &shared), rar_file("ep2.mkv", &shared)],
            rar_sets: vec![Arc::clone(&shared)],
            password: None,
        };

        let json = serde_json::to_string(&meta).expect("serialise");
        assert_eq!(json.matches("v1-a@h").count(), 1, "volumes written twice");
        assert!(json.contains("\"rar_sets\""));

        let back: NzbMeta = serde_json::from_str(&json).expect("round trip");
        assert_eq!(back.files.len(), 2);
        let (NzbMetaSource::Rar { parts: a, .. }, NzbMetaSource::Rar { parts: b, .. }) =
            (&back.files[0].source, &back.files[1].source)
        else {
            panic!("expected two Rar files");
        };
        assert!(Arc::ptr_eq(a, b), "both files must share one allocation");
        assert_eq!(a.len(), 2);
        assert_eq!(a[1].filename, "v2");
    }

    /// `parts` is `#[serde(skip)]` — the deduplicated document keeps the
    /// volumes in `rar_sets`, not inside each file — so a `NzbMetaFile`
    /// deserialised **on its own** comes back with none. Anything loading one
    /// file at a time has to supply them separately.
    ///
    /// This is pinned because getting it wrong is silent: the file parses, the
    /// sizes are right, and reads fail much later at
    /// `parts.get(slice.part_index)` with an out-of-range index rather than
    /// anywhere near the cause.
    #[test]
    fn a_file_deserialised_alone_carries_no_volumes() {
        let shared = Arc::new(vec![part("v1")]);
        let file = rar_file("ep1.mkv", &shared);
        let json = serde_json::to_string(&file).expect("serialise");
        assert!(
            !json.contains("v1-a@h"),
            "a file must not carry the volumes; they live in rar_sets"
        );

        let alone: NzbMetaFile = serde_json::from_str(&json).expect("deserialise");
        let NzbMetaSource::Rar { parts, .. } = &alone.source else {
            panic!("expected a Rar file");
        };
        assert!(
            parts.is_empty(),
            "volumes cannot survive a standalone file; the loader must inject them"
        );
    }

    /// Every row already stored repeats the volumes inline per file. Those must
    /// keep loading, and must collapse onto one allocation as they do, so an
    /// old row costs its deduplicated size in memory before it is ever
    /// rewritten.
    #[test]
    fn a_legacy_row_loads_and_collapses() {
        let inline = serde_json::json!({
            "info_hash": "nzb-legacy",
            "files": [
                { "filename": "ep1.mkv", "total_size": 100, "source": { "Rar": {
                    "parts": [{ "filename": "v1", "total_size": 100, "offsets": [0, 50],
                                "segments": [{"bytes": 50, "number": 1, "message_id": "v1-a@h"}],
                                "decoded_seg_size": 50 }],
                    "slices": [] } } },
                { "filename": "ep2.mkv", "total_size": 100, "source": { "Rar": {
                    "parts": [{ "filename": "v1", "total_size": 100, "offsets": [0, 50],
                                "segments": [{"bytes": 50, "number": 1, "message_id": "v1-a@h"}],
                                "decoded_seg_size": 50 }],
                    "slices": [] } } }
            ]
        })
        .to_string();

        let meta: NzbMeta = serde_json::from_str(&inline).expect("legacy row");
        assert_eq!(meta.files.len(), 2);
        let (NzbMetaSource::Rar { parts: a, .. }, NzbMetaSource::Rar { parts: b, .. }) =
            (&meta.files[0].source, &meta.files[1].source)
        else {
            panic!("expected two Rar files");
        };
        assert!(
            Arc::ptr_eq(a, b),
            "identical inline volumes must collapse to one allocation"
        );
        assert_eq!(a[0].filename, "v1");
        assert_eq!(meta.rar_sets.len(), 1);

        // And rewriting it produces the deduplicated form.
        let json = serde_json::to_string(&meta).expect("serialise");
        assert_eq!(json.matches("v1-a@h").count(), 1);
    }
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
}
