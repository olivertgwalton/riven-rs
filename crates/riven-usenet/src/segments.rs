//! A file's segment list, stored packed rather than as a `Vec` of structs.
//!
//! The shape this replaces was `Vec<NzbSegment>` — 8 bytes of size, 4 of a
//! `number` nothing read after sorting, and a `String` — plus a separate heap
//! allocation per message-id, rounded up to an allocator size class. Message
//! ids in this library measure 32 and 41 characters, so that came to 72–88
//! bytes per segment and one allocation each, at ~146 000 segments for a
//! 100 GB title.
//!
//! Here the ids live end to end in one buffer and the per-segment data is two
//! `u32` columns, so a segment costs 8 bytes plus its own characters — 40–49 —
//! and a whole file costs *three* allocations rather than one per segment.
//! That is ~44 % less, and the allocation count matters as much as the bytes:
//! it is what deserialising a file's worth of segments spends most of its time
//! on, and what leaves the allocator fragmented afterwards.
//!
//! # The stored format does not change
//!
//! [`Serialize`] emits exactly what the old `Vec<NzbSegment>` emitted — a JSON
//! array of `{"bytes":…,"message_id":…}` — and [`Deserialize`] reads that same
//! shape, ignoring the `"number"` key older rows carry. This library holds
//! 4 489 MB of stored metadata across 2 510 rows; none of it needs rewriting,
//! and a rollback to the previous binary reads everything this writes.

use std::fmt;

use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// One segment, by value. The form callers build and tests write; a
/// [`SegmentList`] is collected from these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NzbSegment {
    /// Per-NZB-spec, the encoded article size in bytes (yEnc payload plus a few
    /// bytes of header overhead). Decoded size is ~2 % smaller; it stands in as
    /// an offset proxy until a segment has actually been fetched.
    pub bytes: u64,
    /// Article message-id, without surrounding `<>`.
    pub message_id: String,
}

/// One segment, borrowed from a [`SegmentList`].
///
/// The field names match [`NzbSegment`] deliberately: every `|s| s.message_id`
/// and `|s| s.bytes` closure at a call site goes on working when the collection
/// underneath it changes shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentRef<'a> {
    pub bytes: u64,
    pub message_id: &'a str,
}

/// A file's segments, packed. See the module docs for the layout and why.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SegmentList {
    /// Every message-id concatenated, no separators.
    ids: Box<str>,
    /// End offset of each id within `ids`. The start of `i` is `ends[i - 1]`,
    /// or 0 for the first — one column rather than the `len + 1` offset table
    /// the obvious encoding would use.
    ends: Box<[u32]>,
    /// Encoded size per segment. `u32` because an NZB segment is an article:
    /// the largest seen in practice is 3.84 MB and the format has no way to
    /// express a useful one anywhere near 4 GB. Saturating rather than
    /// wrapping, so a malformed NZB cannot silently produce a tiny size.
    sizes: Box<[u32]>,
}

impl SegmentList {
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// Message-id of segment `index`, borrowed from the shared buffer.
    pub fn id(&self, index: usize) -> Option<&str> {
        let end = *self.ends.get(index)? as usize;
        let start = if index == 0 {
            0
        } else {
            self.ends[index - 1] as usize
        };
        self.ids.get(start..end)
    }

    pub fn get(&self, index: usize) -> Option<SegmentRef<'_>> {
        Some(SegmentRef {
            bytes: u64::from(*self.sizes.get(index)?),
            message_id: self.id(index)?,
        })
    }

    pub fn first(&self) -> Option<SegmentRef<'_>> {
        self.get(0)
    }

    pub fn iter(&self) -> SegmentIter<'_> {
        SegmentIter {
            list: self,
            index: 0,
        }
    }

    /// Segments `first..=last`, clamped to what exists. Replaces slicing, which
    /// a packed list cannot hand out as a contiguous `&[T]`.
    pub fn range(&self, first: usize, last: usize) -> impl Iterator<Item = SegmentRef<'_>> + '_ {
        let last = last.min(self.len().saturating_sub(1));
        let count = (last + 1).saturating_sub(first);
        (0..count).filter_map(move |offset| self.get(first + offset))
    }

    /// Bytes this list occupies on the heap: three allocations, whatever their
    /// contents. Used by the meta caches to weigh an entry, and by the test
    /// that pins the saving over the shape this replaced.
    pub fn heap_bytes(&self) -> usize {
        self.ids.len() + self.ends.len() * 4 + self.sizes.len() * 4
    }

    /// Total encoded bytes across every segment.
    pub fn total_bytes(&self) -> u64 {
        self.sizes.iter().copied().map(u64::from).sum()
    }
}

/// Accumulates segments into the packed layout without an intermediate `Vec`
/// of structs, so deserialising never materialises the shape being avoided.
#[derive(Default)]
pub struct SegmentListBuilder {
    ids: String,
    ends: Vec<u32>,
    sizes: Vec<u32>,
}

impl SegmentListBuilder {
    pub fn with_capacity(segments: usize, id_bytes: usize) -> Self {
        Self {
            ids: String::with_capacity(id_bytes),
            ends: Vec::with_capacity(segments),
            sizes: Vec::with_capacity(segments),
        }
    }

    pub fn push(&mut self, message_id: &str, bytes: u64) {
        self.ids.push_str(message_id);
        // Ids are message-ids; a file whose ids exceed 4 GiB in total is not a
        // thing, but truncating the offset would corrupt every later id, so
        // saturate and let the tail read as empty rather than as garbage.
        self.ends
            .push(u32::try_from(self.ids.len()).unwrap_or(u32::MAX));
        self.sizes.push(u32::try_from(bytes).unwrap_or(u32::MAX));
    }

    pub fn build(self) -> SegmentList {
        SegmentList {
            ids: self.ids.into_boxed_str(),
            ends: self.ends.into_boxed_slice(),
            sizes: self.sizes.into_boxed_slice(),
        }
    }
}

impl FromIterator<NzbSegment> for SegmentList {
    fn from_iter<I: IntoIterator<Item = NzbSegment>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut builder = SegmentListBuilder::with_capacity(lower, lower * 48);
        for segment in iter {
            builder.push(&segment.message_id, segment.bytes);
        }
        builder.build()
    }
}

impl<'a> FromIterator<SegmentRef<'a>> for SegmentList {
    fn from_iter<I: IntoIterator<Item = SegmentRef<'a>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut builder = SegmentListBuilder::with_capacity(lower, lower * 48);
        for segment in iter {
            builder.push(segment.message_id, segment.bytes);
        }
        builder.build()
    }
}

/// A concrete iterator rather than a boxed closure: these are held across
/// `await` points in the read path, and a `Box<dyn Fn>` is not `Send`.
#[derive(Clone)]
pub struct SegmentIter<'a> {
    list: &'a SegmentList,
    index: usize,
}

impl<'a> Iterator for SegmentIter<'a> {
    type Item = SegmentRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.list.get(self.index)?;
        self.index += 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.list.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }

    /// O(1). Without this the default walks `next()` `n` times, and
    /// `.skip(anchor)` is exactly what the read path does to reach the segment
    /// a range starts at — around 100 000 of them for a read late in a large
    /// file, against the `Vec` this replaced where slice iteration made the
    /// same skip free. `Skip::next` calls `nth` for its first element, so this
    /// is what makes the seek constant-time again.
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.index = self.index.saturating_add(n);
        self.next()
    }

    fn count(self) -> usize {
        self.list.len().saturating_sub(self.index)
    }

    fn last(self) -> Option<Self::Item> {
        self.list.get(self.list.len().checked_sub(1)?)
    }
}

impl ExactSizeIterator for SegmentIter<'_> {}

impl<'a> IntoIterator for &'a SegmentList {
    type Item = SegmentRef<'a>;
    type IntoIter = SegmentIter<'a>;

    /// So `for s in &segments` keeps working where it did before.
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl fmt::Debug for SegmentList {
    /// Prints as a list, so a `{:?}` of an `NzbMetaFile` reads the way it did
    /// before — but bounded, because these run to six figures.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const SHOWN: usize = 4;
        let mut list = f.debug_list();
        list.entries(self.iter().take(SHOWN));
        if self.len() > SHOWN {
            list.entry(&format_args!("… {} more", self.len() - SHOWN));
        }
        list.finish()
    }
}

// ── Wire format ──────────────────────────────────────────────────────────────
// Identical to the `Vec<NzbSegment>` this replaced, so stored rows keep working
// and a rollback reads what this writes.

impl Serialize for SegmentList {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for segment in self.iter() {
            seq.serialize_element(&WireSegment {
                bytes: segment.bytes,
                message_id: segment.message_id,
            })?;
        }
        seq.end()
    }
}

#[derive(Serialize)]
struct WireSegment<'a> {
    bytes: u64,
    message_id: &'a str,
}

/// Borrowed where the JSON allows it — a message-id with no escapes is a slice
/// of the input rather than a fresh allocation, which is the common case.
#[derive(Deserialize)]
struct StoredSegment<'a> {
    #[serde(default)]
    bytes: u64,
    #[serde(borrow)]
    message_id: std::borrow::Cow<'a, str>,
}

impl<'de> Deserialize<'de> for SegmentList {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ListVisitor;

        impl<'de> Visitor<'de> for ListVisitor {
            type Value = SegmentList;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an array of NZB segments")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let hint = seq.size_hint().unwrap_or(0);
                let mut builder = SegmentListBuilder::with_capacity(hint, hint * 48);
                while let Some(segment) = seq.next_element::<StoredSegment<'_>>()? {
                    builder.push(&segment.message_id, segment.bytes);
                }
                Ok(builder.build())
            }
        }

        deserializer.deserialize_seq(ListVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> SegmentList {
        [
            NzbSegment {
                bytes: 700_000,
                message_id: "a@host".into(),
            },
            NzbSegment {
                bytes: 700_001,
                message_id: "bb@host".into(),
            },
            NzbSegment {
                bytes: 12,
                message_id: "ccc@host".into(),
            },
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn indexing_and_iteration_agree() {
        let list = sample();
        assert_eq!(list.len(), 3);
        assert!(!list.is_empty());
        assert_eq!(list.first().unwrap().message_id, "a@host");
        assert_eq!(list.id(1), Some("bb@host"));
        assert_eq!(list.get(2).unwrap().bytes, 12);
        assert_eq!(list.get(3), None);
        assert_eq!(list.id(3), None);

        let collected: Vec<&str> = list.iter().map(|s| s.message_id).collect();
        assert_eq!(collected, ["a@host", "bb@host", "ccc@host"]);
        assert_eq!(list.total_bytes(), 700_000 + 700_001 + 12);
    }

    #[test]
    fn range_clamps_instead_of_panicking() {
        let list = sample();
        let ids: Vec<&str> = list.range(1, 2).map(|s| s.message_id).collect();
        assert_eq!(ids, ["bb@host", "ccc@host"]);
        // Past the end: clamped, not a panic — a read must not take the
        // process down because an offset table over-estimated.
        let ids: Vec<&str> = list.range(1, 99).map(|s| s.message_id).collect();
        assert_eq!(ids, ["bb@host", "ccc@host"]);
        assert_eq!(list.range(3, 9).count(), 0);
    }

    /// The whole point: 4 489 MB of stored metadata must stay readable, and a
    /// rollback must be able to read anything this writes.
    #[test]
    fn the_stored_shape_is_unchanged() {
        let stored = r#"[
            {"bytes":700000,"number":1,"message_id":"a@host"},
            {"bytes":700001,"number":2,"message_id":"bb@host"},
            {"bytes":12,"number":3,"message_id":"ccc@host"}
        ]"#;
        let list: SegmentList = serde_json::from_str(stored).expect("legacy rows");
        assert_eq!(list, sample());

        // What we emit is what the previous `Vec<NzbSegment>` emitted, minus
        // the `number` that nothing read.
        let written = serde_json::to_string(&list).expect("serialise");
        assert_eq!(
            written,
            r#"[{"bytes":700000,"message_id":"a@host"},{"bytes":700001,"message_id":"bb@host"},{"bytes":12,"message_id":"ccc@host"}]"#
        );
        let back: SegmentList = serde_json::from_str(&written).expect("round trip");
        assert_eq!(back, sample());
    }

    /// Lifted verbatim from this library's `usenet_meta` table, so the shape
    /// being parsed is the shape actually on disk rather than one written to
    /// match the parser. Note the `number` key every stored segment carries.
    #[test]
    fn a_real_stored_row_parses() {
        let stored = r#"[
            {"bytes": 768000, "number": 1, "message_id": "MAOJbi-N3fMQcKs7ETvFp@Kg64ar.4Uw"},
            {"bytes": 768000, "number": 2, "message_id": "cQ36wtlwwKqRbkbbBRh06w0D@PLl1r5n.s31"},
            {"bytes": 768000, "number": 3, "message_id": "v-P7ZIUwPnpD7JMwtGAYxE0c@_fsK8axi.fIE"}
        ]"#;
        let list: SegmentList = serde_json::from_str(stored).expect("real row");
        assert_eq!(list.len(), 3);
        assert_eq!(list.id(0), Some("MAOJbi-N3fMQcKs7ETvFp@Kg64ar.4Uw"));
        assert_eq!(list.id(2), Some("v-P7ZIUwPnpD7JMwtGAYxE0c@_fsK8axi.fIE"));
        assert!(list.iter().all(|s| s.bytes == 768_000));
        assert_eq!(list.total_bytes(), 3 * 768_000);

        // Ids are packed end to end; each must still come back with its own
        // boundaries, which is the failure mode a packed layout invites.
        let ids: Vec<&str> = list.iter().map(|s| s.message_id).collect();
        assert_eq!(
            ids.concat().len(),
            ids.iter().map(|i| i.len()).sum::<usize>()
        );
        for id in &ids {
            assert!(id.contains('@'), "id lost its boundaries: {id}");
        }
    }

    /// The reason this type exists, measured rather than asserted from the
    /// design. Sized on a real id length (32 chars, from the row above) at the
    /// segment count of a 100 GB title.
    #[test]
    fn the_packed_layout_is_smaller_than_a_vec_of_structs() {
        const SEGMENTS: usize = 146_000;
        const ID_LEN: usize = 32;

        let list: SegmentList = (0..SEGMENTS)
            .map(|i| NzbSegment {
                bytes: 768_000,
                message_id: format!("{i:0width$}", width = ID_LEN),
            })
            .collect();
        assert_eq!(list.len(), SEGMENTS);

        let packed = list.heap_bytes();
        // What the previous shape cost: 24 bytes of struct in the Vec, plus a
        // separate allocation per id that the allocator rounds to a size class
        // (48 for a 32-byte string once the header is counted).
        let previous = SEGMENTS * (24 + 48);
        assert!(
            packed < previous * 3 / 5,
            "expected under 60% of {previous}, got {packed}"
        );
    }

    /// `.skip(n)` must land on the right segment and must not walk there.
    /// Correctness is what is asserted; the cost is why `nth` exists.
    #[test]
    fn skipping_lands_on_the_right_segment() {
        let list = sample();
        assert_eq!(list.iter().nth(1).map(|s| s.message_id), Some("bb@host"));
        assert_eq!(list.iter().nth(2).map(|s| s.message_id), Some("ccc@host"));
        assert_eq!(list.iter().nth(3), None);

        let skipped: Vec<&str> = list.iter().skip(1).map(|s| s.message_id).collect();
        assert_eq!(skipped, ["bb@host", "ccc@host"]);

        // `enumerate().skip(n)` is the read path's shape: the index must still
        // be the absolute segment number, not the position after skipping.
        let pairs: Vec<(usize, &str)> = list
            .iter()
            .enumerate()
            .skip(1)
            .map(|(i, s)| (i, s.message_id))
            .collect();
        assert_eq!(pairs, [(1, "bb@host"), (2, "ccc@host")]);

        assert_eq!(list.iter().count(), 3);
        assert_eq!(list.iter().skip(1).count(), 2);
        assert_eq!(list.iter().last().map(|s| s.message_id), Some("ccc@host"));
    }

    #[test]
    fn an_empty_list_round_trips() {
        let empty = SegmentList::default();
        assert!(empty.is_empty());
        assert_eq!(empty.first(), None);
        let written = serde_json::to_string(&empty).expect("serialise");
        assert_eq!(written, "[]");
        let back: SegmentList = serde_json::from_str(&written).expect("round trip");
        assert_eq!(back, empty);
    }

    /// Escaped ids cannot be borrowed from the input, so the `Cow` has to own —
    /// the path that would be missed by testing only clean ids.
    #[test]
    fn an_escaped_message_id_survives() {
        let stored = r#"[{"bytes":1,"message_id":"we\"ird@host"}]"#;
        let list: SegmentList = serde_json::from_str(stored).expect("escaped id");
        assert_eq!(list.id(0), Some("we\"ird@host"));
    }
}
