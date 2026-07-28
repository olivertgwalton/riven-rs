use bytes::Bytes;

use crate::nntp::NntpError;
use crate::nzb::NzbSegment;

use super::{NzbMetaSource, StreamerError, UsenetStreamer, concat_slices};

impl UsenetStreamer {
    /// Read `[start, end_inclusive]` from `file_index` as a single contiguous
    /// buffer. Buffered HTTP responses and the RAR decrypt path need one
    /// buffer; the VFS should prefer [`read_range_slices`] and avoid the
    /// concatenation.
    pub async fn read_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Bytes, StreamerError> {
        let slices = self
            .read_range_slices(info_hash, file_index, start, end_inclusive)
            .await?;
        let mut buf = concat_slices(slices, start, end_inclusive);
        let want = (end_inclusive - start + 1) as usize;
        if buf.len() > want {
            buf.truncate(want);
        }
        Ok(buf)
    }

    /// Same as [`read_range`] but returns the per-segment decoded slices
    /// instead of concatenating them, so a single-segment read is served by
    /// slicing the cached `Bytes` with no copy.
    ///
    /// Slow origin fetches are logged at `debug`. Both demand and speculative
    /// calls come from the one unified VFS window; there is no nested Usenet
    /// scheduler.
    pub async fn read_range_slices(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let started = std::time::Instant::now();
        let result = self
            .read_range_slices_inner(info_hash, file_index, start, end_inclusive)
            .await;
        let elapsed_ms = started.elapsed().as_millis();
        if elapsed_ms > 300 {
            tracing::debug!(
                info_hash,
                file = %self.cached_file_label(info_hash, file_index),
                start,
                len = end_inclusive.saturating_sub(start) + 1,
                elapsed_ms,
                ok = result.is_ok(),
                "slow origin read"
            );
        }
        result
    }

    async fn read_range_slices_inner(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let meta = self.load_meta(info_hash).await?;
        let file = meta
            .files
            .get(file_index)
            .ok_or(StreamerError::BadFileIndex(file_index))?;
        if start > end_inclusive || end_inclusive >= file.total_size {
            return Err(StreamerError::BadRange);
        }

        let result = match &file.source {
            NzbMetaSource::Direct { offsets, segments } => {
                self.read_direct(offsets, segments, start, end_inclusive)
                    .await
            }
            NzbMetaSource::Rar { parts, slices } => self
                .read_rar(
                    parts,
                    slices,
                    meta.password.as_deref(),
                    start,
                    end_inclusive,
                )
                .await
                .map(|buf| {
                    if buf.is_empty() {
                        Vec::new()
                    } else {
                        vec![buf]
                    }
                }),
        };

        if let Err(StreamerError::Nntp(NntpError::ArticleNotFound(status))) = &result {
            crate::state::report_dead_segment(info_hash, file_index, &file.filename, status);
        }
        result
    }

    /// Read a byte range from a `Direct` source: one contiguous file composed
    /// of yEnc-encoded articles.
    ///
    /// Assembly is anchored at the segment whose offset-table slot contains
    /// `start`, then walks forward accumulating each segment's **actual
    /// decoded length** until the requested byte count is satisfied. The
    /// offset table only picks the anchor and the in-segment skip; it never
    /// sizes a slice. That is deliberate: the table is a cumulative-decoded
    /// map that may be slightly approximate, and sizing from it drops bytes
    /// whenever a segment decodes to a different length than its slot. A short
    /// return mid-file is catastrophic — the Linux kernel treats a short FUSE
    /// read as EOF and truncates the file's cached size — so the only
    /// legitimate short return is at the true end of the file.
    async fn read_direct(
        &self,
        offsets: &[u64],
        segments: &[NzbSegment],
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let want = (end_inclusive - start + 1) as usize;
        if want == 0 || segments.is_empty() {
            return Ok(Vec::new());
        }

        let anchor = direct_anchor_segment(offsets, segments.len(), start);
        let mut skip = start.saturating_sub(offsets[anchor]) as usize;

        let mut slices: Vec<Bytes> = Vec::new();
        let mut produced: usize = 0;

        // Walk forward from the anchor until the requested byte count is
        // satisfied or the file ends — the offset table says where to start,
        // never where to stop. Segments are walked in order: parallelism is
        // owned by the unified VFS window, and fanning out here would nest a
        // second scheduler underneath it.
        for segment in segments.iter().skip(anchor) {
            let decoded = self.fetch_article(&segment.message_id).await?;
            if skip >= decoded.len() {
                skip -= decoded.len();
                continue;
            }
            let take = (want - produced).min(decoded.len() - skip);
            slices.push(decoded.slice(skip..skip + take));
            produced += take;
            skip = 0;
            if produced >= want {
                break;
            }
        }
        Ok(slices)
    }

    /// Fetch one article through the shared segment path.
    pub(super) async fn fetch_article(&self, message_id: &str) -> Result<Bytes, StreamerError> {
        Ok(self.pool.fetch_segment(message_id).await?)
    }

    /// Start every article a range spans, without waiting for any of them.
    ///
    /// This is streamnzb's `ReadAheadSegment`: warm the shared segment cache so
    /// the walk that follows either hits it or joins a fetch already on the
    /// wire, instead of paying each article's round trip end to end. The pool's
    /// single-flight keys on message-id, so overlapping ranges coalesce and a
    /// warm start is never a duplicate fetch.
    pub(super) fn warm_articles<'a>(&self, message_ids: impl Iterator<Item = &'a str>) {
        for message_id in message_ids {
            let pool = self.pool.clone();
            let message_id = message_id.to_string();
            tokio::spawn(async move { drop(pool.fetch_segment(&message_id).await) });
        }
    }
}

/// Index of the segment whose cumulative byte range contains `start`.
/// `offsets` is sorted with length `n_segments + 1`; `offsets[i]..offsets[i+1]`
/// is segment `i`'s span.
pub(super) fn direct_anchor_segment(offsets: &[u64], n_segments: usize, start: u64) -> usize {
    offsets
        .partition_point(|&o| o <= start)
        .saturating_sub(1)
        .min(n_segments.saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_anchor_segment_locates_the_start() {
        let offsets = [0u64, 100, 250, 400];
        assert_eq!(direct_anchor_segment(&offsets, 3, 0), 0);
        assert_eq!(direct_anchor_segment(&offsets, 3, 50), 0);
        assert_eq!(direct_anchor_segment(&offsets, 3, 100), 1);
        assert_eq!(direct_anchor_segment(&offsets, 3, 120), 1);
        assert_eq!(direct_anchor_segment(&offsets, 3, 399), 2);
        assert_eq!(direct_anchor_segment(&offsets, 3, 10_000), 2);
    }
}
