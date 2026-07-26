use bytes::Bytes;
use futures::StreamExt;
use futures::stream;

use crate::nntp::NntpError;
use crate::nzb::NzbSegment;

use super::{NzbMetaSource, SEGMENT_FANOUT, StreamerError, UsenetStreamer, concat_slices};

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
    pub async fn read_range_slices(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<Bytes>, StreamerError> {
        // A player-facing read that stalls is a visible stutter; log where and
        // for how long so it is diagnosable from logs rather than averages.
        let started = std::time::Instant::now();
        let result = self
            .read_range_slices_inner(info_hash, file_index, start, end_inclusive)
            .await;
        let elapsed_ms = started.elapsed().as_millis();
        if elapsed_ms > 300 {
            tracing::warn!(
                info_hash,
                file = %self.cached_file_label(info_hash, file_index),
                start,
                len = end_inclusive.saturating_sub(start) + 1,
                elapsed_ms,
                ok = result.is_ok(),
                "slow playback read"
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

        let (first, last) = direct_segment_span(offsets, segments.len(), start, end_inclusive);
        let mut skip = start.saturating_sub(offsets[first]) as usize;

        let mut slices: Vec<Bytes> = Vec::new();
        let mut produced: usize = 0;
        let mut index = first;
        // Exactly the span the offset table says the request touches. Fetching
        // a speculative margin past it would start articles this read almost
        // never needs, and `buffered` starts them eagerly — the extras then get
        // cancelled mid-BODY, costing their connection.
        let mut horizon = last;

        loop {
            let mut batch = stream::iter(index..=horizon)
                .map(|i| self.fetch_article(&segments[i].message_id, i == 0))
                .buffered(SEGMENT_FANOUT);

            while let Some(decoded) = batch.next().await {
                let decoded = decoded?;
                if skip >= decoded.len() {
                    skip -= decoded.len();
                    continue;
                }
                let take = (want - produced).min(decoded.len() - skip);
                slices.push(decoded.slice(skip..skip + take));
                produced += take;
                skip = 0;
                if produced >= want {
                    return Ok(slices);
                }
            }

            if horizon + 1 >= segments.len() {
                return Ok(slices);
            }
            index = horizon + 1;
            horizon = (horizon + SEGMENT_FANOUT).min(segments.len() - 1);
        }
    }

    /// Fetch one article. The very first article of a release is asked of
    /// every provider at once: it gates playback start, and one provider
    /// missing it is the usual cause of a slow first frame.
    pub(super) async fn fetch_article(
        &self,
        message_id: &str,
        first_of_release: bool,
    ) -> Result<Bytes, StreamerError> {
        let result = if first_of_release {
            self.pool.fetch_segment_first(message_id).await
        } else {
            self.pool.fetch_segment(message_id).await
        };
        Ok(result?)
    }
}

/// Inclusive `[first, last]` segment indices whose cumulative byte ranges
/// overlap `[start, end]`. `offsets` is sorted with length `n_segments + 1`;
/// `offsets[i]..offsets[i+1]` is segment `i`'s span.
pub(super) fn direct_segment_span(
    offsets: &[u64],
    n_segments: usize,
    start: u64,
    end: u64,
) -> (usize, usize) {
    let last_idx = n_segments.saturating_sub(1);
    let first = offsets
        .partition_point(|&o| o <= start)
        .saturating_sub(1)
        .min(last_idx);
    let last = offsets
        .partition_point(|&o| o <= end)
        .saturating_sub(1)
        .min(last_idx);
    (first, last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_segment_span_covers_request() {
        let offsets = [0u64, 100, 250, 400];
        assert_eq!(direct_segment_span(&offsets, 3, 0, 0), (0, 0));
        assert_eq!(direct_segment_span(&offsets, 3, 50, 99), (0, 0));
        assert_eq!(direct_segment_span(&offsets, 3, 50, 150), (0, 1));
        assert_eq!(direct_segment_span(&offsets, 3, 120, 300), (1, 2));
        assert_eq!(direct_segment_span(&offsets, 3, 100, 100), (1, 1));
        assert_eq!(direct_segment_span(&offsets, 3, 0, 399), (0, 2));
    }
}
