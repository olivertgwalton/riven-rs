use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream;

use crate::nntp::{NntpError, Priority};
use crate::nzb::NzbSegment;
use crate::state::{FetchEntry, PromiseSlot, StreamerState};
use crate::yenc;

use super::{
    NzbMetaSource, PREFETCH_FLOOR, StreamerError, UsenetStreamer, segments_overlapping,
};

/// Max attempts when fetching an NNTP segment body. ArticleNotFound is
/// permanent and never retried.
const NNTP_FETCH_ATTEMPTS: usize = 3;
/// Base backoff between retries (linear, not exponential — NNTP errors
/// are usually transient connectivity issues that clear within a second).
const NNTP_RETRY_DELAY_MS: u64 = 300;

impl UsenetStreamer {
    /// Fetch and yEnc-decode a segment's body. Routes through the LRU
    /// cache, retries transient errors with backoff, short-circuits on
    /// previously-observed permanent failures (`ArticleNotFound`), and
    /// deduplicates concurrent fetches of the same message-id — if the
    /// body stream and an eager prefetch both want the same segment,
    /// only one NNTP `BODY` round-trip happens and both observers share
    /// the result via a `Notify` promise.
    ///
    /// `priority` is passed through to the NNTP pool: streaming reads
    /// use `Priority::High`; background ingest uses `Priority::Low`.
    pub(crate) async fn fetch_decoded_cached(
        &self,
        message_id: &str,
        priority: Priority,
    ) -> Result<Bytes, StreamerError> {
        loop {
            if let Some(hit) = self.state.cache.get(message_id) {
                return Ok(hit);
            }
            if self.state.fails.is_dead(message_id) {
                return Err(StreamerError::Nntp(NntpError::ArticleNotFound(
                    "previously marked as missing".into(),
                )));
            }

            match self.state.in_flight.enter_or_wait(message_id) {
                FetchEntry::Wait(slot) => {
                    // Another task is fetching this segment. Park on the
                    // promise slot; when it's marked done, recheck the
                    // cache and the permanent-fail set on the next loop.
                    slot.wait().await;
                    continue;
                }
                FetchEntry::Owner(slot, mid) => {
                    // RAII guard: if this future is cancelled mid-fetch
                    // the explicit `finish` below would be skipped, which
                    // would leave the slot in the in_flight map with
                    // `done = false` and hang any future waiter for this
                    // message-id. The guard's Drop runs even on
                    // cancellation, ensuring the slot is always released.
                    // `mid` is the one shared `Arc<str>` key for this fetch.
                    struct OwnerGuard {
                        state: Arc<StreamerState>,
                        slot: Arc<PromiseSlot>,
                        message_id: Arc<str>,
                        finished: bool,
                    }
                    impl Drop for OwnerGuard {
                        fn drop(&mut self) {
                            if !self.finished {
                                tracing::debug!(
                                    message_id = %self.message_id,
                                    "owner future cancelled mid-fetch; releasing slot"
                                );
                                self.state.in_flight.finish(&self.message_id, &self.slot);
                            }
                        }
                    }
                    let mut guard = OwnerGuard {
                        state: self.state.clone(),
                        slot: slot.clone(),
                        message_id: mid.clone(),
                        finished: false,
                    };

                    let result = self.do_fetch_with_retry(message_id, priority).await;
                    // Cache must be populated BEFORE marking the slot done
                    // so waiters observe the hit on their next loop. Reuse the
                    // shared `Arc<str>` for both inserts — no extra allocation.
                    if let Ok(bytes) = &result {
                        let size = bytes.len() as u64;
                        self.state.cache.put(mid.clone(), bytes.clone());
                        self.state.decoded_sizes.put(mid.clone(), size);
                    }
                    self.state.in_flight.finish(&mid, &slot);
                    guard.finished = true;
                    return result;
                }
            }
        }
    }

    /// Inner retry loop. Side effects (cache.put, fails.mark_dead) are
    /// the caller's responsibility — keeps this fn purely about fetching.
    async fn do_fetch_with_retry(
        &self,
        message_id: &str,
        priority: Priority,
    ) -> Result<Bytes, StreamerError> {
        let mut last_err: Option<NntpError> = None;
        for attempt in 0..NNTP_FETCH_ATTEMPTS {
            tracing::debug!(attempt, message_id, "nntp fetch starting");
            let started = std::time::Instant::now();
            match self.pool.fetch_body(message_id, priority).await {
                Ok(body) => {
                    let wire_ms = started.elapsed().as_millis();
                    let encoded_len = body.len();
                    let decode_started = std::time::Instant::now();
                    let decoded = match tokio::task::spawn_blocking(move || yenc::decode(&body))
                        .await
                    {
                        Ok(Ok((decoded, _info))) => decoded,
                        Ok(Err(e)) => return Err(StreamerError::Yenc(e)),
                        Err(join_err) => {
                            tracing::warn!(message_id, error = %join_err, "yenc decode task panicked");
                            return Err(StreamerError::Nntp(NntpError::Protocol(
                                "yenc decode task panicked",
                            )));
                        }
                    };
                    let decode_ms = decode_started.elapsed().as_millis();
                    tracing::debug!(
                        attempt,
                        message_id,
                        encoded_len,
                        decoded_len = decoded.len(),
                        wire_ms,
                        decode_ms,
                        "nntp fetch ok"
                    );
                    return Ok(decoded);
                }
                Err(NntpError::ArticleNotFound(s)) => {
                    tracing::warn!(message_id, status = %s, "nntp article missing");
                    self.state.fails.mark_dead(message_id.to_string());
                    return Err(StreamerError::Nntp(NntpError::ArticleNotFound(s)));
                }
                Err(e) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    tracing::warn!(
                        attempt,
                        message_id,
                        error = %e,
                        elapsed_ms,
                        "nntp fetch failed; retrying"
                    );
                    last_err = Some(e);
                    if attempt + 1 < NNTP_FETCH_ATTEMPTS {
                        tokio::time::sleep(std::time::Duration::from_millis(NNTP_RETRY_DELAY_MS))
                            .await;
                    }
                }
            }
        }
        tracing::error!(message_id, "nntp fetch exhausted retries");
        Err(StreamerError::Nntp(last_err.unwrap_or(
            NntpError::Protocol("retry exhausted without error"),
        )))
    }

    /// Background-warm the segment cache for the segments that overlap
    /// `[start, end_inclusive]` of `file_index`. Concurrency is capped at
    /// `pool.download_concurrency()` — `min(pool_size, 15)` matching nzbdav —
    /// so a large pool (e.g. 100 connections) doesn't open 100 simultaneous
    /// BODY downloads. Fetches use `Priority::High` — this runs on behalf of
    /// a live stream.
    pub async fn prefetch_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) {
        let prefetch_concurrency = self.pool.download_concurrency().max(PREFETCH_FLOOR);

        let Ok(meta) = self.load_meta(info_hash).await else {
            return;
        };
        let Some(file) = meta.files.get(file_index) else {
            return;
        };
        if start > end_inclusive || end_inclusive >= file.total_size {
            return;
        }

        // Encoded offsets are within ~2% of decoded positions, fine for
        // cache-warming where over-fetching adjacent segments is cheap.
        let mids: Vec<String> = match &file.source {
            NzbMetaSource::Direct { offsets, segments } => {
                segments_overlapping(offsets, segments, start, end_inclusive)
            }
            NzbMetaSource::Rar { parts, slices } => {
                let mut out = Vec::new();
                let mut vpos: u64 = 0;
                for slice in slices {
                    let s0 = vpos;
                    let s1 = vpos + slice.length;
                    vpos = s1;
                    if s1 <= start {
                        continue;
                    }
                    if s0 > end_inclusive {
                        break;
                    }
                    let req_lo = start.max(s0) - s0;
                    let req_hi = end_inclusive.min(s1 - 1) - s0;
                    let part_lo = slice.start_in_part + req_lo;
                    let part_hi = slice.start_in_part + req_hi;
                    if let Some(part) = parts.get(slice.part_index) {
                        out.extend(segments_overlapping(
                            &part.offsets,
                            &part.segments,
                            part_lo,
                            part_hi,
                        ));
                    }
                }
                out
            }
        };

        let streamer = self.clone();
        let cold: Vec<String> = mids
            .into_iter()
            .filter(|mid| !streamer.state.cache.contains(mid))
            .collect();
        let mut stream = stream::iter(cold)
            .map(move |mid| {
                let s = streamer.clone();
                async move { s.fetch_decoded_cached(&mid, Priority::High).await }
            })
            .buffer_unordered(prefetch_concurrency);
        while stream.next().await.is_some() {}
    }

    /// Warm the segment cache for the head and tail of `file_index`.
    /// Players probe the start (container header, codec init) and end
    /// (MKV cues, fragmented MP4 moov) before sequential playback.
    /// Idempotent per `(info_hash, file_index)` per process.
    pub async fn precache_head_tail(&self, info_hash: &str, file_index: usize) {
        const PRECACHE_HEAD_BYTES: u64 = 4 * 1024 * 1024;
        const PRECACHE_TAIL_BYTES: u64 = 4 * 1024 * 1024;

        if !self.state.precached.claim(info_hash, file_index) {
            return;
        }
        // Bound concurrent precache pipelines so a library scan's burst of
        // HEAD requests doesn't spawn hundreds of simultaneous 8 MB
        // fetch+decode operations (a multi-GB allocation spike musl won't
        // return). The permit is held for the duration of the fetch.
        let Ok(_permit) = self.state.precache_sem.acquire().await else {
            return;
        };
        let Ok(meta) = self.load_meta(info_hash).await else {
            return;
        };
        let Some(file) = meta.files.get(file_index) else {
            return;
        };
        let total = file.total_size;
        if total == 0 {
            return;
        }

        let head_end = PRECACHE_HEAD_BYTES.saturating_sub(1).min(total - 1);
        let tail_start = total.saturating_sub(PRECACHE_TAIL_BYTES);
        let tail_end = total - 1;

        let started = std::time::Instant::now();
        let head = self.prefetch_range(info_hash, file_index, 0, head_end);
        if tail_start > head_end {
            let tail = self.prefetch_range(info_hash, file_index, tail_start, tail_end);
            tokio::join!(head, tail);
        } else {
            head.await;
        }
        tracing::info!(
            info_hash,
            file_index,
            elapsed_ms = started.elapsed().as_millis(),
            "usenet precache done"
        );
    }

    /// Read `[start, end_inclusive]` from `file_index`. Walks the meta's
    /// `source` to find the segments that overlap the request, decodes them,
    /// and returns a contiguous byte slice. Buffered (≤1 MB) HTTP responses
    /// and the RAR encrypted-slice decrypt path need a single contiguous
    /// buffer; the streaming body path should prefer `read_range_slices`
    /// to skip the outer `BytesMut` concatenation.
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
        // Never return more than the requested range. When a segment's actual
        // decoded length differs slightly from its rescaled offset slot (yEnc
        // per-segment variance), `slice_segment` falls back to the
        // proportional estimate, which can over-shoot by a few bytes; clamp so
        // a buffered HTTP response (which sets Content-Length from this) can't
        // exceed the requested `bytes=start-end` window.
        let want = (end_inclusive - start + 1) as usize;
        if buf.len() > want {
            buf.truncate(want);
        }
        Ok(buf)
    }

    /// Same as [`read_range`] but returns the per-segment decoded slices
    /// directly instead of concatenating them. The HTTP body stream
    /// emits each slice as its own response frame, avoiding the
    /// per-chunk `BytesMut` allocation + memcpy on segment-boundary
    /// chunks. Single-segment requests (the common 256 KB-inside-700 KB
    /// case) yield a one-element Vec; the slice is sliced out of the
    /// cached `Bytes` with zero copy.
    pub async fn read_range_slices(
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

        match &file.source {
            NzbMetaSource::Direct { offsets, segments } => {
                self.read_direct(offsets, segments, start, end_inclusive)
                    .await
            }
            NzbMetaSource::Rar { parts, slices } => {
                // RAR-contained sources route through a single contiguous
                // buffer because the encrypted-slice path needs in-place
                // AES-CBC decrypt. The boundary cost only matters for
                // Direct sources where 256 KB chunks straddle 720 KB
                // segments; RAR slices are typically a whole volume long
                // (100 MB+) so a body chunk almost always fits in one.
                let buf = self
                    .read_rar(
                        parts,
                        slices,
                        meta.password.as_deref(),
                        start,
                        end_inclusive,
                        Priority::High,
                    )
                    .await?;
                Ok(if buf.is_empty() { Vec::new() } else { vec![buf] })
            }
        }
    }

    /// Read a byte range from a `Direct` source: a single contiguous file
    /// composed of yEnc-encoded NNTP segments. Segments are fetched in
    /// parallel (capped at `pool.download_concurrency()`) and consumed in
    /// order — bounds NNTP round-trip latency for multi-segment reads.
    async fn read_direct(
        &self,
        offsets: &[u64],
        segments: &[NzbSegment],
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let (first, last) = direct_segment_span(offsets, segments.len(), start, end_inclusive);

        let read_concurrency = self.pool.download_concurrency().max(PREFETCH_FLOOR);
        let streamer = self.clone();
        // Index `segments` inside each future rather than cloning every
        // message-id into a temp Vec — the stream is consumed in place here,
        // so the borrow of `segments` outlives all in-flight fetches.
        let mut stream = stream::iter(first..=last)
            .map(move |i| {
                let s = streamer.clone();
                async move {
                    let mid = &segments[i].message_id;
                    (i, s.fetch_decoded_cached(mid, Priority::High).await)
                }
            })
            .buffered(read_concurrency);

        // Collect zero-copy `Bytes` slices. For the common single-segment
        // request (256 KB body chunk inside one ~700 KB segment), the
        // returned Vec has one slice the caller can hand straight to
        // hyper. Segment-boundary chunks return two slices; the body
        // stream emits both as separate response frames, no concat.
        let mut slices: Vec<Bytes> = Vec::new();
        while let Some((idx, result)) = stream.next().await {
            let decoded = result?;
            let slice = slice_segment(&decoded, offsets[idx], offsets[idx + 1], start, end_inclusive);
            if !slice.is_empty() {
                slices.push(slice);
            }
        }

        Ok(slices)
    }
}

/// Inclusive `[first, last]` segment indices whose cumulative byte ranges
/// overlap the request `[start, end]`. Single binary-search-based helper
/// shared by both the buffered (`read_direct`) and streaming
/// (`direct_byte_stream`) assembly paths, so they can never disagree about
/// which segments a range touches. `offsets` is sorted with length
/// `n_segments + 1`; `offsets[i]..offsets[i+1]` is segment `i`'s byte span.
fn direct_segment_span(offsets: &[u64], n_segments: usize, start: u64, end: u64) -> (usize, usize) {
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

/// Slice the requested byte window out of one decoded segment.
///
/// `seg_start`/`seg_end` are the segment's cumulative byte boundaries from
/// the meta's offset table. After `rescale_direct_to_decoded` these are
/// exact decoded offsets, so when the segment's actual decoded length
/// matches the offset delta we address bytes **exactly** — `decoded[(start
/// − seg_start) .. (end + 1 − seg_start)]` — the same model as altmount's
/// `SegmentData{start_offset,end_offset}`.
///
/// When the lengths disagree — a meta whose best-effort ingest rescale
/// failed and still holds encoded offsets, or a rare non-uniform yEnc
/// poster — we fall back to the proportional estimate so the read stays
/// in-bounds and roughly aligned rather than slicing at a wrong absolute
/// position.
fn slice_segment(decoded: &Bytes, seg_start: u64, seg_end: u64, start: u64, end_inclusive: u64) -> Bytes {
    let offset_len = seg_end.saturating_sub(seg_start);
    if offset_len == 0 {
        return Bytes::new();
    }
    let dec_len = decoded.len() as u64;
    let req_lo = start.max(seg_start) - seg_start;
    let req_hi = end_inclusive.min(seg_end - 1) - seg_start;

    let (lo, hi) = if dec_len == offset_len {
        // Exact: offsets are true decoded positions for this segment.
        (req_lo as usize, (req_hi + 1) as usize)
    } else {
        // Approximate: scale the request into the segment's actual decoded
        // span. Preserves the old behaviour for un-rescaled metas.
        let lo = (req_lo as u128 * dec_len as u128 / offset_len as u128) as usize;
        let hi = ((req_hi as u128 + 1) * dec_len as u128 / offset_len as u128) as usize;
        (lo, hi)
    };
    let hi = hi.min(decoded.len());
    let lo = lo.min(hi);
    if lo < hi {
        decoded.slice(lo..hi)
    } else {
        Bytes::new()
    }
}

/// Concatenate decoded segment slices into one contiguous `Bytes`. Used
/// by [`UsenetStreamer::read_range`] for callers that want a single
/// buffer (HTTP buffered responses, RAR encrypted-slice decrypt). The
/// streaming HTTP path uses the slice list directly and skips this.
fn concat_slices(mut slices: Vec<Bytes>, start: u64, end_inclusive: u64) -> Bytes {
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
    fn direct_segment_span_covers_request() {
        // 3 segments: [0,100), [100,250), [250,400).
        let offsets = [0u64, 100, 250, 400];
        assert_eq!(direct_segment_span(&offsets, 3, 0, 0), (0, 0));
        assert_eq!(direct_segment_span(&offsets, 3, 50, 99), (0, 0));
        // Spans the first boundary.
        assert_eq!(direct_segment_span(&offsets, 3, 50, 150), (0, 1));
        // Starts mid-segment-1, ends in segment-2.
        assert_eq!(direct_segment_span(&offsets, 3, 120, 300), (1, 2));
        // Exactly on a boundary start.
        assert_eq!(direct_segment_span(&offsets, 3, 100, 100), (1, 1));
        // Whole file.
        assert_eq!(direct_segment_span(&offsets, 3, 0, 399), (0, 2));
    }

    #[test]
    fn slice_segment_exact_when_lengths_match() {
        // Segment covers decoded bytes [1000, 1700); actual decoded len
        // matches the offset delta (700), so addressing is exact.
        let decoded = Bytes::from((0u8..255).cycle().take(700).collect::<Vec<u8>>());
        // Request [1200, 1299] → bytes [200..300) within the segment.
        let out = slice_segment(&decoded, 1000, 1700, 1200, 1299);
        assert_eq!(out.len(), 100);
        assert_eq!(out[..], decoded[200..300]);
    }

    #[test]
    fn slice_segment_clamps_request_to_segment_bounds() {
        let decoded = Bytes::from(vec![7u8; 700]);
        // Request spans beyond this segment; only [500..700) belongs here.
        let out = slice_segment(&decoded, 1000, 1700, 1500, 9999);
        assert_eq!(out.len(), 200);
    }

    #[test]
    fn slice_segment_falls_back_to_ratio_when_lengths_differ() {
        // Offset delta says 700 but the segment actually decoded to 350
        // (e.g. encoded offsets / non-uniform poster). The fallback scales
        // the request into the real decoded span and stays in-bounds.
        let decoded = Bytes::from(vec![1u8; 350]);
        let out = slice_segment(&decoded, 1000, 1700, 1000, 1699);
        assert!(out.len() <= 350);
        assert!(!out.is_empty());
    }
}
