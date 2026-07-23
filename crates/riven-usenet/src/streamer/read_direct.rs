use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream;

use crate::nntp::{NntpClient, NntpError};
use crate::nzb::NzbSegment;
use crate::state::{FetchEntry, PromiseSlot, StreamerState};
use crate::yenc;

use super::{NzbMetaSource, StreamerError, UsenetStreamer, concat_slices, segments_overlapping};

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
    /// The workload-bound client owns dispatch policy for the whole reader or
    /// job; individual segments do not assign their own priority.
    /// `file` is the name of the file this segment belongs to. It exists only
    /// so the article-level logs below can say *what* is failing: a message-id
    /// is meaningless on its own, and by the time a fetch fails the caller
    /// that knew the filename is several frames up the stack.
    pub(crate) async fn fetch_decoded_cached(
        &self,
        client: &NntpClient,
        message_id: &str,
        file: &str,
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

            // Reserve the process-wide slot *before* registering an owner in
            // `in_flight`.  A fetch-only gate below the owner registration
            // limits BODY requests but still lets every FUSE read-ahead
            // window enqueue a distinct promise.  During a library scan that
            // became hundreds of queued segment owners (and their retained
            // futures) even though only a few were downloading.  Bounding
            // ownership makes this a true global backpressure point.
            let Ok(permit) = self.state.fetch_sem.acquire().await else {
                return Err(StreamerError::Nntp(NntpError::Protocol(
                    "usenet fetch semaphore closed",
                )));
            };

            // A queued caller may have waited long enough for this segment
            // to enter the cache, or for another caller to mark it missing.
            // Re-check after acquiring so it never creates needless work.
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
                    // Only owners consume the scarce slot.  A follower is
                    // already deduplicated and can wait without occupying a
                    // segment budget permit.
                    drop(permit);
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
                    struct OwnerGuard<'a> {
                        state: Arc<StreamerState>,
                        slot: Arc<PromiseSlot>,
                        message_id: Arc<str>,
                        file: &'a str,
                        finished: bool,
                    }
                    impl Drop for OwnerGuard<'_> {
                        fn drop(&mut self) {
                            if !self.finished {
                                tracing::debug!(
                                    message_id = %self.message_id,
                                    file = %self.file,
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
                        file,
                        finished: false,
                    };

                    let result = self.do_fetch_with_retry(client, message_id, file).await;
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
        client: &NntpClient,
        message_id: &str,
        file: &str,
    ) -> Result<Bytes, StreamerError> {
        let mut last_err: Option<NntpError> = None;
        for attempt in 0..NNTP_FETCH_ATTEMPTS {
            tracing::debug!(attempt, message_id, file, "nntp fetch starting");
            let started = std::time::Instant::now();
            match client.fetch_body(message_id).await {
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
                            tracing::warn!(
                                message_id,
                                file,
                                error = %join_err,
                                "yenc decode task panicked"
                            );
                            return Err(StreamerError::Nntp(NntpError::Protocol(
                                "yenc decode task panicked",
                            )));
                        }
                    };
                    let decode_ms = decode_started.elapsed().as_millis();
                    self.state.fetch_metrics.record_ok(decoded.len() as u64);
                    tracing::debug!(
                        attempt,
                        message_id,
                        file,
                        encoded_len,
                        decoded_len = decoded.len(),
                        wire_ms,
                        decode_ms,
                        "nntp fetch ok"
                    );
                    return Ok(decoded);
                }
                Err(NntpError::ArticleNotFound(s)) => {
                    tracing::warn!(message_id, file, status = %s, "nntp article missing");
                    self.state.fails.mark_dead(message_id.to_string());
                    self.state.fetch_metrics.record_failed();
                    return Err(StreamerError::Nntp(NntpError::ArticleNotFound(s)));
                }
                Err(e) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    tracing::warn!(
                        attempt,
                        message_id,
                        file,
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
        tracing::error!(message_id, file, "nntp fetch exhausted retries");
        self.state.fetch_metrics.record_failed();
        Err(StreamerError::Nntp(last_err.unwrap_or(
            NntpError::Protocol("retry exhausted without error"),
        )))
    }

    /// Background-warm the segment cache for the segments that overlap
    /// `[start, end_inclusive]` of `file_index`. Per-operation fan-out is
    /// bounded by `RIVEN_USENET_MAX_PREFETCH`, and the actual BODY/decode work
    /// is further gated by the process-wide fetch semaphore.
    pub async fn prefetch_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) {
        let Some(_admission) = self.admit_prefetch(info_hash, file_index) else {
            return;
        };
        let Ok(meta) = self.load_meta(info_hash).await else {
            return;
        };
        let Some(file) = meta.files.get(file_index) else {
            return;
        };
        if start > end_inclusive || end_inclusive >= file.total_size {
            return;
        }

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

        let client = self.pool.bulk_client();
        self.prefetch_mids(client, mids, &file.filename).await;
    }

    /// Fill a playback reader's ordered segment window beginning at `start`.
    /// The window is expressed in NZB transfer units rather than guessed
    /// bytes, so high-bitrate files naturally maintain the same number of
    /// completed articles ahead of the consumer.
    pub async fn prefetch_window(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        max_segments: usize,
    ) {
        if max_segments == 0 {
            return;
        }
        let Some(_admission) = self.admit_prefetch(info_hash, file_index) else {
            return;
        };
        let Ok(meta) = self.load_meta(info_hash).await else {
            return;
        };
        let Some(file) = meta.files.get(file_index) else {
            return;
        };
        if start >= file.total_size {
            return;
        }

        let mids = match &file.source {
            NzbMetaSource::Direct { offsets, segments } => {
                let first = offsets
                    .partition_point(|&offset| offset <= start)
                    .saturating_sub(1)
                    .min(segments.len().saturating_sub(1));
                segments
                    .iter()
                    .skip(first)
                    .take(max_segments)
                    .map(|segment| segment.message_id.clone())
                    .collect()
            }
            NzbMetaSource::Rar { parts, slices } => {
                let mut mids = Vec::with_capacity(max_segments);
                let mut virtual_pos = 0u64;
                for slice in slices {
                    let slice_end = virtual_pos.saturating_add(slice.length);
                    if slice_end <= start {
                        virtual_pos = slice_end;
                        continue;
                    }
                    let offset_in_slice = start.saturating_sub(virtual_pos);
                    let offset_in_part = slice.start_in_part.saturating_add(offset_in_slice);
                    if let Some(part) = parts.get(slice.part_index) {
                        let first = part
                            .decoded_seg_size
                            .filter(|size| *size > 0)
                            .map_or_else(
                                || {
                                    part.offsets
                                        .partition_point(|&offset| offset <= offset_in_part)
                                        .saturating_sub(1)
                                },
                                |size| (offset_in_part / size) as usize,
                            )
                            .min(part.segments.len().saturating_sub(1));
                        for segment in part.segments.iter().skip(first) {
                            if mids.len() >= max_segments {
                                break;
                            }
                            if !mids.iter().any(|mid| mid == &segment.message_id) {
                                mids.push(segment.message_id.clone());
                            }
                        }
                    }
                    if mids.len() >= max_segments {
                        break;
                    }
                    virtual_pos = slice_end;
                }
                mids
            }
        };

        let client = self.pool.playback_client();
        self.prefetch_mids(client, mids, &file.filename).await;
    }

    /// Global admission control for read-ahead. Read-ahead is *optional* work,
    /// so a window that can't get a permit is dropped rather than queued: a
    /// library scan opening a whole season pack at once starts one prefetch
    /// worker per episode, and letting them all queue means their combined
    /// windows still get scheduled — thousands of articles for eight real
    /// streams, which is exactly the burst that drove the process into OOM.
    /// A genuinely playing stream re-requests its window on the next read, so
    /// it recovers immediately; a scanner touch does not, which is the point.
    fn admit_prefetch(
        &self,
        info_hash: &str,
        file_index: usize,
    ) -> Option<tokio::sync::SemaphorePermit<'_>> {
        match self.state.prefetch_sem.try_acquire() {
            Ok(permit) => Some(permit),
            Err(_) => {
                tracing::debug!(
                    info_hash,
                    file = %self.cached_file_label(info_hash, file_index),
                    file_index,
                    "usenet prefetch window shed: global read-ahead budget full"
                );
                None
            }
        }
    }

    /// `file` names the file these segments belong to, purely so a read-ahead
    /// fetch failure logs like a playback one. Shared as an `Arc<str>` because
    /// every segment in the window carries it into its own future.
    async fn prefetch_mids(&self, client: NntpClient, mids: Vec<String>, file: &str) {
        let prefetch_concurrency = self.prefetch_concurrency(client.capacity());
        let streamer = self.clone();
        let file: Arc<str> = Arc::from(file);
        let cold: Vec<String> = mids
            .into_iter()
            .filter(|mid| !streamer.state.cache.contains(mid))
            .collect();
        let mut stream = stream::iter(cold)
            .map(move |mid| {
                let s = streamer.clone();
                let client = client.clone();
                let file = file.clone();
                async move {
                    // Read-ahead may only ever hold part of the global segment
                    // gate, so a read a player is actually blocked on never
                    // waits behind speculative work.
                    let _bg = s.state.background_sem.acquire().await;
                    s.fetch_decoded_cached(&client, &mid, &file).await
                }
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
            file = %file.filename,
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

        let result = match &file.source {
            NzbMetaSource::Direct { offsets, segments } => {
                self.read_direct(offsets, segments, start, end_inclusive, &file.filename)
                    .await
            }
            NzbMetaSource::Rar { parts, slices } => {
                let client = self.pool.playback_client();
                self.read_rar(
                    parts,
                    slices,
                    meta.password.as_deref(),
                    start,
                    end_inclusive,
                    &client,
                    &file.filename,
                )
                .await
                .map(|buf| {
                    if buf.is_empty() {
                        Vec::new()
                    } else {
                        vec![buf]
                    }
                })
            }
        };

        if let Err(StreamerError::Nntp(NntpError::ArticleNotFound(status))) = &result {
            crate::state::report_dead_segment(info_hash, file_index, &file.filename, status);
        }
        result
    }

    /// Read a byte range from a `Direct` source: a single contiguous file
    /// composed of yEnc-encoded NNTP segments. Segments are fetched in
    /// parallel (capped at `pool.download_concurrency()`) and consumed in
    /// order — bounds NNTP round-trip latency for multi-segment reads.
    ///
    /// Assembly is anchored at the segment whose offset-table slot contains
    /// `start`, then walks forward accumulating each segment's **actual
    /// decoded length** until the requested byte count is satisfied. The
    /// offset table is used only to pick the starting segment and the
    /// in-segment skip — never to size the per-segment slice. This is
    /// deliberate: the table is a cumulative-decoded map that may be slightly
    /// approximate (e.g. metas ingested before exact-offset rescaling), and
    /// sizing slices from it drops or short-changes bytes whenever a segment
    /// decodes to a different length than its slot. A short return is
    /// catastrophic for the FUSE layer — the Linux kernel treats a read that
    /// returns fewer bytes than requested as EOF and truncates the file's
    /// cached size — so we always return exactly `[start, end]` worth of
    /// bytes (small boundary slop from an approximate anchor is tolerated by
    /// players; dropping bytes is not). The only legitimate short return is
    /// at the true end of the file, where we run out of segments.
    async fn read_direct(
        &self,
        offsets: &[u64],
        segments: &[NzbSegment],
        start: u64,
        end_inclusive: u64,
        file: &str,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let want = (end_inclusive - start + 1) as usize;
        if want == 0 || segments.is_empty() {
            return Ok(Vec::new());
        }

        let (first, last) = direct_segment_span(offsets, segments.len(), start, end_inclusive);
        let mut skip = start.saturating_sub(offsets[first]) as usize;
        let client = self.pool.playback_client();
        let read_concurrency = self.prefetch_concurrency(client.capacity());

        let mut slices: Vec<Bytes> = Vec::new();
        let mut produced: usize = 0;

        // Fetch in **bounded, fully-drained batches**. The offset-table span
        // `[first, last]` covers the request; a small margin absorbs ordinary
        // per-segment decode/offset slop so one batch almost always suffices.
        // If slop still leaves us short, we fetch the next batch — never an
        // unbounded stream with an early break. Draining every batch in full is
        // essential: cancelling an in-flight fetch (by dropping a `buffered`
        // stream mid-flight) leaves the pooled NNTP connection with a half-read
        // BODY response, which makes the next user of that socket time out and
        // cascades into the provider's circuit breaker.
        let mut batch_start = first;
        let mut batch_last = (last + 2).min(segments.len() - 1);
        loop {
            let streamer = self.clone();
            let batch_client = client.clone();
            let mut stream = stream::iter(batch_start..=batch_last)
                .map(move |i| {
                    let s = streamer.clone();
                    let client = batch_client.clone();
                    async move {
                        let mid = &segments[i].message_id;
                        s.fetch_decoded_cached(&client, mid, file).await
                    }
                })
                .buffered(read_concurrency);

            while let Some(result) = stream.next().await {
                let decoded = result?;
                if produced >= want {
                    continue;
                }
                if skip >= decoded.len() {
                    skip -= decoded.len();
                    continue;
                }
                let take = (want - produced).min(decoded.len() - skip);
                slices.push(decoded.slice(skip..skip + take));
                produced += take;
                skip = 0;
            }

            if produced >= want || batch_last + 1 >= segments.len() {
                break;
            }
            batch_start = batch_last + 1;
            batch_last = (batch_last + read_concurrency).min(segments.len() - 1);
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
