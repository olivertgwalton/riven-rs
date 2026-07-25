use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream;

use crate::nntp::{NntpClient, NntpError};
use crate::nzb::NzbSegment;
use crate::state::{FetchEntry, PromiseSlot, StreamerState};
use crate::yenc;

use riven_core::local_source::ReadIntent;

use super::{NzbMetaSource, StreamerError, UsenetStreamer, concat_slices};

/// Max attempts when fetching an NNTP segment body. `ArticleNotFound` is
/// permanent and never retried.
const NNTP_FETCH_ATTEMPTS: usize = 2;
/// Base backoff between error retries (linear, not exponential — NNTP errors
/// are usually transient connectivity issues that clear within a second).
/// Skipped entirely after a timeout, which has already waited out its own
/// deadline.
const NNTP_RETRY_DELAY_MS: u64 = 300;

/// Wait this long for an article before racing a second copy of the same
/// request down another connection and taking whichever answers first.
///
/// This is the fix for the stall that made high-bitrate playback buffer, and
/// it is worth being precise about why, because the obvious diagnoses are all
/// wrong. Measured against Newshosting with 100 connections: article latency
/// is p50 224 ms, p99 700 ms — but the slowest of ~2400 took **4.5 s**, with
/// `queue_ms=0`, so it was neither pool contention nor a slow provider. A
/// single connection simply got starved of its share of bandwidth.
///
/// That rare outlier is catastrophic here because a read is only served once
/// the *whole* contiguous range is assembled: one 4.5 s article freezes the
/// reader, which stops the buffer draining, which stops read-ahead
/// dispatching. Traces show the entire pipeline going silent for 3.3 s behind
/// one article, then a burst of ~16 fetches the instant it lands.
///
/// Retrying serially cannot help — you would have to abandon the original,
/// having already paid for it. Racing a duplicate does: the tail is a
/// *property of one connection*, not of the article, so a second connection
/// almost always answers in the usual ~224 ms. Cost is bounded and tiny,
/// because the threshold sits far out in the tail: under 1% of articles are
/// ever hedged, and the loser is cancelled the moment the winner lands.
/// (Dean & Barroso's "The Tail at Scale" hedged request, applied to NNTP.)
const HEDGE_AFTER: std::time::Duration = std::time::Duration::from_millis(900);

impl UsenetStreamer {
    /// The pool client matching a read's urgency.
    ///
    /// The pool has always had a `Hot > Stream > Bulk` priority order, but
    /// the read path asked for `Hot` unconditionally — including for
    /// speculative read-ahead, which is most of the traffic. That put a
    /// blocked player read into one FIFO behind every read-ahead article
    /// already queued, so the priority order it was supposed to benefit from
    /// never applied to it. Routing read-ahead to `Stream` restores the
    /// distinction: read-ahead still outranks ingest and repair, but yields
    /// to the read a player is actually waiting on.
    fn lane_client(&self, intent: ReadIntent) -> NntpClient {
        match intent {
            ReadIntent::Demand => self.pool.playback_client(),
            ReadIntent::ReadAhead => self.pool.stream_client(),
        }
    }

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

            // Concurrency is bounded by the NNTP pool's slot actors (a fetch
            // can't run without a connection), so no process-wide gate is
            // needed here — only the single-flight dedup below, which keeps
            // N readers of one segment to one wire fetch.
            match self.state.in_flight.enter_or_wait(message_id) {
                FetchEntry::Wait(slot) => {
                    slot.wait().await;
                    continue;
                }
                FetchEntry::Owner(slot, mid) => {
                    // RAII guard: if this future is cancelled mid-fetch (a
                    // FUSE read aborted by the player), Drop still releases
                    // the in_flight slot so waiters for this message-id are
                    // never hung.
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

    /// One article fetch, with a hedge against a single starved connection.
    ///
    /// Issues the `BODY`, and if it has not answered within [`HEDGE_AFTER`],
    /// issues a second one and returns whichever wins. Dropping the loser
    /// cancels its job; the pool either skips it (not yet dispatched, the
    /// reply channel is closed) or lets the slot finish reading the body and
    /// discard it, so the connection is handed back clean either way.
    async fn fetch_body_hedged(
        client: &NntpClient,
        message_id: &str,
        file: &str,
    ) -> Result<crate::bufpool::PooledBuf, NntpError> {
        if !client.is_latency_sensitive() {
            return client.fetch_body(message_id).await;
        }
        let primary = client.fetch_body(message_id);
        tokio::pin!(primary);
        match tokio::time::timeout(HEDGE_AFTER, &mut primary).await {
            Ok(result) => result,
            Err(_) => {
                tracing::debug!(
                    message_id,
                    file,
                    hedge_after_ms = HEDGE_AFTER.as_millis(),
                    "nntp fetch is in the latency tail; racing a hedge"
                );
                let hedge = client.fetch_body(message_id);
                tokio::pin!(hedge);
                // Whichever answers first wins, *including* a failure: an
                // error from one connection is a real answer about the
                // article, and the outer retry loop owns what to do with it.
                tokio::select! {
                    result = &mut primary => result,
                    result = &mut hedge => result,
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
            match Self::fetch_body_hedged(client, message_id, file).await {
                Ok(body) => {
                    let wire_ms = started.elapsed().as_millis();
                    let encoded_len = body.len();
                    let decode_started = std::time::Instant::now();
                    let decoded =
                        match tokio::task::spawn_blocking(move || yenc::decode(&body)).await {
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
                    // A timeout already spent its whole deadline waiting, and
                    // the pool dropped that connection — the retry dials or
                    // pops a different one, so there is nothing to back off
                    // from. Sleeping would only extend a stall the player is
                    // already feeling.
                    let timed_out = matches!(e, NntpError::Timeout | NntpError::DeadlineExceeded);
                    tracing::warn!(
                        attempt,
                        message_id,
                        file,
                        error = %e,
                        elapsed_ms,
                        "nntp fetch failed; retrying"
                    );
                    last_err = Some(e);
                    if attempt + 1 < NNTP_FETCH_ATTEMPTS && !timed_out {
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
        intent: ReadIntent,
    ) -> Result<Bytes, StreamerError> {
        let slices = self
            .read_range_slices(info_hash, file_index, start, end_inclusive, intent)
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
        intent: ReadIntent,
    ) -> Result<Vec<Bytes>, StreamerError> {
        // Any player-facing read that stalls is a visible stutter; make it
        // observable with where and how long, so a stall is diagnosable from
        // logs instead of averages.
        let started = std::time::Instant::now();
        let result = self
            .read_range_slices_inner(info_hash, file_index, start, end_inclusive, intent)
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
        intent: ReadIntent,
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
                self.read_direct(
                    offsets,
                    segments,
                    start,
                    end_inclusive,
                    &file.filename,
                    intent,
                )
                .await
            }
            NzbMetaSource::Rar { parts, slices } => {
                let client = self.lane_client(intent);
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
        intent: ReadIntent,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let want = (end_inclusive - start + 1) as usize;
        if want == 0 || segments.is_empty() {
            return Ok(Vec::new());
        }

        // Fan out across the *whole* span at once.
        //
        // A read is only served when every article covering it has arrived,
        // so its latency is the slowest article's no matter how they are
        // scheduled. Fetching them a few at a time therefore cannot lower the
        // floor — it only adds rounds on top of it. Measured: an 8 MiB read
        // spans ~11 articles, whose slowest is ~415 ms at p50; four at a time
        // turned that into 801 ms. Real socket use stays bounded by the
        // pool's slot actors and, above them, the read-ahead admission that
        // decides how many reads are in flight at once.
        //
        // The cap keeps a pathological range (a repair job reading a whole
        // file) from queueing thousands of futures in one go.
        const MAX_READ_FANOUT: usize = 24;
        let (first, last) = direct_segment_span(offsets, segments.len(), start, end_inclusive);
        let mut skip = start.saturating_sub(offsets[first]) as usize;
        let client = self.lane_client(intent);

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
            let fanout = (batch_last - batch_start + 1).min(MAX_READ_FANOUT);
            let mut stream = stream::iter(batch_start..=batch_last)
                .map(move |i| {
                    let s = streamer.clone();
                    let client = batch_client.clone();
                    async move {
                        let mid = &segments[i].message_id;
                        s.fetch_decoded_cached(&client, mid, file).await
                    }
                })
                .buffered(fanout);

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
            batch_last = (batch_last + MAX_READ_FANOUT).min(segments.len() - 1);
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
