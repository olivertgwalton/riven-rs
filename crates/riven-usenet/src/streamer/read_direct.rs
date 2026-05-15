use std::sync::Arc;

use futures::StreamExt;
use futures::stream;

use crate::nntp::NntpError;
use crate::nzb::NzbSegment;
use crate::state::{FetchEntry, PromiseSlot, StreamerState};
use crate::yenc;

use super::{
    NzbMetaSource, READ_PREFETCH_WINDOW, StreamerError, UsenetStreamer, segments_overlapping,
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
    pub(crate) async fn fetch_decoded_cached(
        &self,
        message_id: &str,
    ) -> Result<Arc<Vec<u8>>, StreamerError> {
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
                FetchEntry::Owner(slot) => {
                    // RAII guard: if this future is cancelled mid-fetch
                    // the explicit `finish` below would be skipped, which
                    // would leave the slot in the in_flight map with
                    // `done = false` and hang any future waiter for this
                    // message-id. The guard's Drop runs even on
                    // cancellation, ensuring the slot is always released.
                    struct OwnerGuard {
                        state: Arc<StreamerState>,
                        slot: Arc<PromiseSlot>,
                        message_id: String,
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
                        message_id: message_id.to_string(),
                        finished: false,
                    };

                    let result = self.do_fetch_with_retry(message_id).await;
                    // Cache must be populated BEFORE marking the slot done
                    // so waiters observe the hit on their next loop.
                    if let Ok(arc) = &result {
                        let size = arc.len() as u64;
                        self.state.cache.put(message_id.to_string(), arc.clone());
                        self.state.decoded_sizes.put(message_id.to_string(), size);
                    }
                    self.state.in_flight.finish(message_id, &slot);
                    guard.finished = true;
                    return result;
                }
            }
        }
    }

    /// Inner retry loop. Side effects (cache.put, fails.mark_dead) are
    /// the caller's responsibility — keeps this fn purely about fetching.
    async fn do_fetch_with_retry(&self, message_id: &str) -> Result<Arc<Vec<u8>>, StreamerError> {
        let mut last_err: Option<NntpError> = None;
        for attempt in 0..NNTP_FETCH_ATTEMPTS {
            tracing::debug!(attempt, message_id, "nntp fetch starting");
            let started = std::time::Instant::now();
            match self.pool.fetch_body(message_id).await {
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
                    return Ok(Arc::new(decoded));
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
    /// `[start, end_inclusive]` of `file_index`. Concurrency is sized to
    /// match `READ_PREFETCH_WINDOW` so the prefetch can saturate the
    /// NNTP pool ahead of the body stream's own reads; in-flight
    /// deduplication via `fetch_decoded_cached` makes this safe —
    /// overlapping segments share a single fetch instead of doubling up.
    pub async fn prefetch_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) {
        const PREFETCH_CONCURRENCY: usize = READ_PREFETCH_WINDOW;

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
            .filter(|mid| streamer.state.cache.get(mid).is_none())
            .collect();
        let mut stream = stream::iter(cold)
            .map(move |mid| {
                let s = streamer.clone();
                async move { s.fetch_decoded_cached(&mid).await }
            })
            .buffer_unordered(PREFETCH_CONCURRENCY);
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
    /// and returns a contiguous byte slice.
    pub async fn read_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<u8>, StreamerError> {
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
                self.read_rar(
                    parts,
                    slices,
                    meta.password.as_deref(),
                    start,
                    end_inclusive,
                )
                .await
            }
        }
    }

    /// Read a byte range from a `Direct` source: a single contiguous file
    /// composed of yEnc-encoded NNTP segments. Segments are fetched in
    /// parallel (up to READ_PREFETCH_WINDOW concurrent) and consumed in
    /// order — bounds NNTP round-trip latency for multi-segment reads.
    async fn read_direct(
        &self,
        offsets: &[u64],
        segments: &[NzbSegment],
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<u8>, StreamerError> {
        let mut first = 0usize;
        let mut last = segments.len() - 1;
        for (i, win) in offsets.windows(2).enumerate() {
            if win[1] > start {
                first = i;
                break;
            }
        }
        for (i, win) in offsets.windows(2).enumerate() {
            if win[0] > end_inclusive {
                last = i.saturating_sub(1);
                break;
            }
            last = i;
        }

        let mut decoded_concat = Vec::with_capacity((end_inclusive - start + 1) as usize);

        let streamer = self.clone();
        let mids: Vec<(usize, String)> = (first..=last)
            .map(|i| (i, segments[i].message_id.clone()))
            .collect();
        let mut stream = stream::iter(mids)
            .map(move |(i, mid)| {
                let s = streamer.clone();
                async move { (i, s.fetch_decoded_cached(&mid).await) }
            })
            .buffered(READ_PREFETCH_WINDOW);

        while let Some((idx, result)) = stream.next().await {
            let decoded = result?;

            let seg_enc_start = offsets[idx];
            let seg_enc_end = offsets[idx + 1];
            let enc_len = seg_enc_end - seg_enc_start;
            let dec_len = decoded.len() as u64;

            let req_lo_enc = start.max(seg_enc_start) - seg_enc_start;
            let req_hi_enc = end_inclusive.min(seg_enc_end - 1) - seg_enc_start;
            let lo = ((req_lo_enc as u128 * dec_len as u128) / enc_len as u128) as usize;
            let hi = (((req_hi_enc as u128 + 1) * dec_len as u128) / enc_len as u128) as usize;
            let hi = hi.min(decoded.len());
            if lo < hi {
                decoded_concat.extend_from_slice(&decoded[lo..hi]);
            }
        }

        Ok(decoded_concat)
    }
}
