use bytes::Bytes;
use futures::StreamExt;
use futures::stream;

use crate::nntp::Priority;
use crate::rar;

use super::{
    NzbRarPart, NzbRarSlice, PREFETCH_FLOOR, StreamerError, UsenetStreamer, concat_slices,
};

impl UsenetStreamer {
    /// Read a byte range from a `Rar` source. RAR slice offsets are exact
    /// *decoded* byte positions within each volume (RAR file headers
    /// declare these precisely), so this path must do exact decoded-byte
    /// reads rather than the encoded-byte approximation used by direct
    /// reads. Otherwise the contained file's first bytes — MKV's EBML
    /// signature, MP4's `ftyp` box — would be a few bytes off and the
    /// player's codec probe would fail.
    pub(super) async fn read_rar(
        &self,
        parts: &[NzbRarPart],
        slices: &[NzbRarSlice],
        password: Option<&str>,
        start: u64,
        end_inclusive: u64,
        priority: Priority,
    ) -> Result<Bytes, StreamerError> {
        // Most reads stay inside a single RAR slice; we collect zero-copy
        // `Bytes` slices first and only concatenate when more than one
        // contributes to the range.
        let mut parts_out: Vec<Bytes> = Vec::new();

        let mut virtual_pos: u64 = 0;
        for slice in slices {
            let slice_start = virtual_pos;
            let slice_end = virtual_pos + slice.length;
            virtual_pos = slice_end;

            if slice_end <= start {
                continue;
            }
            if slice_start > end_inclusive {
                break;
            }

            let req_v_lo = start.max(slice_start);
            let req_v_hi = end_inclusive.min(slice_end - 1);

            // Offsets WITHIN this slice's plaintext stream (i.e. byte 0
            // is the first plaintext byte this volume contributes to the
            // contained file).
            let slice_plain_lo = req_v_lo - slice_start;
            let slice_plain_hi = req_v_hi - slice_start;

            let part = parts
                .get(slice.part_index)
                .ok_or(StreamerError::BadRange)?;

            let bytes = match &slice.encryption {
                None => {
                    let part_byte_lo = slice.start_in_part + slice_plain_lo;
                    let part_byte_hi = slice.start_in_part + slice_plain_hi;
                    self.read_decoded_range_within_part(part, part_byte_lo, part_byte_hi, priority)
                        .await?
                }
                Some(enc) => {
                    let pw = password.ok_or(StreamerError::MissingPassword)?;
                    self.read_encrypted_slice(part, slice, enc, pw, slice_plain_lo, slice_plain_hi, priority)
                        .await?
                }
            };
            if !bytes.is_empty() {
                parts_out.push(bytes);
            }
        }

        Ok(concat_slices(parts_out, start, end_inclusive))
    }

    /// Fetch + decrypt a plaintext range from one slice's CBC ciphertext.
    async fn read_encrypted_slice(
        &self,
        part: &NzbRarPart,
        slice: &NzbRarSlice,
        enc: &rar::RarEncryption,
        password: &str,
        slice_plain_lo: u64,
        slice_plain_hi: u64,
        priority: Priority,
    ) -> Result<Bytes, StreamerError> {
        use crate::crypto::{AES_BLOCK, decrypt_blocks_in_place, derive_key};

        let block = AES_BLOCK as u64;
        let block_lo = slice_plain_lo / block;
        let block_hi = slice_plain_hi / block;

        let mut cipher_lo_in_part = slice.start_in_part + block_lo * block;
        let cipher_hi_in_part = slice.start_in_part + (block_hi + 1) * block - 1;

        let need_predecessor = block_lo > 0;
        if need_predecessor {
            cipher_lo_in_part -= block;
        }

        // Decrypt mutates in place, so materialise into an owned BytesMut.
        // The extra copy here is unavoidable but the encrypted-RAR path is
        // not the streaming hot path.
        let fetched_bytes = self
            .read_decoded_range_within_part(part, cipher_lo_in_part, cipher_hi_in_part, priority)
            .await?;
        if fetched_bytes.len() < AES_BLOCK {
            return Err(StreamerError::BadRange);
        }
        let mut fetched: Vec<u8> = fetched_bytes.into();

        let key = derive_key(password, &enc.salt, enc.log2_count);
        let iv = if need_predecessor {
            let mut iv = [0u8; AES_BLOCK];
            iv.copy_from_slice(&fetched[..AES_BLOCK]);
            iv
        } else {
            enc.iv
        };

        let ct_offset = if need_predecessor { AES_BLOCK } else { 0 };
        decrypt_blocks_in_place(&key, &iv, &mut fetched[ct_offset..])?;

        let plain_offset_in_fetched = ct_offset + (slice_plain_lo - block_lo * block) as usize;
        let plain_len = (slice_plain_hi - slice_plain_lo + 1) as usize;
        let end = plain_offset_in_fetched + plain_len;
        if end > fetched.len() {
            return Err(StreamerError::BadRange);
        }
        // Truncate to the plaintext window without re-allocating: drop the
        // tail, then split off the leading padding into a throwaway prefix.
        fetched.truncate(end);
        let plain = fetched.split_off(plain_offset_in_fetched);
        Ok(Bytes::from(plain))
    }

    /// Read decoded bytes `[dec_start, dec_end_inclusive]` from a single
    /// part's segment stream with exact decoded-byte addressing.
    pub(super) async fn read_decoded_range_within_part(
        &self,
        part: &NzbRarPart,
        dec_start: u64,
        dec_end_inclusive: u64,
        priority: Priority,
    ) -> Result<Bytes, StreamerError> {
        match part.decoded_seg_size {
            Some(seg_size) if seg_size > 0 => {
                self.read_decoded_range_uniform(part, seg_size, dec_start, dec_end_inclusive, priority)
                    .await
            }
            _ => {
                self.read_decoded_range_walk(part, dec_start, dec_end_inclusive, priority)
                    .await
            }
        }
    }

    /// Fast path: uniform segment size known. The uniform size is used ONLY
    /// to jump to the anchor segment (the one whose slot contains `dec_start`)
    /// and the in-segment skip — never to size the per-segment slice, which
    /// always comes from each segment's ACTUAL decoded length. yEnc posters
    /// usually use a fixed `=ypart` size, but not always: a non-last segment
    /// can decode shorter than `seg_size`. Sizing slices from `idx * seg_size`
    /// (the old behaviour) then drifts every following segment and returns a
    /// mid-file short read — which the FUSE layer turns into EOF/EIO and the
    /// player stops. So we instead accumulate decoded bytes forward, in
    /// bounded fully-drained batches, until the requested window is filled.
    async fn read_decoded_range_uniform(
        &self,
        part: &NzbRarPart,
        seg_size: u64,
        dec_start: u64,
        dec_end_inclusive: u64,
        priority: Priority,
    ) -> Result<Bytes, StreamerError> {
        let total_segs = part.segments.len();
        // Anchor: floor-divide picks the segment whose uniform slot contains
        // `dec_start`. `skip` is always in `[0, seg_size)`, so it can't
        // underflow even when actual sizes drift from `seg_size`.
        let first_seg = (dec_start / seg_size) as usize;
        if first_seg >= total_segs {
            return Ok(Bytes::new());
        }
        let skip = (dec_start - (first_seg as u64) * seg_size) as usize;
        // First batch generously covers the request so it almost always
        // suffices in one pass; `+2` absorbs ordinary per-segment slop.
        let last_hint = ((dec_end_inclusive / seg_size) as usize).min(total_segs - 1);
        let batch_last = (last_hint + 2).min(total_segs - 1);

        self.assemble_decoded_forward(part, dec_start, dec_end_inclusive, first_seg, batch_last, skip, priority)
            .await
    }

    /// Slow fallback: no uniform size known. Advance past leading segments
    /// whose ACTUAL decoded size is already memoized and which end before
    /// `dec_start`, then assemble forward from there.
    async fn read_decoded_range_walk(
        &self,
        part: &NzbRarPart,
        dec_start: u64,
        dec_end_inclusive: u64,
        priority: Priority,
    ) -> Result<Bytes, StreamerError> {
        let total_segs = part.segments.len();

        let mut decoded_cursor: u64 = 0;
        let mut first_seg = 0usize;
        while first_seg < total_segs {
            let seg = &part.segments[first_seg];
            let Some(size) = self.state.decoded_sizes.get(&seg.message_id) else {
                break;
            };
            if decoded_cursor + size <= dec_start {
                decoded_cursor += size;
                first_seg += 1;
            } else {
                break;
            }
        }
        if first_seg >= total_segs {
            return Ok(Bytes::new());
        }
        let skip = dec_start.saturating_sub(decoded_cursor) as usize;
        let read_concurrency = self.pool.download_concurrency().max(PREFETCH_FLOOR);
        let batch_last = (first_seg + read_concurrency - 1).min(total_segs - 1);

        self.assemble_decoded_forward(part, dec_start, dec_end_inclusive, first_seg, batch_last, skip, priority)
            .await
    }

    /// Assemble `[dec_start, dec_end_inclusive]` by fetching a part's segments
    /// forward from `first_seg`, accumulating each segment's ACTUAL decoded
    /// length (never a slot-derived size) until the requested window is full.
    ///
    /// Mirrors `read_direct`'s assembly invariants:
    ///   - **Bounded, fully-drained batches.** Never break a `buffered` stream
    ///     early: dropping an in-flight `fetch_decoded_cached` cancels its BODY
    ///     mid-read, poisoning the pooled NNTP connection so the next user
    ///     times out and trips the provider circuit breaker. We drain each
    ///     batch in full, only growing to a new batch if still short.
    ///   - **Never short except at true end-of-part.** A mid-file short read is
    ///     catastrophic (FUSE truncates the cached size to EOF), so we keep
    ///     fetching later segments to cover any per-segment decode slop. A
    ///     slightly-off anchor only shifts content by a few bytes (tolerated by
    ///     players); dropping bytes is not tolerated.
    async fn assemble_decoded_forward(
        &self,
        part: &NzbRarPart,
        dec_start: u64,
        dec_end_inclusive: u64,
        first_seg: usize,
        first_batch_last: usize,
        mut skip: usize,
        priority: Priority,
    ) -> Result<Bytes, StreamerError> {
        let want = (dec_end_inclusive - dec_start + 1) as usize;
        let total_segs = part.segments.len();
        if want == 0 || total_segs == 0 {
            return Ok(Bytes::new());
        }

        let read_concurrency = self.pool.download_concurrency().max(PREFETCH_FLOOR);
        let segments = part.segments.as_slice();
        let mut slices: Vec<Bytes> = Vec::new();
        let mut produced: usize = 0;

        let mut batch_start = first_seg;
        let mut batch_last = first_batch_last.max(first_seg).min(total_segs - 1);
        loop {
            let streamer = self.clone();
            let mut stream = stream::iter(batch_start..=batch_last)
                .map(move |i| {
                    let s = streamer.clone();
                    async move { s.fetch_decoded_cached(&segments[i].message_id, priority).await }
                })
                .buffered(read_concurrency);

            while let Some(result) = stream.next().await {
                let decoded = result?;
                if produced >= want {
                    // Request satisfied; keep draining so no in-flight fetch in
                    // this batch is cancelled, but stop accumulating.
                    continue;
                }
                if skip >= decoded.len() {
                    // Anchor skip spans past this whole segment (decode slop put
                    // `dec_start` in a later segment than the slot suggested).
                    skip -= decoded.len();
                    continue;
                }
                let take = (want - produced).min(decoded.len() - skip);
                slices.push(decoded.slice(skip..skip + take));
                produced += take;
                skip = 0;
            }

            if produced >= want || batch_last + 1 >= total_segs {
                // Filled, or ran out of segments (legitimate only at true
                // end-of-part — the outer guard fails EIO if it was mid-file).
                break;
            }
            batch_start = batch_last + 1;
            batch_last = (batch_last + read_concurrency).min(total_segs - 1);
        }

        Ok(concat_slices(slices, dec_start, dec_end_inclusive))
    }
}

