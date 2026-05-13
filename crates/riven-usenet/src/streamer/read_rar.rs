use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesOrdered;

use crate::rar;

use super::{
    FetchFuture, NzbRarPart, NzbRarSlice, READ_PREFETCH_WINDOW, StreamerError, UsenetStreamer,
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
    ) -> Result<Vec<u8>, StreamerError> {
        let mut out = Vec::with_capacity((end_inclusive - start + 1) as usize);

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
                    self.read_decoded_range_within_part(part, part_byte_lo, part_byte_hi)
                        .await?
                }
                Some(enc) => {
                    let pw = password.ok_or(StreamerError::MissingPassword)?;
                    self.read_encrypted_slice(part, slice, enc, pw, slice_plain_lo, slice_plain_hi)
                        .await?
                }
            };
            out.extend_from_slice(&bytes);
        }

        Ok(out)
    }

    /// Fetch + decrypt a plaintext range from one slice's CBC ciphertext.
    ///
    /// `slice_plain_lo..=slice_plain_hi` are byte offsets in this slice's
    /// plaintext (which the contained file sees). The slice's ciphertext
    /// is a single CBC stream beginning at `slice.start_in_part` of length
    /// `slice.ciphertext_length`. The slice's IV is the chaining IV for
    /// block 0; for any later block we use the preceding ciphertext block
    /// as the chaining IV (standard CBC random-access pattern).
    async fn read_encrypted_slice(
        &self,
        part: &NzbRarPart,
        slice: &NzbRarSlice,
        enc: &rar::RarEncryption,
        password: &str,
        slice_plain_lo: u64,
        slice_plain_hi: u64,
    ) -> Result<Vec<u8>, StreamerError> {
        use crate::crypto::{AES_BLOCK, decrypt_blocks_in_place, derive_key};

        let block = AES_BLOCK as u64;
        let block_lo = slice_plain_lo / block;
        let block_hi = slice_plain_hi / block;

        // Ciphertext byte range within the part (inclusive).
        // Block K of the CBC stream lives at start_in_part + K*block.
        let mut cipher_lo_in_part = slice.start_in_part + block_lo * block;
        let cipher_hi_in_part = slice.start_in_part + (block_hi + 1) * block - 1;

        // If we're not at block 0, fetch one extra preceding block to use
        // as the CBC chaining IV.
        let need_predecessor = block_lo > 0;
        if need_predecessor {
            cipher_lo_in_part -= block;
        }

        let mut fetched = self
            .read_decoded_range_within_part(part, cipher_lo_in_part, cipher_hi_in_part)
            .await?;
        if fetched.len() < AES_BLOCK {
            return Err(StreamerError::BadRange);
        }

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

        // Slice out the requested plaintext window.
        let plain_offset_in_fetched = ct_offset + (slice_plain_lo - block_lo * block) as usize;
        let plain_len = (slice_plain_hi - slice_plain_lo + 1) as usize;
        let end = plain_offset_in_fetched + plain_len;
        if end > fetched.len() {
            return Err(StreamerError::BadRange);
        }
        Ok(fetched[plain_offset_in_fetched..end].to_vec())
    }

    /// Read decoded bytes `[dec_start, dec_end_inclusive]` from a single
    /// part's segment stream with exact decoded-byte addressing.
    ///
    /// When `part.decoded_seg_size` is known (populated at ingest from the
    /// first segment's yEnc part size), we map decoded positions to
    /// segments in O(1) and fetch only the segments that overlap the
    /// request. Otherwise we walk from the first segment with an unknown
    /// decoded size, which is slow on cold seeks but self-corrects via
    /// `DecodedSizes` memoization.
    async fn read_decoded_range_within_part(
        &self,
        part: &NzbRarPart,
        dec_start: u64,
        dec_end_inclusive: u64,
    ) -> Result<Vec<u8>, StreamerError> {
        match part.decoded_seg_size {
            Some(seg_size) if seg_size > 0 => {
                self.read_decoded_range_uniform(part, seg_size, dec_start, dec_end_inclusive)
                    .await
            }
            _ => {
                self.read_decoded_range_walk(part, dec_start, dec_end_inclusive)
                    .await
            }
        }
    }

    /// Fast path: uniform segment size known, jump directly to the segment
    /// containing `dec_start` and fetch just the overlapping segments.
    async fn read_decoded_range_uniform(
        &self,
        part: &NzbRarPart,
        seg_size: u64,
        dec_start: u64,
        dec_end_inclusive: u64,
    ) -> Result<Vec<u8>, StreamerError> {
        let want_len = (dec_end_inclusive - dec_start + 1) as usize;
        let mut out = Vec::with_capacity(want_len);

        let total_segs = part.segments.len();
        let first_seg = (dec_start / seg_size) as usize;
        let last_seg = ((dec_end_inclusive / seg_size) as usize).min(total_segs - 1);
        if first_seg >= total_segs {
            return Ok(out);
        }

        let mut in_flight: FuturesOrdered<FetchFuture<(usize, Result<Arc<Vec<u8>>, StreamerError>)>> =
            FuturesOrdered::new();
        let mut next_to_launch = first_seg;

        while in_flight.len() < READ_PREFETCH_WINDOW && next_to_launch <= last_seg {
            let i = next_to_launch;
            let mid = part.segments[i].message_id.clone();
            let streamer = self.clone();
            in_flight.push_back(Box::pin(async move {
                (i, streamer.fetch_decoded_cached(&mid).await)
            }));
            next_to_launch += 1;
        }

        while let Some((idx, result)) = in_flight.next().await {
            let decoded = result?;
            if next_to_launch <= last_seg {
                let i = next_to_launch;
                let mid = part.segments[i].message_id.clone();
                let streamer = self.clone();
                in_flight.push_back(Box::pin(async move {
                    (i, streamer.fetch_decoded_cached(&mid).await)
                }));
                next_to_launch += 1;
            }

            let seg_lo = (idx as u64) * seg_size;
            let dec_len_usize = decoded.len();
            // Last segment may be shorter than seg_size; otherwise the
            // fetched length confirms our uniform assumption.
            let take_lo_u64 = dec_start.saturating_sub(seg_lo);
            let take_lo = (take_lo_u64 as usize).min(dec_len_usize);
            let take_hi_inclusive_u64 = dec_end_inclusive.saturating_sub(seg_lo);
            let take_hi_inclusive =
                (take_hi_inclusive_u64 as usize).min(dec_len_usize.saturating_sub(1));
            if take_lo <= take_hi_inclusive {
                out.extend_from_slice(&decoded[take_lo..=take_hi_inclusive]);
            }

            if out.len() >= want_len {
                break;
            }
        }

        Ok(out)
    }

    /// Slow fallback: no uniform size known. Walk segments from the first
    /// non-memoized one to build the decoded cursor exactly.
    async fn read_decoded_range_walk(
        &self,
        part: &NzbRarPart,
        dec_start: u64,
        dec_end_inclusive: u64,
    ) -> Result<Vec<u8>, StreamerError> {
        let want_len = (dec_end_inclusive - dec_start + 1) as usize;
        let mut out = Vec::with_capacity(want_len);
        let mut decoded_cursor: u64 = 0;

        let total_segs = part.segments.len();
        let mut next_to_launch = 0usize;

        while next_to_launch < total_segs {
            let seg = &part.segments[next_to_launch];
            let Some(size) = self.decoded_sizes.get(&seg.message_id) else {
                break;
            };
            if decoded_cursor + size <= dec_start {
                decoded_cursor += size;
                next_to_launch += 1;
            } else {
                break;
            }
        }

        let mut in_flight: FuturesOrdered<FetchFuture<Result<Arc<Vec<u8>>, StreamerError>>> =
            FuturesOrdered::new();

        while in_flight.len() < READ_PREFETCH_WINDOW && next_to_launch < total_segs {
            let mid = part.segments[next_to_launch].message_id.clone();
            let streamer = self.clone();
            in_flight
                .push_back(Box::pin(async move { streamer.fetch_decoded_cached(&mid).await }));
            next_to_launch += 1;
        }

        while let Some(result) = in_flight.next().await {
            let decoded = result?;
            if next_to_launch < total_segs {
                let mid = part.segments[next_to_launch].message_id.clone();
                let streamer = self.clone();
                in_flight.push_back(Box::pin(async move {
                    streamer.fetch_decoded_cached(&mid).await
                }));
                next_to_launch += 1;
            }

            let dec_len = decoded.len() as u64;
            let seg_lo = decoded_cursor;
            let seg_hi = decoded_cursor + dec_len;
            decoded_cursor = seg_hi;

            if seg_hi <= dec_start {
                continue;
            }
            if seg_lo > dec_end_inclusive {
                break;
            }

            let dec_len_usize = decoded.len();
            let take_lo = (dec_start.max(seg_lo) - seg_lo) as usize;
            let take_hi_inclusive = (dec_end_inclusive.min(seg_hi - 1) - seg_lo) as usize;
            let take_lo = take_lo.min(dec_len_usize);
            let take_hi_inclusive = take_hi_inclusive.min(dec_len_usize.saturating_sub(1));
            if take_lo <= take_hi_inclusive {
                let Some(slice) = decoded.get(take_lo..=take_hi_inclusive) else {
                    return Err(StreamerError::BadRange);
                };
                out.extend_from_slice(slice);
            }

            if out.len() >= want_len {
                break;
            }
        }

        Ok(out)
    }
}
