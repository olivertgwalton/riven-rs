use futures::StreamExt;
use futures::stream;

use crate::nntp::Priority;
use crate::rar;

use super::{NzbRarPart, NzbRarSlice, PREFETCH_FLOOR, StreamerError, UsenetStreamer};

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
                    self.read_decoded_range_within_part(part, part_byte_lo, part_byte_hi, priority)
                        .await?
                }
                Some(enc) => {
                    let pw = password.ok_or(StreamerError::MissingPassword)?;
                    self.read_encrypted_slice(part, slice, enc, pw, slice_plain_lo, slice_plain_hi, priority)
                        .await?
                }
            };
            out.extend_from_slice(&bytes);
        }

        Ok(out)
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
    ) -> Result<Vec<u8>, StreamerError> {
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

        let mut fetched = self
            .read_decoded_range_within_part(part, cipher_lo_in_part, cipher_hi_in_part, priority)
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
    pub(super) async fn read_decoded_range_within_part(
        &self,
        part: &NzbRarPart,
        dec_start: u64,
        dec_end_inclusive: u64,
        priority: Priority,
    ) -> Result<Vec<u8>, StreamerError> {
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

    /// Fast path: uniform segment size known, jump directly to the segment
    /// containing `dec_start` and fetch just the overlapping segments.
    async fn read_decoded_range_uniform(
        &self,
        part: &NzbRarPart,
        seg_size: u64,
        dec_start: u64,
        dec_end_inclusive: u64,
        priority: Priority,
    ) -> Result<Vec<u8>, StreamerError> {
        let want_len = (dec_end_inclusive - dec_start + 1) as usize;
        let mut out = Vec::with_capacity(want_len);

        let total_segs = part.segments.len();
        let first_seg = (dec_start / seg_size) as usize;
        let last_seg = ((dec_end_inclusive / seg_size) as usize).min(total_segs - 1);
        if first_seg >= total_segs {
            return Ok(out);
        }

        let read_concurrency = self.pool.download_concurrency().max(PREFETCH_FLOOR);
        let streamer = self.clone();
        let mids: Vec<(usize, String)> = (first_seg..=last_seg)
            .map(|i| (i, part.segments[i].message_id.clone()))
            .collect();
        let mut stream = stream::iter(mids)
            .map(move |(i, mid)| {
                let s = streamer.clone();
                async move { (i, s.fetch_decoded_cached(&mid, priority).await) }
            })
            .buffered(read_concurrency);

        while let Some((idx, result)) = stream.next().await {
            let decoded = result?;
            let seg_lo = (idx as u64) * seg_size;
            let dec_len_usize = decoded.len();
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
        priority: Priority,
    ) -> Result<Vec<u8>, StreamerError> {
        let want_len = (dec_end_inclusive - dec_start + 1) as usize;
        let mut out = Vec::with_capacity(want_len);
        let mut decoded_cursor: u64 = 0;

        let total_segs = part.segments.len();
        let mut next_to_launch = 0usize;

        while next_to_launch < total_segs {
            let seg = &part.segments[next_to_launch];
            let Some(size) = self.state.decoded_sizes.get(&seg.message_id) else {
                break;
            };
            if decoded_cursor + size <= dec_start {
                decoded_cursor += size;
                next_to_launch += 1;
            } else {
                break;
            }
        }

        let read_concurrency = self.pool.download_concurrency().max(PREFETCH_FLOOR);
        let streamer = self.clone();
        let mids: Vec<String> = (next_to_launch..total_segs)
            .map(|i| part.segments[i].message_id.clone())
            .collect();
        let mut stream = stream::iter(mids)
            .map(move |mid| {
                let s = streamer.clone();
                async move { s.fetch_decoded_cached(&mid, priority).await }
            })
            .buffered(read_concurrency);

        while let Some(result) = stream.next().await {
            let decoded = result?;
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
