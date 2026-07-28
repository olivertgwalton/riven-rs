use bytes::Bytes;

use crate::rar;

use super::{NzbRarPart, NzbRarSlice, StreamerError, UsenetStreamer, concat_slices};

impl UsenetStreamer {
    /// Read a byte range from a `Rar` source. RAR slice offsets are exact
    /// *decoded* byte positions within each volume, so this path addresses
    /// decoded bytes directly rather than using the encoded-byte
    /// approximation the direct path tolerates. A few bytes of drift here
    /// would move the contained file's MKV EBML signature or MP4 `ftyp` box
    /// and the player's codec probe would fail.
    pub(super) async fn read_rar(
        &self,
        parts: &[NzbRarPart],
        slices: &[NzbRarSlice],
        password: Option<&str>,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Bytes, StreamerError> {
        let mut out: Vec<Bytes> = Vec::new();
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

            let requested_lo = start.max(slice_start) - slice_start;
            let requested_hi = end_inclusive.min(slice_end - 1) - slice_start;
            let part = parts.get(slice.part_index).ok_or(StreamerError::BadRange)?;

            let bytes = match &slice.encryption {
                None => {
                    self.read_decoded_range_within_part(
                        part,
                        slice.start_in_part + requested_lo,
                        slice.start_in_part + requested_hi,
                    )
                    .await?
                }
                Some(encryption) => {
                    let password = password.ok_or(StreamerError::MissingPassword)?;
                    self.read_encrypted_slice(
                        part,
                        slice,
                        encryption,
                        password,
                        requested_lo,
                        requested_hi,
                    )
                    .await?
                }
            };
            if !bytes.is_empty() {
                out.push(bytes);
            }
        }

        Ok(concat_slices(out, start, end_inclusive))
    }

    /// Fetch and decrypt a plaintext range out of one slice's CBC ciphertext.
    async fn read_encrypted_slice(
        &self,
        part: &NzbRarPart,
        slice: &NzbRarSlice,
        encryption: &rar::RarEncryption,
        password: &str,
        plain_lo: u64,
        plain_hi: u64,
    ) -> Result<Bytes, StreamerError> {
        use crate::crypto::{AES_BLOCK, decrypt_blocks_in_place, derive_key};

        let block = AES_BLOCK as u64;
        let block_lo = plain_lo / block;
        let block_hi = plain_hi / block;

        let mut cipher_lo = slice.start_in_part + block_lo * block;
        let cipher_hi = slice.start_in_part + (block_hi + 1) * block - 1;

        // CBC needs the preceding ciphertext block as the IV for any block
        // that is not the slice's first.
        let need_predecessor = block_lo > 0;
        if need_predecessor {
            cipher_lo -= block;
        }

        let fetched = self
            .read_decoded_range_within_part(part, cipher_lo, cipher_hi)
            .await?;
        if fetched.len() < AES_BLOCK {
            return Err(StreamerError::BadRange);
        }
        let mut fetched: Vec<u8> = fetched.into();

        let key = derive_key(password, &encryption.salt, encryption.log2_count);
        let iv = if need_predecessor {
            let mut iv = [0u8; AES_BLOCK];
            iv.copy_from_slice(&fetched[..AES_BLOCK]);
            iv
        } else {
            encryption.iv
        };

        let cipher_offset = if need_predecessor { AES_BLOCK } else { 0 };
        decrypt_blocks_in_place(&key, &iv, &mut fetched[cipher_offset..])?;

        let plain_offset = cipher_offset + (plain_lo - block_lo * block) as usize;
        let end = plain_offset + (plain_hi - plain_lo + 1) as usize;
        if end > fetched.len() {
            return Err(StreamerError::BadRange);
        }
        fetched.truncate(end);
        Ok(Bytes::from(fetched.split_off(plain_offset)))
    }

    /// Read decoded bytes `[dec_start, dec_end_inclusive]` from one volume's
    /// segment stream with exact decoded-byte addressing.
    pub(super) async fn read_decoded_range_within_part(
        &self,
        part: &NzbRarPart,
        dec_start: u64,
        dec_end_inclusive: u64,
    ) -> Result<Bytes, StreamerError> {
        let total = part.segments.len();
        if total == 0 || dec_start > dec_end_inclusive {
            return Ok(Bytes::new());
        }

        let (anchor, skip) = match part.decoded_seg_size {
            // Uniform segment size known: jump straight to the anchor. The
            // size is used ONLY for the anchor and the in-segment skip, never
            // to size a slice — posters usually but not always use a fixed
            // `=ypart` size, and sizing from it drifts every later segment.
            Some(seg_size) if seg_size > 0 => {
                let anchor = (dec_start / seg_size) as usize;
                if anchor >= total {
                    return Ok(Bytes::new());
                }
                (anchor, (dec_start - anchor as u64 * seg_size) as usize)
            }
            // No uniform size: skip leading segments whose actual decoded
            // length we already know and which end before the request.
            _ => {
                let mut cursor = 0u64;
                let mut anchor = 0usize;
                while anchor < total {
                    let Some(size) = self.pool.decoded_size(&part.segments[anchor].message_id)
                    else {
                        break;
                    };
                    if cursor + size > dec_start {
                        break;
                    }
                    cursor += size;
                    anchor += 1;
                }
                if anchor >= total {
                    return Ok(Bytes::new());
                }
                (anchor, dec_start.saturating_sub(cursor) as usize)
            }
        };

        // Only the segments the request actually spans: `assemble_decoded_forward`
        // widens on its own if decode slop leaves it short, and speculative
        // extras would be started eagerly and then cancelled mid-BODY.
        let horizon = match part.decoded_seg_size {
            Some(seg_size) if seg_size > 0 => {
                ((dec_end_inclusive / seg_size) as usize).min(total - 1)
            }
            _ => anchor,
        };

        self.assemble_decoded_forward(part, dec_start, dec_end_inclusive, anchor, horizon, skip)
            .await
    }

    /// Assemble `[dec_start, dec_end_inclusive]` by walking a volume's
    /// segments forward from `anchor`, accumulating each segment's actual
    /// decoded length until the window is full. Never returns short except at
    /// the true end of the volume: a mid-file short read makes FUSE truncate
    /// the file's cached size.
    async fn assemble_decoded_forward(
        &self,
        part: &NzbRarPart,
        dec_start: u64,
        dec_end_inclusive: u64,
        anchor: usize,
        first_horizon: usize,
        mut skip: usize,
    ) -> Result<Bytes, StreamerError> {
        let want = (dec_end_inclusive - dec_start + 1) as usize;
        let total = part.segments.len();
        if want == 0 || total == 0 {
            return Ok(Bytes::new());
        }

        let mut slices: Vec<Bytes> = Vec::new();
        let mut produced = 0usize;
        let mut index = anchor;
        let mut horizon = first_horizon.max(anchor).min(total - 1);

        // A read-ahead unit is aligned on the *virtual file*; a segment is
        // aligned on its *volume*. The two grids never coincide, so essentially
        // every archive unit spans two articles — and walking them serially
        // paid both round trips end to end, which measured as exactly double a
        // single-article read in production. Start them together first, the way
        // streamnzb's read-ahead does, so the walk below collects what is
        // already in flight.
        self.warm_articles(
            part.segments[anchor..=horizon]
                .iter()
                .map(|segment| segment.message_id.as_str()),
        );

        loop {
            // Bytes are still assembled in order: the walk consumes the fetches
            // started above rather than opening a new one per segment.
            for segment in part.segments.iter().take(horizon + 1).skip(index) {
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
                    return Ok(concat_slices(slices, dec_start, dec_end_inclusive));
                }
            }

            if horizon + 1 >= total {
                return Ok(concat_slices(slices, dec_start, dec_end_inclusive));
            }
            index = horizon + 1;
            horizon = (horizon + 1).min(total - 1);
        }
    }
}
