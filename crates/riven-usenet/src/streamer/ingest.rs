use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesOrdered;
use redis::AsyncCommands;

use crate::nntp::NntpError;
use crate::nzb::{
    NzbFile, NzbSegment, detect_rar_volume_set, filename_from_subject, looks_like_media, parse_nzb,
};
use crate::rar::{self, RarEncryption, RarVolumeFileEntry};

use super::{
    FetchFuture, META_TTL_SECS, NzbMeta, NzbMetaFile, NzbMetaSource, NzbRarPart, NzbRarSlice,
    StreamerError, UsenetStreamer, io_error, meta_key,
};

/// In-progress per-inner-file accumulator during multi-file RAR reconstruction.
struct RarFileGroup {
    name: String,
    unpacked_size: u64,
    encryption: Option<RarEncryption>,
    slices: Vec<NzbRarSlice>,
    /// Running sum of `slices[i].length`. When this equals `unpacked_size`
    /// after every volume has been visited, the file's coverage is complete
    /// and it's safe to expose as a virtual file.
    plaintext_sum: u64,
}

/// Match against the same extensions the downstream persist step accepts as
/// playable video — see `crates/riven-queue/src/flows/download_item/helpers.rs`
/// `is_video_file`. Kept in sync intentionally: returning a virtual file
/// whose extension the queue ignores wastes an ingest cycle.
fn is_media_filename(name: &str) -> bool {
    let ext = name
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        ext.as_str(),
        "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm"
    )
}

/// Sample size for the article-availability probe at ingest. Up to this
/// many random segments from the candidate file are STAT-checked.
const AVAILABILITY_SAMPLE_SIZE: usize = 12;
/// Reject ingest if more than this fraction of probed segments is missing.
const AVAILABILITY_MISSING_LIMIT: f64 = 0.10;
/// Bytes from each volume's prefix to pull before attempting to parse the
/// RAR archive header. One typical NNTP segment is ~750 KB encoded, which
/// after yEnc decode is more than enough to cover MAIN_HEAD + FILE_HEAD.
const RAR_HEADER_PROBE_BYTES: u64 = 32 * 1024;

impl UsenetStreamer {
    /// Probe a sample of `segments` for article availability on the
    /// configured NNTP provider. Returns `Err(IncompleteRelease)` if more
    /// than `AVAILABILITY_MISSING_LIMIT` of the probed segments are
    /// missing; the caller treats this as "release unusable, try the next
    /// ranked candidate." On a healthy release this is a few cheap STAT
    /// round-trips and a green light.
    async fn probe_availability(
        &self,
        segments: &[NzbSegment],
    ) -> Result<(), StreamerError> {
        if segments.is_empty() {
            return Ok(());
        }
        let n = segments.len().min(AVAILABILITY_SAMPLE_SIZE);
        // Spread the sample across the file rather than concentrating at
        // the front: corrupted releases often have gaps anywhere.
        let step = segments.len().max(1) / n.max(1);
        let mut probes: FuturesOrdered<FetchFuture<Result<bool, NntpError>>> =
            FuturesOrdered::new();
        for i in 0..n {
            let idx = (i * step).min(segments.len() - 1);
            let mid = segments[idx].message_id.clone();
            let pool = self.pool.clone();
            probes.push_back(Box::pin(async move { pool.stat(&mid).await }));
        }
        let mut missing = 0usize;
        let mut checked = 0usize;
        while let Some(result) = probes.next().await {
            checked += 1;
            match result {
                Ok(true) => {}
                Ok(false) => missing += 1,
                Err(error) => {
                    // Treat hard NNTP errors as missing so a flaky provider
                    // doesn't pass an empty release through.
                    tracing::debug!(error = %error, "availability probe error; counting as missing");
                    missing += 1;
                }
            }
        }
        let fraction = missing as f64 / checked.max(1) as f64;
        if fraction > AVAILABILITY_MISSING_LIMIT {
            return Err(StreamerError::IncompleteRelease { missing, checked });
        }
        if missing > 0 {
            tracing::info!(missing, checked, "release passed availability probe with some gaps");
        }
        Ok(())
    }

    /// Parse an NZB, build the appropriate metadata (direct file or virtual
    /// RAR-contained file), and persist to Redis. `password` is consulted
    /// only when the archive's file headers report encryption.
    pub async fn ingest(
        &self,
        info_hash: &str,
        nzb_xml: &str,
        password: Option<&str>,
    ) -> Result<NzbMeta, StreamerError> {
        let files = parse_nzb(nzb_xml)?;
        if files.is_empty() {
            return Err(StreamerError::NoMediaFile);
        }

        // For stored multi-volume RAR archives the contained media isn't
        // present as a top-level NZB file — try building virtual files first.
        // A single-file RAR archive produces one virtual file (movie); a
        // multi-file RAR (season pack) produces one virtual file per inner
        // media file.
        let (meta_files, file_password) =
            match self.try_build_rar_virtual_files(&files, password).await? {
                Some(virtual_files) if !virtual_files.is_empty() => {
                    let is_encrypted = virtual_files.iter().any(|vf| {
                        matches!(
                            &vf.source,
                            NzbMetaSource::Rar { slices, .. }
                                if slices.iter().any(|s| s.encryption.is_some())
                        )
                    });
                    // All virtual files share the same `parts` array; probe
                    // availability once across every segment.
                    if let Some(NzbMetaSource::Rar { parts, .. }) =
                        virtual_files.first().map(|vf| &vf.source)
                    {
                        let probe_segments: Vec<NzbSegment> = parts
                            .iter()
                            .flat_map(|p| p.segments.iter().cloned())
                            .collect();
                        self.probe_availability(&probe_segments).await?;
                    }
                    let mut out = virtual_files;
                    // Keep the underlying RAR parts as additional entries so
                    // sidecars (par2/nfo/etc.) and the parts themselves remain
                    // reachable for debugging.
                    for f in &files {
                        out.push(direct_meta_file(f));
                    }
                    let pw = if is_encrypted {
                        password.map(str::to_string)
                    } else {
                        None
                    };
                    (out, pw)
                }
                _ => {
                    let mut ordered: Vec<NzbMetaFile> = files.iter().map(direct_meta_file).collect();
                    if let Some(primary_idx) = pick_primary_media_index(&files) {
                        ordered.swap(0, primary_idx);
                    }
                    if let Some(primary) = ordered.first()
                        && let NzbMetaSource::Direct { segments, .. } = &primary.source
                    {
                        self.probe_availability(segments).await?;
                    }
                    (ordered, None)
                }
            };

        let meta = NzbMeta {
            info_hash: info_hash.to_string(),
            files: meta_files,
            password: file_password,
        };

        let json = serde_json::to_string(&meta).map_err(|e| {
            StreamerError::Redis(redis::RedisError::from(io_error(e.to_string())))
        })?;
        let mut redis = self.redis.clone();
        let _: () =
            AsyncCommands::set_ex(&mut redis, meta_key(info_hash), json, META_TTL_SECS as u64)
                .await?;

        self.meta_cache
            .put(info_hash.to_string(), Arc::new(meta.clone()));

        Ok(meta)
    }

    /// If `files` contains a stored multi-volume RAR archive, fetch each
    /// volume's header bytes, parse them, and return one virtual `NzbMetaFile`
    /// per contained media file. A single-file RAR (movie split across
    /// volumes) yields one virtual file; a multi-file RAR (season pack)
    /// yields one per inner episode.
    ///
    /// Returns `Ok(None)` if it isn't a RAR set, any volume's header can't be
    /// fetched/parsed, or no inner media file's slices add up to its declared
    /// unpacked size. The caller falls back to exposing the top-level NZB
    /// files directly when this returns `None`.
    async fn try_build_rar_virtual_files(
        &self,
        files: &[NzbFile],
        password: Option<&str>,
    ) -> Result<Option<Vec<NzbMetaFile>>, StreamerError> {
        let Some(ordered_indices) = detect_rar_volume_set(files) else {
            return Ok(None);
        };
        if ordered_indices.is_empty() {
            return Ok(None);
        }

        let mut parts: Vec<NzbRarPart> = Vec::with_capacity(ordered_indices.len());
        let mut volume_entries: Vec<Vec<RarVolumeFileEntry>> =
            Vec::with_capacity(ordered_indices.len());

        for (vol_idx, &nzb_idx) in ordered_indices.iter().enumerate() {
            let f = &files[nzb_idx];
            if f.segments.is_empty() {
                return Ok(None);
            }
            let mut part = build_rar_part(f);

            let header_bytes = match self
                .fetch_volume_header_bytes(&part, RAR_HEADER_PROBE_BYTES)
                .await
            {
                Ok(b) => b,
                Err(error) => {
                    tracing::debug!(
                        vol_idx,
                        filename = %part.filename,
                        error = %error,
                        "RAR volume header fetch failed; treating as non-RAR NZB"
                    );
                    return Ok(None);
                }
            };

            // The header probe just decoded the first segment(s) of this
            // part. Their decoded sizes are memoized — record the first
            // one as the part's uniform segment size so the read path can
            // map decoded positions to segments in O(1).
            if let Some(first_seg) = part.segments.first()
                && let Some(size) = self.decoded_sizes.get(&first_seg.message_id)
            {
                part.decoded_seg_size = Some(size);
            }
            let parsed = match rar::parse_volume_header(&header_bytes) {
                Ok(h) => h,
                Err(error) => {
                    tracing::debug!(
                        vol_idx,
                        filename = %part.filename,
                        error = %error,
                        "RAR header parse failed; treating as non-RAR NZB"
                    );
                    return Ok(None);
                }
            };
            let entries: Vec<RarVolumeFileEntry> =
                parsed.files.into_iter().filter(|e| e.is_stored()).collect();
            if entries.is_empty() {
                tracing::debug!(
                    vol_idx,
                    "no stored file entry in this RAR volume; bailing"
                );
                return Ok(None);
            }

            volume_entries.push(entries);
            parts.push(part);
        }

        // Walk every (volume, stored_entry) pair in order and group by
        // inner-file name. Each unique name becomes a candidate virtual file
        // assembled from its per-volume slices. Grouping by name (rather than
        // relying on RAR4's split_before/after flags) means we also handle
        // RAR5 archives — our parser doesn't read RAR5 extra-area split
        // records, so those entries arrive with both flags zero.
        let any_encrypted = volume_entries
            .iter()
            .flatten()
            .any(|e| e.encryption.is_some());
        if any_encrypted && password.is_none() {
            tracing::warn!(
                "RAR archive is encrypted but no archive password configured; \
                 set the `archivepassword` plugin setting"
            );
            return Err(StreamerError::MissingPassword);
        }

        // Preserve first-occurrence order so the returned files match the
        // playback order in the archive (E01 before E02, etc.).
        let mut group_order: Vec<String> = Vec::new();
        let mut groups: std::collections::HashMap<String, RarFileGroup> =
            std::collections::HashMap::new();

        for (vol_idx, entries) in volume_entries.iter().enumerate() {
            for entry in entries {
                let g = groups.entry(entry.name.clone()).or_insert_with(|| {
                    group_order.push(entry.name.clone());
                    RarFileGroup {
                        name: entry.name.clone(),
                        unpacked_size: entry.unpacked_size,
                        encryption: entry.encryption.clone(),
                        slices: Vec::new(),
                        plaintext_sum: 0,
                    }
                });
                // Encrypted slice plaintext length is capped at the file's
                // remaining unpacked bytes — the last volume absorbs the
                // 16-byte AES padding difference.
                let cipher_len = entry.packed_size;
                let plaintext_len = if g.encryption.is_some() {
                    cipher_len.min(g.unpacked_size.saturating_sub(g.plaintext_sum))
                } else {
                    cipher_len
                };
                g.slices.push(NzbRarSlice {
                    part_index: vol_idx,
                    start_in_part: entry.data_offset,
                    length: plaintext_len,
                    encryption: entry.encryption.clone(),
                    ciphertext_length: cipher_len,
                });
                g.plaintext_sum = g.plaintext_sum.saturating_add(plaintext_len);
            }
        }

        // For each inner file whose observed slices don't cover its declared
        // unpacked size, synthesize the missing first slice. This handles
        // the common case where the 32 KB front-of-volume probe sees the
        // previous file's end-continuation header (with `split_after=false`)
        // and breaks before reaching the next file's start header in the
        // same "transition" volume.
        //
        // Synthesis is pure arithmetic on what we already know:
        //   - The file's `unpacked_size` is repeated in every continuation
        //     header so we have it from any observed slice.
        //   - The missing chunk lives in the volume immediately before the
        //     file's first observed slice. In that volume, our parser's
        //     last seen entry's `data_offset + packed_size` is the byte
        //     position where the previous file's data ended — i.e. exactly
        //     where this file's missing first chunk begins.
        //   - The missing length is `unpacked_size - plaintext_sum`.
        //
        // Works for both RAR4 and RAR5; no split-flag dependency. The
        // edge case it doesn't handle is a small inner file fitting
        // entirely inside a single transition volume with no observed
        // continuation in a later volume (would require fetching).
        // Season-pack episodes are large enough that this doesn't happen.
        for name in &group_order {
            let Some(g) = groups.get(name) else { continue };
            if g.plaintext_sum >= g.unpacked_size {
                continue;
            }
            let Some(first_slice) = g.slices.first() else {
                continue;
            };
            let prev_vol = match first_slice.part_index.checked_sub(1) {
                Some(v) => v,
                None => continue, // first observed slice is in vol 0 — nowhere to look back to
            };
            let Some(prev_last) = volume_entries[prev_vol].last() else {
                continue;
            };
            let synth_start = prev_last.data_offset.saturating_add(prev_last.packed_size);
            let synth_len = g.unpacked_size.saturating_sub(g.plaintext_sum);
            // Encrypted RAR multi-file synthesis would need AES-block
            // alignment maths the observed slices already encode for us;
            // it's not in scope here. Drop those rather than risk emitting
            // an unreadable virtual file.
            if g.encryption.is_some() {
                tracing::debug!(
                    name = %g.name,
                    "incomplete encrypted RAR inner file; synthesis skipped"
                );
                continue;
            }
            let synth_slice = NzbRarSlice {
                part_index: prev_vol,
                start_in_part: synth_start,
                length: synth_len,
                encryption: None,
                ciphertext_length: synth_len,
            };
            tracing::debug!(
                name = %g.name,
                prev_vol,
                synth_start,
                synth_len,
                "synthesized missing first slice for RAR inner file"
            );
            let g = groups.get_mut(name).expect("group exists");
            g.slices.insert(0, synth_slice);
            g.plaintext_sum = g.plaintext_sum.saturating_add(synth_len);
        }

        // Validate and filter. A group is usable when (a) its slice plaintext
        // sum equals the declared unpacked size — i.e. we observed every
        // volume the file lives in — and (b) it has a media file extension.
        // Anything else (incomplete coverage, .nfo, .sfv, sample.mkv if the
        // sample isn't actually playable, etc.) gets dropped here so the
        // persist step downstream doesn't waste a slot on it.
        let mut out: Vec<NzbMetaFile> = Vec::new();
        for name in group_order {
            let Some(g) = groups.remove(&name) else { continue };
            if g.plaintext_sum != g.unpacked_size {
                tracing::debug!(
                    name = %g.name,
                    plaintext_sum = g.plaintext_sum,
                    declared = g.unpacked_size,
                    "RAR inner file slices do not cover its declared size; skipping"
                );
                continue;
            }
            if !is_media_filename(&g.name) {
                tracing::debug!(
                    name = %g.name,
                    "RAR inner file is not a media type; skipping"
                );
                continue;
            }
            out.push(NzbMetaFile {
                filename: g.name,
                total_size: g.unpacked_size,
                source: NzbMetaSource::Rar {
                    parts: parts.clone(),
                    slices: g.slices,
                },
            });
        }

        if out.is_empty() {
            tracing::debug!("no usable media files reconstructed from RAR set; bailing");
            return Ok(None);
        }
        Ok(Some(out))
    }

    /// Read up to `wanted` bytes from the start of a RAR volume (one NZB
    /// file). May return slightly more (whole segments). Routes through the
    /// cache so header segments stay warm for subsequent playback reads.
    async fn fetch_volume_header_bytes(
        &self,
        part: &NzbRarPart,
        wanted: u64,
    ) -> Result<Vec<u8>, StreamerError> {
        let mut buf: Vec<u8> = Vec::with_capacity(wanted as usize + 4096);
        for seg in &part.segments {
            let decoded = self.fetch_decoded_cached(&seg.message_id).await?;
            buf.extend_from_slice(&decoded);
            if (buf.len() as u64) >= wanted {
                break;
            }
        }
        Ok(buf)
    }

}

pub(crate) fn direct_meta_file(f: &NzbFile) -> NzbMetaFile {
    let mut offsets = Vec::with_capacity(f.segments.len() + 1);
    let mut acc: u64 = 0;
    offsets.push(0);
    for seg in &f.segments {
        acc += seg.bytes;
        offsets.push(acc);
    }
    NzbMetaFile {
        filename: filename_from_subject(&f.subject),
        total_size: acc,
        source: NzbMetaSource::Direct {
            offsets,
            segments: f.segments.clone(),
        },
    }
}

fn build_rar_part(f: &NzbFile) -> NzbRarPart {
    let mut offsets = Vec::with_capacity(f.segments.len() + 1);
    let mut acc: u64 = 0;
    offsets.push(0);
    for seg in &f.segments {
        acc += seg.bytes;
        offsets.push(acc);
    }
    NzbRarPart {
        filename: filename_from_subject(&f.subject),
        total_size: acc,
        offsets,
        segments: f.segments.clone(),
        decoded_seg_size: None,
    }
}

pub(crate) fn pick_primary_media_index(files: &[NzbFile]) -> Option<usize> {
    files
        .iter()
        .enumerate()
        .filter(|(_, f)| looks_like_media(f))
        .max_by_key(|(_, f)| f.segments.iter().map(|s| s.bytes).sum::<u64>())
        .map(|(i, _)| i)
}
