use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesOrdered;
use redis::AsyncCommands;

use crate::nntp::NntpError;
use crate::nzb::{
    NzbFile, NzbSegment, detect_rar_volume_set, filename_from_subject, looks_like_media, parse_nzb,
};
use crate::rar::{self, RarVolumeFileEntry};

use super::{
    FetchFuture, META_TTL_SECS, NzbMeta, NzbMetaFile, NzbMetaSource, NzbRarPart, NzbRarSlice,
    StreamerError, UsenetStreamer, io_error, meta_key,
};

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
        // present as a top-level NZB file — try building a virtual file first.
        let (meta_files, file_password) = match self.try_build_rar_virtual_file(&files, password).await? {
            Some(virtual_file) => {
                let is_encrypted = matches!(
                    &virtual_file.source,
                    NzbMetaSource::Rar { slices, .. } if slices.iter().any(|s| s.encryption.is_some())
                );
                if let NzbMetaSource::Rar { parts, .. } = &virtual_file.source {
                    let probe_segments: Vec<NzbSegment> = parts
                        .iter()
                        .flat_map(|p| p.segments.iter().cloned())
                        .collect();
                    self.probe_availability(&probe_segments).await?;
                }
                let mut out = vec![virtual_file];
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
            None => {
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

    /// If `files` contains a stored multi-volume RAR archive whose contained
    /// file is media, fetch each volume's header bytes, parse them, and
    /// return a virtual `NzbMetaFile` representing the contained file.
    /// Returns `Ok(None)` if no RAR set, if it isn't stored, or if any
    /// volume's header can't be fetched/parsed.
    async fn try_build_rar_virtual_file(
        &self,
        files: &[NzbFile],
        password: Option<&str>,
    ) -> Result<Option<NzbMetaFile>, StreamerError> {
        let Some(ordered_indices) = detect_rar_volume_set(files) else {
            return Ok(None);
        };
        if ordered_indices.is_empty() {
            return Ok(None);
        }

        let mut parts: Vec<NzbRarPart> = Vec::with_capacity(ordered_indices.len());
        let mut entries_per_volume: Vec<RarVolumeFileEntry> = Vec::with_capacity(ordered_indices.len());

        for (vol_idx, &nzb_idx) in ordered_indices.iter().enumerate() {
            let f = &files[nzb_idx];
            if f.segments.is_empty() {
                return Ok(None);
            }
            let part = build_rar_part(f);
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

            // Require a single stored primary entry per volume (typical scene
            // packaging); bail if anything's weird.
            let Some(file_entry) = parsed.files.into_iter().find(|e| e.is_stored()) else {
                tracing::debug!(
                    vol_idx,
                    "no stored file entry in this RAR volume; bailing"
                );
                return Ok(None);
            };

            // RAR4 sanity: volume 0 must not have split_before; later
            // volumes must. RAR5 leaves both flags false on this parser
            // since split detection lives in extra-area records — we let
            // the sum-vs-unpacked-size check below catch malformed sets.
            if vol_idx == 0 && file_entry.split_before {
                tracing::debug!("first volume has SPLIT_BEFORE set; bailing");
                return Ok(None);
            }

            entries_per_volume.push(file_entry);
            parts.push(part);
        }

        // First volume's declared unpacked size is the contained file's true
        // size; the sum of slice plaintext lengths must match.
        let total_unpacked = entries_per_volume[0].unpacked_size;
        let any_encrypted = entries_per_volume.iter().any(|e| e.encryption.is_some());
        if any_encrypted && password.is_none() {
            tracing::warn!(
                "RAR archive is encrypted but no archive password configured; \
                 set the `archivepassword` plugin setting"
            );
            return Err(StreamerError::MissingPassword);
        }
        let mut slices = Vec::with_capacity(entries_per_volume.len());
        let mut plaintext_sum: u64 = 0;
        for (i, entry) in entries_per_volume.iter().enumerate() {
            // For encrypted slices `packed_size` is the on-volume
            // ciphertext byte count (16-byte aligned). The slice's
            // PLAINTEXT length is the per-volume contribution to the
            // contained file's unpacked size. We reconstruct it by
            // subtracting any cumulative excess from the running total —
            // last volume absorbs the diff.
            let cipher_len = entry.packed_size;
            let plaintext_len = if entry.encryption.is_some() {
                cipher_len.min(total_unpacked.saturating_sub(plaintext_sum))
            } else {
                cipher_len
            };
            slices.push(NzbRarSlice {
                part_index: i,
                start_in_part: entry.data_offset,
                length: plaintext_len,
                encryption: entry.encryption.clone(),
                ciphertext_length: cipher_len,
            });
            plaintext_sum = plaintext_sum.saturating_add(plaintext_len);
        }
        if plaintext_sum != total_unpacked {
            tracing::debug!(
                plaintext_sum,
                declared = total_unpacked,
                "RAR slice plaintext total != declared unpacked size; bailing"
            );
            return Ok(None);
        }

        let contained_name = entries_per_volume[0].name.clone();
        Ok(Some(NzbMetaFile {
            filename: contained_name,
            total_size: total_unpacked,
            source: NzbMetaSource::Rar { parts, slices },
        }))
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
