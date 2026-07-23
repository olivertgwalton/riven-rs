use std::sync::Arc;

use futures::StreamExt;
use futures::stream;

use crate::nzb::{
    NzbFile, NzbSegment, detect_rar_volume_groups, filename_from_subject, looks_like_media,
    looks_obfuscated, parse_nzb_document,
};
use crate::par2::{
    Par2Block, Par2FileDesc, looks_like_par2, parse_file_descriptors, parse_ifsc_packets,
    parse_slice_size,
};
use crate::rar::{self, RarEncryption, RarVolumeFileEntry};

use super::store;
use super::{
    NzbMeta, NzbMetaFile, NzbMetaSource, NzbRarPart, NzbRarSlice, StreamerError, SweepCounts,
    UsenetStreamer, select_validation_indices, stat_sweep,
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
    let ext = name.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    matches!(
        ext.as_str(),
        "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm"
    )
}

/// Whether a group's slices (assumed already in ascending `part_index`
/// order, as produced by the volume-order iteration plus at most one
/// prepended synthesized slice) skip a RAR volume. A single file's data is
/// always contiguous across volumes, so consecutive slices must sit in
/// adjacent volumes; a jump of more than one means at least one volume's
/// worth of this file's data was never located. Returns the boundary
/// `(prev_part_index, next_part_index)` of the first such gap, if any.
pub(crate) fn first_slice_gap(slices: &[NzbRarSlice]) -> Option<(usize, usize)> {
    slices
        .windows(2)
        .find(|w| w[1].part_index > w[0].part_index + 1)
        .map(|w| (w[0].part_index, w[1].part_index))
}

/// Per-NZB PAR2 index used to verify a RAR volume's actual content, not just
/// its existence or header shape: block-level CRC32 checksums from the
/// release's own PAR2 set, keyed by [`normalize_par2_name`] of the volume
/// filename. Neither `probe_availability` (confirms articles exist) nor the
/// RAR header parse (confirms the bytes *parse* as a valid volume) can catch
/// a volume whose segments point at entirely different, but validly-shaped,
/// content — this can.
struct Par2Index {
    slice_size: u64,
    blocks_by_filename: std::collections::HashMap<String, Vec<Par2Block>>,
}

/// Normalizes a filename for matching a PAR2 `FileDesc` name against an NZB
/// volume's own filename. These are supposed to be the same string, but in
/// the wild some posting tools write the `FileDesc` name with spaces where
/// the actual filename (and NZB subject) uses dots — e.g. PAR2 says
/// `"Show S01 x265.part07.rar"` while the NZB file is
/// `Show.S01.x265.part07.rar`. An exact-match lookup silently misses every
/// volume in that case (verified against a real release: this is not a
/// hypothetical). Stripping all non-alphanumeric characters and
/// lowercasing sidesteps every separator-style difference we've observed.
fn normalize_par2_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// Blocks to sample per volume when verifying against PAR2: first, middle,
/// last. Unlike the free STAT-based segment-availability sampling above,
/// each sampled block here is a real data fetch (a PAR2 slice is typically
/// ~1 MB, versus a STAT's single round-trip), so the sample stays fixed and
/// small rather than scaling with `sample_percent`. Three blocks are enough
/// to catch whole-volume content substitution — segments that exist, STAT
/// fine, and parse as a valid RAR header, but whose payload belongs to
/// something else entirely (the failure mode this check exists for) — without
/// materially slowing ingest.
pub(crate) fn par2_sample_block_indices(total_blocks: usize) -> Vec<usize> {
    match total_blocks {
        0 => Vec::new(),
        1 => vec![0],
        2 => vec![0, 1],
        n => {
            let mut v = vec![0, n / 2, n - 1];
            v.sort_unstable();
            v.dedup();
            v
        }
    }
}

/// Default fraction of a candidate file's segments to STAT-probe at ingest,
/// matching altmount's `segment_sample_percentage` default of 5%. A fixed tiny
/// sample (the old 12 segments) is ~0.1% of a 10 GB REMUX, so sparsely-dead
/// releases passed ingest and only failed mid-playback. 5% reliably surfaces
/// gaps. Overridable per-install via the `availabilitysamplepercent` setting.
pub const DEFAULT_AVAILABILITY_SAMPLE_PERCENT: usize = 5;
/// Maximum fraction of probed segments allowed to error transiently (provider
/// hiccup) before we treat the release as unverifiable. *Confirmed*-missing
/// segments (STAT says not present) are zero-tolerance — the read path has no
/// par2 data repair, so a single dead segment in the played range stalls
/// playback (mirrors altmount's zero-missing health policy).
const AVAILABILITY_ERROR_LIMIT: f64 = 0.5;
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
        sample_percent: usize,
    ) -> Result<(), StreamerError> {
        if segments.is_empty() {
            return Ok(());
        }
        let mids: Vec<String> = select_validation_indices(segments.len(), sample_percent)
            .into_iter()
            .map(|i| segments[i].message_id.clone())
            .collect();
        let n = mids.len();
        if n == 0 {
            return Ok(());
        }
        let probe_concurrency = self
            .prefetch_concurrency(self.pool.bulk_client().capacity())
            .min(n);
        // stop_on_first_miss: zero tolerance for confirmed-missing segments
        // means the rest of the sample is wasted work the instant one hits —
        // there's no par2 data repair on the read path, so even one dead
        // segment in the file stalls playback when it's reached. Cancel and
        // let the caller try the next ranked candidate (mirrors altmount's
        // FastFailReleaseProbe cancelling on first miss).
        let SweepCounts {
            missing,
            errors,
            checked,
        } = stat_sweep(&self.pool.bulk_client(), mids, probe_concurrency, true).await;

        if missing > 0 {
            return Err(StreamerError::IncompleteRelease { missing, checked });
        }
        if errors as f64 / checked.max(1) as f64 > AVAILABILITY_ERROR_LIMIT {
            return Err(StreamerError::IncompleteRelease {
                missing: errors,
                checked,
            });
        }
        Ok(())
    }

    /// Parse an NZB, build the appropriate metadata (direct file or virtual
    /// RAR-contained file), and persist to Redis. `password` is consulted
    /// only when the archive's file headers report encryption; when omitted,
    /// the NZB's own `<meta type="password">` is used as a fallback.
    /// `verify_par2` gates the PAR2 block-checksum check on RAR volumes
    /// (see [`Self::fetch_par2_index`]) — opt-in because, unlike every other
    /// ingest-time check, it fetches real slice-sized chunks of content
    /// rather than just STATing existence, adding real bandwidth per volume
    /// covered by the release's PAR2 set.
    pub async fn ingest(
        &self,
        info_hash: &str,
        nzb_xml: &str,
        password: Option<&str>,
        sample_percent: usize,
        verify_par2: bool,
    ) -> Result<NzbMeta, StreamerError> {
        if let Some(existing) = store::load(&self.db, info_hash).await? {
            tracing::debug!(
                info_hash,
                file_count = existing.files.len(),
                "usenet ingest: reusing persisted NZB meta (idempotent hit)"
            );
            self.state
                .meta_cache
                .put(info_hash.to_string(), Arc::new(existing.clone()));
            return Ok(existing);
        }

        let document = parse_nzb_document(nzb_xml)?;
        let release_title = document.release_title();
        let resolved_password: Option<String> = password
            .map(str::to_string)
            .or_else(|| document.password().map(str::to_string));
        let password = resolved_password.as_deref();
        let files = document.files;
        if files.is_empty() {
            return Err(StreamerError::NoMediaFile);
        }

        // Fetched at most once regardless of how many of the checks below end
        // up needing it (PAR2 block verification, obfuscated-filename
        // recovery) — see `fetch_par2_blob`/`recover_filenames_from_par2`.
        // Only fetched at all when `verify_par2` opts in: unlike every other
        // ingest-time check, it's a real content fetch (the blob itself, plus
        // whatever `verify_volume_against_par2` samples per RAR volume below),
        // not just a STAT or a single 32 KB header probe.
        let par2_blob = if verify_par2 {
            self.fetch_par2_blob(&files).await
        } else {
            None
        };

        let (mut meta_files, file_password) = match self
            .try_build_rar_virtual_files(&files, password, par2_blob.as_deref())
            .await?
        {
            Some(virtual_files) if !virtual_files.is_empty() => {
                let is_encrypted = virtual_files.iter().any(|vf| {
                    matches!(
                        &vf.source,
                        NzbMetaSource::Rar { slices, .. }
                            if slices.iter().any(|s| s.encryption.is_some())
                    )
                });
                if let Some(NzbMetaSource::Rar { parts, .. }) =
                    virtual_files.first().map(|vf| &vf.source)
                {
                    let probe_segments: Vec<NzbSegment> = parts
                        .iter()
                        .flat_map(|p| p.segments.iter().cloned())
                        .collect();
                    self.probe_availability(&probe_segments, sample_percent)
                        .await?;
                }
                let mut out = virtual_files;
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
                    self.probe_availability(segments, sample_percent).await?;
                }
                (ordered, None)
            }
        };

        let obfuscated_playable: Vec<usize> = meta_files
            .iter()
            .enumerate()
            .filter(|(_, f)| is_media_filename(&f.filename) && looks_obfuscated(&f.filename))
            .map(|(i, _)| i)
            .collect();
        if !obfuscated_playable.is_empty()
            && let Some(par2_names) = self
                .recover_filenames_from_par2(&files, par2_blob.as_deref())
                .await
        {
            if par2_names.len() == obfuscated_playable.len() {
                for (slot, recovered) in obfuscated_playable.iter().zip(par2_names.iter()) {
                    let old = meta_files[*slot].filename.clone();
                    meta_files[*slot].filename = recovered.clone();
                    tracing::info!(
                        info_hash,
                        old = %old,
                        new = %recovered,
                        "renamed obfuscated file via par2 FileDesc"
                    );
                }
            } else {
                tracing::debug!(
                    info_hash,
                    par2_count = par2_names.len(),
                    obfuscated_count = obfuscated_playable.len(),
                    "par2 FileDesc count differs from obfuscated file count; skipping rename"
                );
            }
        }

        let playable_count = meta_files
            .iter()
            .filter(|f| is_media_filename(&f.filename))
            .count();
        if playable_count == 1
            && let Some(release_title) = release_title.as_deref()
            && let Some(file) = meta_files
                .iter_mut()
                .find(|f| is_media_filename(&f.filename))
            && looks_obfuscated(&file.filename)
        {
            let ext = file
                .filename
                .rsplit('.')
                .next()
                .filter(|s| !s.is_empty())
                .unwrap_or("mkv");
            let new_name = format!("{release_title}.{ext}");
            tracing::info!(
                info_hash,
                old = %file.filename,
                new = %new_name,
                "renamed obfuscated single-file NZB to release title"
            );
            file.filename = new_name;
        }

        let rescale_concurrency = self.prefetch_concurrency(self.pool.bulk_client().capacity());
        stream::iter(
            meta_files
                .iter_mut()
                .filter(|f| is_media_filename(&f.filename)),
        )
        .for_each_concurrent(rescale_concurrency, |file| async move {
            if let Err(error) = self.rescale_direct_to_decoded(file).await {
                tracing::debug!(
                    info_hash,
                    filename = %file.filename,
                    error = %error,
                    "could not rescale Direct offsets to decoded space; \
                     leaving as encoded approximation"
                );
            }
        })
        .await;

        let meta = NzbMeta {
            info_hash: info_hash.to_string(),
            files: meta_files,
            password: file_password,
        };

        store::store(&self.db, info_hash, &meta).await?;

        self.state
            .meta_cache
            .put(info_hash.to_string(), Arc::new(meta.clone()));

        Ok(meta)
    }

    /// If `files` contains one or more stored multi-volume RAR archives,
    /// build a virtual `NzbMetaFile` per inner media file across every set.
    ///
    /// NZB files are normalised by base name (`{base}.partNN.rar`,
    /// `{base}.rNN`) and each group is parsed as its own RAR set. A movie
    /// release yields one group → one or more inner files; a season pack with
    /// N episodes yields N groups, each contributing its episode's inner
    /// `.mkv`. Returns `Ok(None)` only when no group parses (so the caller
    /// falls back to exposing raw NZB files); a partial set — say 22 of 23
    /// episodes parse — returns the successful 22 and the persist layer's
    /// cascade picks up the missing one as a per-episode scrape.
    async fn try_build_rar_virtual_files(
        &self,
        files: &[NzbFile],
        password: Option<&str>,
        par2_blob: Option<&[u8]>,
    ) -> Result<Option<Vec<NzbMetaFile>>, StreamerError> {
        let groups = detect_rar_volume_groups(files);
        if groups.is_empty() {
            return Ok(None);
        }
        let par2_index = par2_blob.and_then(|blob| self.build_par2_index(blob));
        let mut all_virtual: Vec<NzbMetaFile> = Vec::new();
        let mut missing_password = false;
        for (group_idx, ordered_indices) in groups.iter().enumerate() {
            match self
                .build_rar_group_virtual_files(
                    files,
                    ordered_indices,
                    password,
                    par2_index.as_ref(),
                )
                .await
            {
                Ok(Some(mut group_files)) => all_virtual.append(&mut group_files),
                Ok(None) => {
                    tracing::debug!(
                        group_idx,
                        volumes = ordered_indices.len(),
                        "RAR group did not produce usable virtual files; skipping"
                    );
                }
                Err(StreamerError::MissingPassword) => {
                    missing_password = true;
                }
                Err(error) => return Err(error),
            }
        }
        if all_virtual.is_empty() {
            if missing_password {
                return Err(StreamerError::MissingPassword);
            }
            return Ok(None);
        }
        if missing_password {
            tracing::warn!(
                produced = all_virtual.len(),
                "some RAR groups in this NZB are encrypted and could not be ingested without a password"
            );
        }
        Ok(Some(all_virtual))
    }

    /// Per-group RAR ingest: walk one archive's volumes, parse headers,
    /// produce one `NzbMetaFile` per inner stored media file. Extracted from
    /// the original whole-NZB body so `try_build_rar_virtual_files` can fan
    /// out across multiple sets in a single NZB.
    async fn build_rar_group_virtual_files(
        &self,
        files: &[NzbFile],
        ordered_indices: &[usize],
        password: Option<&str>,
        par2_index: Option<&Par2Index>,
    ) -> Result<Option<Vec<NzbMetaFile>>, StreamerError> {
        if ordered_indices.is_empty() {
            return Ok(None);
        }

        let mut parts: Vec<NzbRarPart> = Vec::with_capacity(ordered_indices.len());
        for &nzb_idx in ordered_indices {
            let f = &files[nzb_idx];
            if f.segments.is_empty() {
                return Ok(None);
            }
            parts.push(build_rar_part(f));
        }

        let header_fetch_concurrency =
            self.prefetch_concurrency(self.pool.bulk_client().capacity());
        let streamer = self.clone();
        // Fetch every volume's header at bounded concurrency, but cancel the
        // rest of the group the instant one volume fails: a missing or
        // unreadable part dooms the whole RAR set (no par2 repair at ingest
        // time), so paying for sibling volumes' header fetches after that is
        // wasted work — mirrors altmount's per-group short-circuit in
        // `FastFailCheckFiles`, where one broken group member skips every
        // remaining Stat for that set. `buffer_unordered` (not `buffered`)
        // so a failure anywhere in the batch is seen as soon as it resolves,
        // not held back behind an earlier still-in-flight volume.
        let mut header_bytes: Vec<Option<Vec<u8>>> = (0..parts.len()).map(|_| None).collect();
        {
            let mut fetches = stream::iter(parts.iter().cloned().enumerate())
                .map(move |(vol_idx, part)| {
                    let s = streamer.clone();
                    async move {
                        let result = s
                            .fetch_volume_header_bytes(&part, RAR_HEADER_PROBE_BYTES)
                            .await;
                        (vol_idx, part.filename, result)
                    }
                })
                .buffer_unordered(header_fetch_concurrency);

            while let Some((vol_idx, filename, result)) = fetches.next().await {
                match result {
                    Ok(bytes) => header_bytes[vol_idx] = Some(bytes),
                    Err(error) => {
                        tracing::debug!(
                            vol_idx,
                            filename = %filename,
                            error = %error,
                            "RAR volume header fetch failed; treating as non-RAR NZB"
                        );
                        // Dropping `fetches` cancels the remaining in-flight
                        // sibling-volume fetches.
                        break;
                    }
                }
            }
        }
        if header_bytes.iter().any(Option::is_none) {
            return Ok(None);
        }
        let header_bytes: Vec<Vec<u8>> = header_bytes.into_iter().flatten().collect();

        let mut volume_entries: Vec<Vec<RarVolumeFileEntry>> = Vec::with_capacity(parts.len());
        let mut archive_format: Option<rar::RarFormat> = None;

        for (vol_idx, (part, header)) in parts.iter_mut().zip(header_bytes).enumerate() {
            if let Some(first_seg) = part.segments.first()
                && let Some(size) = self.state.decoded_sizes.get(&first_seg.message_id)
            {
                part.decoded_seg_size = Some(size);
            }
            let parsed = match rar::parse_volume_header(&header) {
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
            if archive_format.is_none() {
                archive_format = parsed.format;
            }
            let entries: Vec<RarVolumeFileEntry> = parsed
                .files
                .into_iter()
                .filter(super::super::rar::RarVolumeFileEntry::is_stored)
                .collect();
            if entries.is_empty() {
                tracing::debug!(vol_idx, "no stored file entry in this RAR volume; bailing");
                return Ok(None);
            }

            volume_entries.push(entries);
        }

        if let Some(par2) = par2_index {
            for part in &parts {
                let key = normalize_par2_name(&part.filename);
                let Some(blocks) = par2.blocks_by_filename.get(&key) else {
                    continue;
                };
                if blocks.is_empty() {
                    continue;
                }
                match self
                    .verify_volume_against_par2(part, blocks, par2.slice_size)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(
                            filename = %part.filename,
                            "RAR volume's segments don't match this release's own PAR2 \
                             checksums — the articles exist and parse as a valid RAR \
                             header, but the payload doesn't match the archive's own \
                             recovery set, meaning it's the wrong content rather than \
                             missing content; bailing on this NZB rather than serving it"
                        );
                        return Ok(None);
                    }
                    Err(error) => {
                        tracing::debug!(
                            filename = %part.filename,
                            error = %error,
                            "PAR2 block verification fetch failed; skipping the check \
                             for this volume rather than failing ingest on a transient \
                             provider error"
                        );
                    }
                }
            }
        }

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
                None => continue,
            };
            let Some(prev_last) = volume_entries[prev_vol].last() else {
                continue;
            };
            let prev_data_end = prev_last.data_offset.saturating_add(prev_last.packed_size);
            if g.encryption.is_some() {
                tracing::debug!(
                    name = %g.name,
                    "incomplete encrypted RAR inner file; synthesis skipped"
                );
                continue;
            }

            // `prev_data_end` lands on the start of the *next* file's RAR
            // header, not on its data. The header itself can be 100+ bytes
            // (filename string + flags + sizes), so naïvely treating
            // `prev_data_end` as `start_in_part` would have the streamer
            // serve those header bytes as the first bytes of the inner
            // file — which is exactly the off-by-N that broke ffprobe on
            // every season-pack episode after the first. Fetch a small
            // probe at `prev_data_end`, parse the block at that offset,
            // and skip past its header so `start_in_part` lands on the
            // file's real first byte.
            let part = &parts[prev_vol];
            let Some(format) = archive_format else {
                continue;
            };
            let probe_end = prev_data_end.saturating_add(1023);
            let client = self.pool.bulk_client();
            let probe = match self
                .read_decoded_range_within_part(part, prev_data_end, probe_end, &client)
                .await
            {
                Ok(p) => p,
                Err(error) => {
                    tracing::debug!(
                        name = %g.name,
                        prev_vol,
                        prev_data_end,
                        error = %error,
                        "synthesis probe fetch failed; skipping this inner file"
                    );
                    continue;
                }
            };
            let header_skip = match rar::block_layout_at(&probe, format) {
                Some((header_size, _data_size)) => header_size,
                None => {
                    tracing::warn!(
                        name = %g.name,
                        prev_vol,
                        prev_data_end,
                        probe_len = probe.len(),
                        "RAR block re-parse at synthesized start failed; \
                         skipping this inner file rather than emitting a \
                         slice that would serve header bytes as data"
                    );
                    continue;
                }
            };
            let synth_start = prev_data_end.saturating_add(header_skip);
            let synth_len = g.unpacked_size.saturating_sub(g.plaintext_sum);

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
                header_skip,
                "synthesized missing first slice for RAR inner file"
            );
            let g = groups.get_mut(name).expect("group exists");
            g.slices.insert(0, synth_slice);
            g.plaintext_sum = g.plaintext_sum.saturating_add(synth_len);
        }

        let mut out: Vec<NzbMetaFile> = Vec::new();
        for name in group_order {
            let Some(g) = groups.remove(&name) else {
                continue;
            };
            if g.plaintext_sum != g.unpacked_size {
                tracing::debug!(
                    name = %g.name,
                    plaintext_sum = g.plaintext_sum,
                    declared = g.unpacked_size,
                    "RAR inner file slices do not cover its declared size; skipping"
                );
                continue;
            }
            if let Some((prev_part, next_part)) = first_slice_gap(&g.slices) {
                // The byte-sum check above only proves the *lengths* add up to
                // `unpacked_size` — it can't prove the bytes are the right
                // ones. Two header-probes hitting real (non-synthesized)
                // FILE_HEAD blocks that skip one or more intervening volumes
                // means some other file's header was found sitting inside
                // that gap (observed in a Black Mirror S02 season-pack: the
                // header probe is front-of-volume only, so when a volume's
                // *true* first header is itself missed, a stale/adjacent
                // file's header can be misattributed and the sums still add
                // up by coincidence). A contiguous RAR file can never
                // legitimately skip a whole volume, so treat this as
                // unverifiable rather than serving corrupted bytes.
                tracing::warn!(
                    name = %g.name,
                    prev_part,
                    next_part,
                    "RAR inner file slices skip one or more volumes; \
                     reconstruction is unverifiable, skipping"
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
        let client = self.pool.bulk_client();
        for seg in &part.segments {
            let decoded = self.fetch_decoded_cached(&client, &seg.message_id).await?;
            buf.extend_from_slice(&decoded);
            if (buf.len() as u64) >= wanted {
                break;
            }
        }
        Ok(buf)
    }

    /// Fetch and parse this NZB's PAR2 index — the smallest `.par2` file,
    /// which (per the PAR2 spec) mirrors the Main/FileDesc/IFSC packets
    /// across every volume in the set; only `RecvSlic` recovery-slice
    /// packets scale with file size, and this doesn't need those. Returns
    /// `None` (verification simply skipped, not failed) when there's no
    /// PAR2 file, the fetch fails, or the blob is missing a Main or IFSC
    /// packet — PAR2 cover is optional, releases without it ingest exactly
    /// as before this check existed.
    /// Fetch the smallest `.par2` file's full decoded body — the one blob
    /// both PAR2 block verification and obfuscated-filename recovery parse.
    /// Shared so a release needing both never fetches it twice.
    async fn fetch_par2_blob(&self, files: &[NzbFile]) -> Option<Vec<u8>> {
        let smallest = files
            .iter()
            .filter(|f| looks_like_par2(&filename_from_subject(&f.subject)))
            .min_by_key(|f| f.segments.iter().map(|s| s.bytes).sum::<u64>())?;
        if smallest.segments.is_empty() {
            return None;
        }
        let total_bytes: u64 = smallest.segments.iter().map(|s| s.bytes).sum();
        let mut buf: Vec<u8> = Vec::with_capacity(total_bytes as usize);
        let client = self.pool.bulk_client();
        for seg in &smallest.segments {
            match self.fetch_decoded_cached(&client, &seg.message_id).await {
                Ok(decoded) => buf.extend_from_slice(&decoded),
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        message_id = %seg.message_id,
                        "par2 blob fetch failed"
                    );
                    return None;
                }
            }
        }
        Some(buf)
    }

    /// Parse a fetched PAR2 blob into the block-checksum index used to
    /// verify RAR volume content. Returns `None` when the blob is missing a
    /// Main or IFSC packet — PAR2 cover is optional, releases without usable
    /// IFSC data ingest exactly as before this check existed.
    fn build_par2_index(&self, blob: &[u8]) -> Option<Par2Index> {
        let slice_size = parse_slice_size(blob).ok().filter(|s| *s > 0)?;
        let filedescs = parse_file_descriptors(blob).ok()?;
        let ifsc = parse_ifsc_packets(blob).ok()?;

        let mut blocks_by_filename = std::collections::HashMap::new();
        for desc in filedescs {
            if let Some(blocks) = ifsc.get(&desc.file_id) {
                blocks_by_filename.insert(normalize_par2_name(&desc.filename), blocks.clone());
            }
        }
        if blocks_by_filename.is_empty() {
            return None;
        }
        Some(Par2Index {
            slice_size,
            blocks_by_filename,
        })
    }

    /// Verify a sample of one RAR volume's blocks against the release's own
    /// PAR2 checksums. `Ok(false)` means a sampled block's CRC32 doesn't
    /// match — the volume's segments contain the wrong content. `Err` means
    /// the fetch itself failed (treated leniently by the caller: skip the
    /// check, don't reject the release over a transient provider hiccup).
    async fn verify_volume_against_par2(
        &self,
        part: &NzbRarPart,
        blocks: &[Par2Block],
        slice_size: u64,
    ) -> Result<bool, StreamerError> {
        let client = self.pool.bulk_client();
        for idx in par2_sample_block_indices(blocks.len()) {
            let block = &blocks[idx];
            let start = idx as u64 * slice_size;
            let end_inclusive = start + slice_size - 1;
            let fetched = self
                .read_decoded_range_within_part(part, start, end_inclusive, &client)
                .await?;
            // PAR2 checksums the slice zero-padded to `slice_size`; a file's
            // final block is normally shorter than that on disk.
            let mut chunk = fetched.to_vec();
            chunk.resize(slice_size as usize, 0);
            if crc32fast::hash(&chunk) != block.crc32 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Replace a Direct file's encoded-byte offsets with decoded-byte
    /// offsets so the file we advertise to HTTP clients matches the bytes
    /// we actually serve. We fetch the first segment to learn its
    /// decoded/encoded ratio and then scale every segment in the file
    /// uniformly — yEnc encoding is uniform within a single posting (same
    /// poster settings, contiguous binary), so one sample is enough. Only
    /// applies to `NzbMetaSource::Direct`; `Rar` sources are skipped
    /// (their slice lengths are exact decoded byte counts from the RAR
    /// header).
    pub(super) async fn rescale_direct_to_decoded(
        &self,
        file: &mut NzbMetaFile,
    ) -> Result<(), StreamerError> {
        let NzbMetaSource::Direct { offsets, segments } = &mut file.source else {
            return Ok(());
        };
        let Some(first) = segments.first() else {
            return Ok(());
        };
        if first.bytes == 0 {
            return Ok(());
        }
        let client = self.pool.bulk_client();
        let dec_first = self
            .fetch_decoded_cached(&client, &first.message_id)
            .await?
            .len() as u64;
        if dec_first == 0 {
            return Ok(());
        }
        let n = segments.len();
        let dec_last = if n <= 1 {
            dec_first
        } else {
            let last = &segments[n - 1];
            let measured = if last.bytes == 0 {
                dec_first
            } else {
                self.fetch_decoded_cached(&client, &last.message_id)
                    .await?
                    .len() as u64
            };
            if measured == 0 { dec_first } else { measured }
        };
        let mut new_offsets = Vec::with_capacity(n + 1);
        let mut acc: u64 = 0;
        new_offsets.push(0);
        for i in 0..n {
            let seg_dec = if i + 1 == n { dec_last } else { dec_first };
            acc = acc.saturating_add(seg_dec);
            new_offsets.push(acc);
        }
        *offsets = new_offsets;
        file.total_size = acc;
        Ok(())
    }

    /// Parse the media filenames a PAR2 blob claims (one entry per FileDesc
    /// packet, NZB order). The caller pairs these against obfuscated virtual
    /// files to recover real names. Returns `None` if there's no PAR2 file,
    /// the fetch fails, the blob has no FileDesc packets, or every recovered
    /// name is itself obfuscated.
    ///
    /// `prefetched_blob` lets a caller that already fetched the NZB's PAR2
    /// blob this ingest (e.g. for block verification) hand it over instead
    /// of triggering a second fetch of the same file; `None` fetches it here
    /// via [`Self::fetch_par2_blob`], picking the smallest `.par2` — its
    /// index packet is always the smallest in a set, `*.volNN+NN.par2`
    /// slices duplicate the same FileDesc packets but are larger and carry
    /// unused recovery data.
    async fn recover_filenames_from_par2(
        &self,
        files: &[NzbFile],
        prefetched_blob: Option<&[u8]>,
    ) -> Option<Vec<String>> {
        let owned_buf;
        let buf = match prefetched_blob {
            Some(b) => b,
            None => {
                owned_buf = self.fetch_par2_blob(files).await?;
                &owned_buf
            }
        };
        let descs: Vec<Par2FileDesc> = match parse_file_descriptors(buf) {
            Ok(d) => d,
            Err(error) => {
                tracing::debug!(error = %error, "par2 FileDesc parse failed");
                return None;
            }
        };
        let media: Vec<String> = descs
            .into_iter()
            .filter(|d| is_media_filename(&d.filename))
            .map(|d| d.filename)
            .collect();
        if media.is_empty() {
            return None;
        }
        if media.iter().all(|n| looks_obfuscated(n)) {
            tracing::debug!("par2 FileDesc names are themselves obfuscated; skipping rename");
            return None;
        }
        Some(media)
    }
}

/// Cumulative byte offsets of a file's segments: `offsets[i]` is where
/// segment `i` starts; the final entry is the total size.
fn segment_offsets(segments: &[NzbSegment]) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(segments.len() + 1);
    let mut acc: u64 = 0;
    offsets.push(0);
    for seg in segments {
        acc += seg.bytes;
        offsets.push(acc);
    }
    offsets
}

pub(crate) fn direct_meta_file(f: &NzbFile) -> NzbMetaFile {
    let offsets = segment_offsets(&f.segments);
    NzbMetaFile {
        filename: filename_from_subject(&f.subject),
        total_size: *offsets.last().unwrap_or(&0),
        source: NzbMetaSource::Direct {
            offsets,
            segments: f.segments.clone(),
        },
    }
}

fn build_rar_part(f: &NzbFile) -> NzbRarPart {
    let offsets = segment_offsets(&f.segments);
    NzbRarPart {
        filename: filename_from_subject(&f.subject),
        total_size: *offsets.last().unwrap_or(&0),
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
