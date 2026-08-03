//! One-shot in-place migration that fills `decoded_seg_size` on RAR parts
//! ingested before that field existed. Runs as a background task on first
//! load; `MigratedMetas` ensures it fires at most once per info_hash per
//! process.

use std::sync::Arc;

use super::meta::{NzbMeta, NzbMetaSource};
use super::store;
use super::{StreamerError, UsenetStreamer};

/// Probes written to one connection before its replies are read.
///
/// This used to be a fan-out of 8 concurrent single-article fetches, which is 8
/// connection slots held by a background migration — the same slots playback
/// competes for. One connection per batch costs the migration wall-clock time
/// it has no deadline for, and gives those slots back.
const BACKFILL_BATCH: usize = 8;
/// Articles fetched at once for whatever a batch's pipelined pass did not
/// resolve. Deliberately below [`BACKFILL_BATCH`]: the fallback path is the one
/// that takes a connection per article, so it should not reach the footprint
/// this change exists to remove.
const BACKFILL_FALLBACK_CONCURRENCY: usize = 2;

impl UsenetStreamer {
    pub(super) fn maybe_kick_backfill(&self, meta: &Arc<NzbMeta>) {
        let needs = meta.files.iter().any(|f| match &f.source {
            NzbMetaSource::Rar { parts, .. } => parts.iter().any(|p| p.decoded_seg_size.is_none()),
            _ => false,
        });
        if !needs {
            return;
        }
        if !self.state.migrated.claim(&meta.info_hash) {
            return;
        }
        let streamer = self.clone();
        let info_hash = meta.info_hash.clone();
        let release = meta.label().to_string();
        tokio::spawn(async move {
            if let Err(e) = streamer.backfill_decoded_seg_sizes(&info_hash).await {
                tracing::warn!(info_hash, release, error = %e, "decoded_seg_size backfill failed");
            }
        });
    }

    async fn backfill_decoded_seg_sizes(&self, info_hash: &str) -> Result<(), StreamerError> {
        let arc = self.load_meta_raw(info_hash).await?;
        let mut meta = (*arc).clone();
        let started = std::time::Instant::now();

        // The probe carries the volume's own filename: each probe is a real
        // article fetch, and its failure logs deep in the fetch path have no
        // other way to say which volume of which release went bad.
        // Per *volume set*, not per file. A season pack's episodes all share
        // one set, so walking files probed the same volume once per episode —
        // twenty real article fetches for one answer on the worst release here.
        let mut to_probe: Vec<(usize, usize, String, String)> = Vec::new();
        for (si, set) in meta.rar_sets.iter().enumerate() {
            for (pi, p) in set.iter().enumerate() {
                if p.decoded_seg_size.is_none()
                    && let Some(seg) = p.segments.first()
                {
                    to_probe.push((si, pi, seg.message_id.to_string(), p.filename.clone()));
                }
            }
        }
        if to_probe.is_empty() {
            return Ok(());
        }
        let total = to_probe.len();

        let mut filled = 0usize;
        let mut sized: Vec<(usize, usize, u64)> = Vec::new();
        for batch in to_probe.chunks(BACKFILL_BATCH) {
            let ids: Vec<String> = batch.iter().map(|(_, _, mid, _)| mid.clone()).collect();
            let probed = self
                .pool
                .fetch_batch(&ids, BACKFILL_FALLBACK_CONCURRENCY)
                .await;

            for ((si, pi, _, name), result) in batch.iter().zip(probed) {
                let (si, pi) = (*si, *pi);
                match result {
                    Ok(bytes) if !bytes.is_empty() => {
                        sized.push((si, pi, bytes.len() as u64));
                        filled += 1;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::debug!(
                            info_hash,
                            volume = %name,
                            si,
                            pi,
                            error = %e,
                            "backfill probe failed"
                        );
                    }
                }
            }
        }

        if filled == 0 {
            return Ok(());
        }

        // Apply to the sets, then re-point the files that shared each old
        // allocation at the new one so the sharing survives the update.
        let mut by_set: std::collections::BTreeMap<usize, Vec<(usize, u64)>> =
            std::collections::BTreeMap::new();
        for (si, pi, size) in sized {
            by_set.entry(si).or_default().push((pi, size));
        }
        for (si, updates) in by_set {
            let Some(old) = meta.rar_sets.get(si).cloned() else {
                continue;
            };
            let mut parts = (*old).clone();
            for (pi, size) in updates {
                if let Some(part) = parts.get_mut(pi) {
                    part.decoded_seg_size = Some(size);
                }
            }
            let new = Arc::new(parts);
            for f in &mut meta.files {
                if let NzbMetaSource::Rar { parts, .. } = &mut f.source
                    && Arc::ptr_eq(parts, &old)
                {
                    *parts = Arc::clone(&new);
                }
            }
            meta.rar_sets[si] = new;
        }

        store::store(&self.db, info_hash, &meta).await?;
        let release = meta.label().to_string();
        let arc = Arc::new(meta);
        crate::state::cache_meta(&self.state.meta_cache, info_hash.to_string(), arc);

        tracing::info!(
            info_hash,
            release,
            filled,
            total,
            elapsed_ms = started.elapsed().as_millis(),
            "decoded_seg_size backfill complete"
        );
        Ok(())
    }

    async fn load_meta_raw(&self, info_hash: &str) -> Result<Arc<NzbMeta>, StreamerError> {
        if let Some(hit) = self.state.meta_cache.get(info_hash) {
            return Ok(hit);
        }
        let meta = store::load(&self.db, info_hash)
            .await?
            .ok_or_else(|| StreamerError::NotIngested(info_hash.to_string()))?;
        Ok(Arc::new(meta))
    }
}
