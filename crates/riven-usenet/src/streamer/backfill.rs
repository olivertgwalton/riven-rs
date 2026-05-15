//! One-shot in-place migration that fills `decoded_seg_size` on RAR parts
//! ingested before that field existed. Runs as a background task on first
//! load; `MigratedMetas` ensures it fires at most once per info_hash per
//! process.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream;
use redis::AsyncCommands;

use super::meta::{META_TTL_SECS, NzbMeta, NzbMetaSource, io_error, meta_key};
use super::{StreamerError, UsenetStreamer};

const BACKFILL_CONCURRENCY: usize = 8;

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
        tokio::spawn(async move {
            if let Err(e) = streamer.backfill_decoded_seg_sizes(&info_hash).await {
                tracing::warn!(info_hash, error = %e, "decoded_seg_size backfill failed");
            }
        });
    }

    async fn backfill_decoded_seg_sizes(&self, info_hash: &str) -> Result<(), StreamerError> {
        let arc = self.load_meta_raw(info_hash).await?;
        let mut meta = (*arc).clone();
        let started = std::time::Instant::now();

        let mut to_probe: Vec<(usize, usize, String)> = Vec::new();
        for (fi, f) in meta.files.iter().enumerate() {
            if let NzbMetaSource::Rar { parts, .. } = &f.source {
                for (pi, p) in parts.iter().enumerate() {
                    if p.decoded_seg_size.is_none()
                        && let Some(seg) = p.segments.first()
                    {
                        to_probe.push((fi, pi, seg.message_id.clone()));
                    }
                }
            }
        }
        if to_probe.is_empty() {
            return Ok(());
        }
        let total = to_probe.len();

        let streamer = self.clone();
        let mut probes = stream::iter(to_probe)
            .map(move |(fi, pi, mid)| {
                let s = streamer.clone();
                async move {
                    let r = s.fetch_decoded_cached(&mid).await.map(|arc| arc.len() as u64);
                    (fi, pi, r)
                }
            })
            .buffer_unordered(BACKFILL_CONCURRENCY);

        let mut filled = 0usize;
        while let Some((fi, pi, result)) = probes.next().await {
            match result {
                Ok(size) if size > 0 => {
                    if let NzbMetaSource::Rar { parts, .. } = &mut meta.files[fi].source {
                        parts[pi].decoded_seg_size = Some(size);
                        filled += 1;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!(info_hash, fi, pi, error = %e, "backfill probe failed");
                }
            }
        }

        if filled == 0 {
            return Ok(());
        }

        let json = serde_json::to_string(&meta).map_err(|e| {
            StreamerError::Redis(redis::RedisError::from(io_error(e.to_string())))
        })?;
        let mut redis = self.redis.clone();
        let _: () = redis::AsyncCommands::set_ex(
            &mut redis,
            meta_key(info_hash),
            json,
            META_TTL_SECS as u64,
        )
        .await?;
        let arc = Arc::new(meta);
        self.state.meta_cache.put(info_hash.to_string(), arc);

        tracing::info!(
            info_hash,
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
        let mut redis = self.redis.clone();
        let raw: Option<String> =
            AsyncCommands::get(&mut redis, meta_key(info_hash)).await?;
        let raw = raw.ok_or_else(|| StreamerError::NotIngested(info_hash.to_string()))?;
        let meta: NzbMeta = serde_json::from_str(&raw)
            .map_err(|e| StreamerError::Redis(redis::RedisError::from(io_error(e.to_string()))))?;
        Ok(Arc::new(meta))
    }
}
