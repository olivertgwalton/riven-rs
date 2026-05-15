//! Background article-availability re-verification.
//!
//! Usenet articles age out of provider retention. A media item we ingested
//! months ago may today be a phantom: the VFS still advertises it, the player
//! still tries to open it, and only when the first NNTP `BODY` fails does
//! anything notice. This task periodically STATs a sample of articles for
//! each completed usenet-streamed file; when the majority of the sample is
//! gone, the filesystem entry is removed and the stream is blacklisted, so
//! the state recompute flips the item back to `Indexed` and re-scrapes.
//!
//! Cadence is tracked via a Redis key per entry (`riven:usenet:hc:{id}`) with
//! the recheck interval as TTL — presence = recently checked, absence = due.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use redis::AsyncCommands;
use riven_core::http::HttpClient;
use riven_core::settings::PluginSettings;
use riven_usenet::nntp::NntpPool;
use riven_usenet::nzb::NzbSegment;
use riven_usenet::streamer::{NzbMeta, NzbMetaSource};
use sqlx::PgPool;

use crate::{PROVIDER, availnzb, nzb_url_redis_key};

const HEALTH_CHECK_TICK: Duration = Duration::from_secs(24 * 60 * 60);
const PER_ENTRY_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const HEALTH_CHECK_SAMPLE: usize = 4;

// ≥2 of 4 sampled segments missing → mark the file dead. Avoids over-acting
// on a single transient miss.
const FAILURE_THRESHOLD: f64 = 0.5;

const BATCH_LIMIT: i64 = 100;

pub fn spawn(
    db_pool: PgPool,
    redis: redis::aio::ConnectionManager,
    pool: Arc<NntpPool>,
    http: HttpClient,
    settings: PluginSettings,
) {
    use std::sync::OnceLock;
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        loop {
            match run_once(&db_pool, &redis, &pool, &http, &settings).await {
                Ok(summary) => {
                    if summary.checked > 0 {
                        tracing::info!(
                            checked = summary.checked,
                            removed = summary.removed,
                            skipped_no_meta = summary.skipped_no_meta,
                            "usenet health check pass complete"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "usenet health check pass failed");
                }
            }
            tokio::time::sleep(HEALTH_CHECK_TICK).await;
        }
    });
}

#[derive(Default)]
struct PassSummary {
    checked: usize,
    removed: usize,
    skipped_no_meta: usize,
}

async fn run_once(
    db_pool: &PgPool,
    redis_seed: &redis::aio::ConnectionManager,
    pool: &Arc<NntpPool>,
    http: &HttpClient,
    settings: &PluginSettings,
) -> Result<PassSummary> {
    let rows: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT fe.id, COALESCE(s.info_hash, '') AS info_hash, fe.media_item_id \
         FROM filesystem_entries fe \
         LEFT JOIN streams s ON s.id = fe.stream_id \
         WHERE fe.plugin = $1 AND fe.entry_type = 'media' \
         ORDER BY fe.updated_at NULLS FIRST, fe.id \
         LIMIT $2",
    )
    .bind(PROVIDER)
    .bind(BATCH_LIMIT)
    .fetch_all(db_pool)
    .await?;

    let mut summary = PassSummary::default();
    let mut redis = redis_seed.clone();

    for (entry_id, info_hash, media_item_id) in rows {
        if info_hash.is_empty() {
            continue;
        }
        let cooldown_key = format!("riven:usenet:hc:{entry_id}");
        let already_checked: bool = AsyncCommands::exists(&mut redis, &cooldown_key)
            .await
            .unwrap_or(false);
        if already_checked {
            continue;
        }

        // No cached meta means Redis TTL expired; we can't verify segments,
        // but bump the cooldown anyway so we don't re-attempt every tick.
        let meta_json: Option<String> =
            AsyncCommands::get(&mut redis, format!("riven:nzb:meta:{info_hash}"))
                .await
                .unwrap_or(None);
        let Some(meta_json) = meta_json else {
            summary.skipped_no_meta += 1;
            let _: redis::RedisResult<()> = AsyncCommands::set_ex(
                &mut redis,
                &cooldown_key,
                "no-meta",
                PER_ENTRY_INTERVAL.as_secs(),
            )
            .await;
            continue;
        };
        let meta: NzbMeta = match serde_json::from_str(&meta_json) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let segments = sample_segments(&meta);
        if segments.is_empty() {
            continue;
        }
        summary.checked += 1;
        let (alive, total) = stat_sample(pool, &segments).await;
        let missing = total.saturating_sub(alive);
        let miss_rate = missing as f64 / total.max(1) as f64;
        // Only feed AvailNZB when stat_sample produced a real verdict;
        // total == 0 means the pool errored and we have no signal to report.
        let report_signal = (total > 0).then(|| miss_rate < FAILURE_THRESHOLD);
        let nzb_url: Option<String> =
            AsyncCommands::get(&mut redis, nzb_url_redis_key(&info_hash))
                .await
                .unwrap_or(None);
        let release_name = meta.files.first().map(|f| f.filename.clone());
        if miss_rate >= FAILURE_THRESHOLD {
            tracing::warn!(
                entry_id,
                media_item_id,
                info_hash = %info_hash,
                alive,
                total,
                "usenet health check: majority articles missing; removing entry"
            );
            if let Err(error) =
                riven_db::repo::blacklist_stream_by_hash(db_pool, media_item_id, &info_hash).await
            {
                tracing::warn!(entry_id, info_hash, %error, "health check: failed to blacklist stream");
            }
            match riven_db::repo::delete_filesystem_entry(db_pool, entry_id).await {
                Ok((true, _)) => summary.removed += 1,
                Ok((false, _)) => {}
                Err(error) => {
                    tracing::warn!(entry_id, %error, "health check: failed to delete entry");
                }
            }
            let _: redis::RedisResult<()> = AsyncCommands::set_ex(
                &mut redis,
                &cooldown_key,
                "removed",
                PER_ENTRY_INTERVAL.as_secs(),
            )
            .await;
            if let (Some(url), Some(false)) = (nzb_url.as_ref(), report_signal) {
                availnzb::spawn_report_if_configured(
                    http.clone(),
                    settings,
                    url.clone(),
                    false,
                    release_name.clone(),
                );
            }
        } else {
            let _: redis::RedisResult<()> = AsyncCommands::set_ex(
                &mut redis,
                &cooldown_key,
                "ok",
                PER_ENTRY_INTERVAL.as_secs(),
            )
            .await;
            if let (Some(url), Some(true)) = (nzb_url.as_ref(), report_signal) {
                availnzb::spawn_report_if_configured(
                    http.clone(),
                    settings,
                    url.clone(),
                    true,
                    release_name.clone(),
                );
            }
        }
    }

    Ok(summary)
}

// Spread-spaced sample so a release that lost only its tail is still caught.
fn sample_segments(meta: &NzbMeta) -> Vec<NzbSegment> {
    let Some(first) = meta.files.first() else {
        return Vec::new();
    };
    let all_segments: Vec<NzbSegment> = match &first.source {
        NzbMetaSource::Direct { segments, .. } => segments.clone(),
        NzbMetaSource::Rar { parts, .. } => parts
            .iter()
            .flat_map(|p| p.segments.iter().cloned())
            .collect(),
    };
    if all_segments.len() <= HEALTH_CHECK_SAMPLE {
        return all_segments;
    }
    let step = all_segments.len() / HEALTH_CHECK_SAMPLE;
    (0..HEALTH_CHECK_SAMPLE)
        .map(|i| all_segments[(i * step).min(all_segments.len() - 1)].clone())
        .collect()
}

async fn stat_sample(pool: &NntpPool, segments: &[NzbSegment]) -> (usize, usize) {
    let mut alive = 0usize;
    let mut total = 0usize;
    for seg in segments {
        total += 1;
        match pool.stat(&seg.message_id).await {
            Ok(true) => alive += 1,
            Ok(false) => {}
            Err(error) => {
                // Treat a hard error as unknown rather than missing; bail so
                // a transient pool hiccup doesn't nuke healthy entries.
                tracing::debug!(message_id = %seg.message_id, %error, "health check STAT errored; treating as unknown");
                return (0, 0);
            }
        }
    }
    (alive, total)
}
