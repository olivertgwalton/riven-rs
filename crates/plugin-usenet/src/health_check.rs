//! Background article-availability re-verification.
//!

use std::time::Duration;

use anyhow::Result;
use redis::AsyncCommands;
use riven_core::settings::PluginSettings;
use riven_usenet::UsenetStreamer;
use riven_usenet::nntp::NntpClient;
use riven_usenet::nzb::NzbSegment;
use riven_usenet::streamer::{NzbMeta, NzbMetaSource};
use sea_orm::{DbBackend, FromQueryResult, Statement};
use serde::{Deserialize, Serialize};

use crate::PROVIDER;

const HEALTH_CHECK_TICK: Duration = Duration::from_secs(24 * 60 * 60);
const PER_ENTRY_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const FAILURE_BACKOFF: Duration = Duration::from_secs(2 * 60 * 60);
const BATCH_LIMIT: i64 = 100;

const VERIFY_FIRST_N: usize = 3;
const VERIFY_LAST_N: usize = 2;
const MIDDLE_SAMPLE: usize = 6;

/// ≥50% of the sample missing → mark the file dead for this pass. Combined
/// with `max_failures` so a single bad sample doesn't trigger a delete.
const FAILURE_THRESHOLD: f64 = 0.5;

const DEFAULT_MAX_FAILURES: u32 = 2;

#[derive(Default, Serialize, Deserialize)]
struct HcState {
    #[serde(default)]
    failures: u32,
}

pub fn spawn(
    redis: redis::aio::ConnectionManager,
    streamer: UsenetStreamer,
    settings: PluginSettings,
) {
    use std::sync::OnceLock;
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    let max_failures = settings
        .get_parsed_or::<u32>("healthcheckmaxfailures", DEFAULT_MAX_FAILURES)
        .max(1);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        loop {
            match run_once(&redis, &streamer, max_failures).await {
                Ok(summary) => {
                    if summary.checked > 0 {
                        tracing::info!(
                            checked = summary.checked,
                            removed = summary.removed,
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
}

/// One candidate usenet media entry: its filesystem id, the joined stream's
/// info_hash (empty when no stream row), and the owning media item.
#[derive(FromQueryResult)]
struct HealthCheckRow {
    id: i64,
    info_hash: String,
    media_item_id: i64,
}

async fn run_once(
    redis_seed: &redis::aio::ConnectionManager,
    streamer: &UsenetStreamer,
    max_failures: u32,
) -> Result<PassSummary> {
    let rows = HealthCheckRow::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT fe.id, COALESCE(s.info_hash, '') AS info_hash, fe.media_item_id \
         FROM filesystem_entries fe \
         LEFT JOIN streams s ON s.id = fe.stream_id \
         WHERE fe.plugin = $1 AND fe.entry_type::text = 'media' \
         ORDER BY fe.updated_at NULLS FIRST, fe.id \
         LIMIT $2",
        [PROVIDER.into(), BATCH_LIMIT.into()],
    ))
    .all(riven_db::orm())
    .await?;

    let mut summary = PassSummary::default();
    let mut redis = redis_seed.clone();

    for HealthCheckRow {
        id: entry_id,
        info_hash,
        media_item_id,
    } in rows
    {
        if info_hash.is_empty() {
            continue;
        }
        let key = format!("riven:usenet:hc:{entry_id}");
        let ttl: i64 = AsyncCommands::ttl(&mut redis, &key).await.unwrap_or(-2);
        if ttl > 0 {
            continue;
        }
        let prior: HcState = match AsyncCommands::get::<_, Option<String>>(&mut redis, &key)
            .await
            .ok()
            .flatten()
        {
            Some(s) => serde_json::from_str(&s).unwrap_or_default(),
            None => HcState::default(),
        };

        let meta = match streamer.load_meta(&info_hash).await {
            Ok(m) => m,
            Err(error) => {
                tracing::debug!(entry_id, %error, "health check: meta load failed; bumping cooldown");
                set_state(&mut redis, &key, &prior, PER_ENTRY_INTERVAL).await;
                continue;
            }
        };

        let segments = sample_segments(&meta);
        if segments.is_empty() {
            set_state(&mut redis, &key, &prior, PER_ENTRY_INTERVAL).await;
            continue;
        }
        summary.checked += 1;
        let pool = streamer.pool();
        let client = pool.bulk_client();
        let (alive, total) = stat_sample(&client, &segments).await;
        if total == 0 {
            set_state(&mut redis, &key, &prior, FAILURE_BACKOFF).await;
            continue;
        }
        let miss_rate = (total - alive) as f64 / total as f64;
        let healthy = miss_rate < FAILURE_THRESHOLD;

        if healthy {
            set_state(
                &mut redis,
                &key,
                &HcState { failures: 0 },
                PER_ENTRY_INTERVAL,
            )
            .await;
            continue;
        }

        let new_failures = prior.failures.saturating_add(1);
        if new_failures >= max_failures {
            tracing::warn!(
                entry_id,
                media_item_id,
                info_hash = %info_hash,
                alive,
                total,
                consecutive_failures = new_failures,
                "usenet health check: failure threshold breached; removing entry"
            );
            if let Err(error) =
                riven_db::repo::blacklist_stream_by_hash(media_item_id, &info_hash).await
            {
                tracing::warn!(entry_id, info_hash, %error, "health check: failed to blacklist stream");
            }
            match riven_db::repo::delete_filesystem_entry(entry_id).await {
                Ok((true, _)) => summary.removed += 1,
                Ok((false, _)) => {}
                Err(error) => {
                    tracing::warn!(entry_id, %error, "health check: failed to delete entry");
                }
            }
            let _del: redis::RedisResult<()> = AsyncCommands::del(&mut redis, &key).await;
        } else {
            tracing::debug!(
                entry_id,
                alive,
                total,
                consecutive_failures = new_failures,
                "usenet health check: bad sample, will retry"
            );
            set_state(
                &mut redis,
                &key,
                &HcState {
                    failures: new_failures,
                },
                FAILURE_BACKOFF,
            )
            .await;
        }
    }

    Ok(summary)
}

async fn set_state(
    redis: &mut redis::aio::ConnectionManager,
    key: &str,
    state: &HcState,
    ttl: Duration,
) {
    let value = serde_json::to_string(state).unwrap_or_else(|_| "{}".to_string());
    let _set: redis::RedisResult<()> =
        AsyncCommands::set_ex(redis, key, value, ttl.as_secs()).await;
}

/// First N + last M + a handful of evenly-spaced middle segments. Inspired
/// by altmount's strategic sample: the head catches DMCA takedowns that
/// start at the release's first segment, the tail catches truncated
/// uploads, and the middle catches generic retention loss.
fn sample_segments(meta: &NzbMeta) -> Vec<NzbSegment> {
    let Some(first) = meta.files.first() else {
        return Vec::new();
    };
    let all: Vec<NzbSegment> = match &first.source {
        NzbMetaSource::Direct { segments, .. } => segments.clone(),
        NzbMetaSource::Rar { parts, .. } => parts
            .iter()
            .flat_map(|p| p.segments.iter().cloned())
            .collect(),
    };
    if all.is_empty() {
        return Vec::new();
    }
    let total = all.len();
    if total <= VERIFY_FIRST_N + VERIFY_LAST_N + MIDDLE_SAMPLE {
        return all;
    }

    let mut indices: Vec<usize> = (0..VERIFY_FIRST_N).collect();
    indices.extend((total - VERIFY_LAST_N)..total);

    let middle_start = VERIFY_FIRST_N;
    let middle_end = total - VERIFY_LAST_N;
    let middle_count = middle_end - middle_start;
    for i in 0..MIDDLE_SAMPLE {
        let idx = middle_start + ((2 * i + 1) * middle_count) / (2 * MIDDLE_SAMPLE);
        if idx < middle_end {
            indices.push(idx);
        }
    }
    indices.sort_unstable();
    indices.dedup();
    indices.into_iter().map(|i| all[i].clone()).collect()
}

async fn stat_sample(client: &NntpClient, segments: &[NzbSegment]) -> (usize, usize) {
    let mut alive = 0usize;
    let mut total = 0usize;
    for seg in segments {
        total += 1;
        match client.stat(&seg.message_id).await {
            Ok(true) => alive += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::debug!(message_id = %seg.message_id, %error, "health check STAT errored; treating as unknown");
                return (0, 0);
            }
        }
    }
    (alive, total)
}
