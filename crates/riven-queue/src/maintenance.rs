use std::collections::HashSet;

use apalis_redis::RedisConfig;
use chrono::Utc;

/// Queue names used by apalis-redis (must match `RedisConfig::new` calls in `JobQueue`).
pub(crate) const QUEUE_NAMES: &[&str] = &[
    "riven:index",
    "riven:index-plugin",
    "riven:scrape",
    "riven:scrape-plugin",
    "riven:parse",
    "riven:download",
    "riven:content",
];

const APALIS_WORKERS_METADATA_PREFIX: &str = "core::apalis::workers:metadata::";

const COMPLETED_JOB_MAX_AGE_SECS: i64 = 60 * 60 * 6;
const FAILED_JOB_MAX_AGE_SECS: i64 = 60 * 60 * 24;
const COMPLETED_JOB_MAX_COUNT: isize = 500;
const FAILED_JOB_MAX_COUNT: isize = 5_000;

/// Re-enqueue all inflight jobs and clear all worker registrations (called at startup).
pub async fn clear_worker_registrations(redis: &mut redis::aio::ConnectionManager) {
    rescue_workers(redis, None).await;
}

/// Re-enqueue inflight jobs from workers whose heartbeat is older than `stale_threshold_secs`.
pub async fn recover_stale_workers(
    redis: &mut redis::aio::ConnectionManager,
    stale_threshold_secs: i64,
) {
    rescue_workers(redis, Some(Utc::now().timestamp() - stale_threshold_secs)).await;
}

async fn rescue_workers(redis: &mut redis::aio::ConnectionManager, max_score: Option<i64>) {
    for &queue_name in QUEUE_NAMES {
        let config = RedisConfig::new(queue_name);

        let members: Vec<String> = match max_score {
            None => redis::cmd("ZRANGE")
                .arg(config.workers_set())
                .arg(0i64)
                .arg(-1i64)
                .query_async(redis)
                .await
                .unwrap_or_default(),
            Some(cutoff) => redis::cmd("ZRANGEBYSCORE")
                .arg(config.workers_set())
                .arg(0i64)
                .arg(cutoff)
                .query_async(redis)
                .await
                .unwrap_or_default(),
        };

        if members.is_empty() {
            continue;
        }

        // Fetch all worker job sets in one pipelined round-trip.
        let all_job_sets: Vec<Vec<String>> = {
            let mut pipe = redis::pipe();
            for key in &members {
                pipe.cmd("SMEMBERS").arg(key);
            }
            pipe.query_async(redis).await.unwrap_or_default()
        };

        // Collect rescued jobs and pipeline all DEL commands together.
        let mut rescued: Vec<String> = Vec::new();
        let mut del_pipe = redis::pipe();
        for (key, jobs) in members.iter().zip(all_job_sets) {
            rescued.extend(jobs);
            del_pipe
                .del(format!("{APALIS_WORKERS_METADATA_PREFIX}{key}"))
                .del(key);
        }
        let _: Result<(), _> = del_pipe.query_async(redis).await;

        if !rescued.is_empty() {
            let _: Result<(), _> = redis::pipe()
                .rpush(config.active_jobs_list(), &rescued)
                .del(config.signal_list())
                .lpush(config.signal_list(), 1u8)
                .query_async(redis)
                .await;
            tracing::info!(
                queue = queue_name,
                count = rescued.len(),
                "re-enqueued inflight jobs from stale workers"
            );
        }

        if max_score.is_none() {
            let _: Result<(), _> = redis::cmd("DEL")
                .arg(config.workers_set())
                .query_async(redis)
                .await;
        } else {
            // Remove all stale worker entries in a single ZREM varargs call.
            let _: Result<(), _> = redis::cmd("ZREM")
                .arg(config.workers_set())
                .arg(&members)
                .query_async(redis)
                .await;
        }

        tracing::info!(
            queue = queue_name,
            count = members.len(),
            "cleared stale worker registrations"
        );
    }
}

pub async fn prune_queue_history(redis: &mut redis::aio::ConnectionManager) {
    for &queue in QUEUE_NAMES {
        let config = RedisConfig::new(queue);
        let data = config.job_data_hash();
        let meta = config.job_meta_hash();
        let done = prune_set(
            redis,
            &config.done_jobs_set(),
            &data,
            &meta,
            COMPLETED_JOB_MAX_AGE_SECS,
            COMPLETED_JOB_MAX_COUNT,
        )
        .await;
        let failed = prune_set(
            redis,
            &config.failed_jobs_set(),
            &data,
            &meta,
            FAILED_JOB_MAX_AGE_SECS,
            FAILED_JOB_MAX_COUNT,
        )
        .await;
        let dead = prune_set(
            redis,
            &config.dead_jobs_set(),
            &data,
            &meta,
            FAILED_JOB_MAX_AGE_SECS,
            FAILED_JOB_MAX_COUNT,
        )
        .await;
        if done + failed + dead > 0 {
            tracing::info!(queue, done, failed, dead, "pruned redis job history");
        }
    }
}

async fn prune_set(
    redis: &mut redis::aio::ConnectionManager,
    set_key: &str,
    job_data_hash: &str,
    job_meta_hash: &str,
    max_age_secs: i64,
    max_count: isize,
) -> usize {
    let cutoff = Utc::now().timestamp() - max_age_secs;
    let mut ids: HashSet<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(set_key)
        .arg("-inf")
        .arg(cutoff)
        .query_async::<Vec<String>>(redis)
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();

    let total: isize = redis::cmd("ZCARD")
        .arg(set_key)
        .query_async(redis)
        .await
        .unwrap_or(0);
    let overflow = total.saturating_sub(max_count);
    if overflow > 0 {
        let extra: Vec<String> = redis::cmd("ZRANGE")
            .arg(set_key)
            .arg(0)
            .arg(overflow - 1)
            .query_async(redis)
            .await
            .unwrap_or_default();
        ids.extend(extra);
    }

    if ids.is_empty() {
        return 0;
    }

    let ids: Vec<String> = ids.into_iter().collect();
    let meta_keys: Vec<String> = ids
        .iter()
        .map(|id| format!("{job_meta_hash}:{id}"))
        .collect();
    let _: Result<(), _> = redis::pipe()
        .atomic()
        .zrem(set_key, &ids)
        .hdel(job_data_hash, &ids)
        .del(meta_keys)
        .query_async(redis)
        .await;

    ids.len()
}
