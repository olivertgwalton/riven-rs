use std::collections::HashSet;

use apalis_redis::Config;
use chrono::Utc;
use riven_core::settings::{FilesystemSettings, LibraryProfileMembership};
use riven_db::repo;

/// Recompute each media filesystem entry's stored library-profile membership
/// against `settings` and persist the rows whose membership changed. Returns the
/// number of rows updated.
///
/// This is the single source of truth for "which custom library profiles claim
/// this entry": it runs when filesystem settings change (a profile added,
/// removed, or its filter rules edited) and once at startup so an install whose
/// membership drifted from the current rules self-heals. Only diffs are written,
/// so a steady-state call updates nothing.
pub async fn reconcile_library_profiles(settings: &FilesystemSettings) -> anyhow::Result<u64> {
    let candidates = repo::list_filesystem_profile_entry_candidates().await?;
    let updates = candidates
        .into_iter()
        .filter_map(|candidate| {
            let next = settings.matching_profile_keys(
                &candidate.filesystem_metadata(),
                candidate.filesystem_content_type(),
            );
            let current = LibraryProfileMembership::from_json(candidate.library_profiles.as_ref());
            (next != current).then(|| (candidate.id, next.into_json()))
        })
        .collect::<Vec<_>>();

    repo::update_library_profiles_batch(&updates).await
}

/// Move a Redis that apalis-redis 1.0.0-rc.8 wrote to 1.0's layout, once.
/// rc.8 registered workers under their inflight set's name and kept history in
/// sorted sets under the keys 1.0 writes plain sets to: left as they are, the
/// claims are never reaped and every acknowledgement fails with `WRONGTYPE`.
/// Must run before any worker starts.
pub async fn migrate_from_apalis_rc(
    redis: &mut redis::aio::ConnectionManager,
) -> redis::RedisResult<()> {
    let (queues, kept, requeued, dropped): (u64, u64, u64, u64) =
        redis::Script::new(include_str!("apalis_rc_migration.lua"))
            .invoke_async(redis)
            .await?;
    if queues > 0 {
        tracing::info!(
            queues,
            kept,
            requeued,
            dropped,
            "migrated job queues to apalis 1.0"
        );
    }
    Ok(())
}

const QUEUE_REGISTRY_KEY: &str = "core:apalis:queues:list";

/// Drop the worker registrations of queues this process no longer runs (a
/// plugin disabled, a queue renamed). apalis only ever reaps a dead worker from
/// a live worker on the same queue, so a queue with no live workers left would
/// keep its stale workers on the dashboard forever.
pub async fn purge_orphaned_worker_sets(
    redis: &mut redis::aio::ConnectionManager,
    live_queues: &[String],
) {
    let live: HashSet<String> = live_queues
        .iter()
        .map(|q| Config::default().queue(q).workers_set())
        .collect();

    let registered: Vec<String> = match redis::cmd("ZRANGE")
        .arg(QUEUE_REGISTRY_KEY)
        .arg(0i64)
        .arg(-1i64)
        .query_async(redis)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "purge_orphaned_worker_sets: ZRANGE failed");
            return;
        }
    };

    let mut removed_queues: Vec<String> = Vec::new();
    for workers_set in registered {
        if live.contains(&workers_set) {
            continue;
        }
        let Some(queue) = workers_set.strip_suffix(":workers") else {
            continue;
        };
        let config = Config::default().queue(queue);
        let workers: Vec<String> = redis::cmd("ZRANGE")
            .arg(&workers_set)
            .arg(0i64)
            .arg(-1i64)
            .query_async(redis)
            .await
            .unwrap_or_default();
        let mut pipe = redis::pipe();
        for worker in &workers {
            pipe.del(format!("{queue}:workers:metadata:{worker}"));
            pipe.del(format!("{}:{worker}", config.inflight_jobs_set()));
        }
        pipe.del(&workers_set);
        pipe.zrem(QUEUE_REGISTRY_KEY, &workers_set);
        let _result: Result<(), _> = pipe.query_async(redis).await;
        removed_queues.push(workers_set);
    }

    if !removed_queues.is_empty() {
        tracing::info!(
            count = removed_queues.len(),
            queues = ?removed_queues,
            "purged worker registrations for removed queues"
        );
    }
}

const COMPLETED_JOB_MAX_AGE_SECS: u64 = 60 * 60 * 6;
const DEAD_JOB_MAX_AGE_SECS: u64 = 60 * 60 * 24;
const COMPLETED_JOB_MAX_COUNT: usize = 500;
const DEAD_JOB_MAX_COUNT: usize = 5_000;

const DEDUP_KEY_PATTERN: &str = "riven:dedup:*";

/// Delete all `riven:dedup:*` keys left over by `DedupGuard::drop`, which only
/// *attempts* an async cleanup and can lose the race against process exit on
/// a hard restart. Runs before any worker starts, so anything holding a dedup
/// key at this point belongs to a dead process.
pub async fn purge_stale_dedup_keys(redis: &mut redis::aio::ConnectionManager) {
    let mut cursor: u64 = 0;
    let mut purged = 0usize;
    loop {
        let (next_cursor, keys): (u64, Vec<String>) = match redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(DEDUP_KEY_PATTERN)
            .arg("COUNT")
            .arg(500)
            .query_async(redis)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, "purge_stale_dedup_keys: SCAN failed");
                return;
            }
        };

        if !keys.is_empty() {
            let _result: Result<(), _> = redis::cmd("DEL").arg(&keys).query_async(redis).await;
            purged += keys.len();
        }

        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    if purged > 0 {
        tracing::info!(count = purged, "purged stale dedup keys");
    }
}

/// Hand-rolled history retention, kept deliberately: apalis-redis's
/// `vacuum_before` has no count cap, walks the entire data hash with
/// `HGETALL` (blocking Redis for the duration on a large one), and leaves every
/// id it deletes in the done and dead sets, so those grow forever.
pub async fn prune_queue_history(redis: &mut redis::aio::ConnectionManager, queues: &[String]) {
    for queue in queues {
        let config = Config::default().queue(queue);
        let done = prune_set(
            redis,
            &config,
            &config.done_jobs_set(),
            COMPLETED_JOB_MAX_AGE_SECS,
            COMPLETED_JOB_MAX_COUNT,
        )
        .await;
        let dead = prune_set(
            redis,
            &config,
            &config.dead_jobs_set(),
            DEAD_JOB_MAX_AGE_SECS,
            DEAD_JOB_MAX_COUNT,
        )
        .await;
        if done + dead > 0 {
            tracing::info!(queue, done, dead, "pruned redis job history");
        }
    }
}

/// Remove the tasks in `set_key` older than `max_age_secs`, then the oldest of
/// what is left beyond `max_count`. A task's age is the `done_at` apalis
/// stamps on its metadata when it finishes.
async fn prune_set(
    redis: &mut redis::aio::ConnectionManager,
    config: &Config,
    set_key: &str,
    max_age_secs: u64,
    max_count: usize,
) -> usize {
    let ids: Vec<String> = redis::cmd("SMEMBERS")
        .arg(set_key)
        .query_async(redis)
        .await
        .unwrap_or_default();
    if ids.is_empty() {
        return 0;
    }
    let meta_key = |id: &str| format!("{}:{id}", config.job_meta_hash());
    let mut pipe = redis::pipe();
    for id in &ids {
        pipe.hget(meta_key(id), "done_at");
    }
    let done_at: Vec<Option<u64>> = match pipe.query_async(redis).await {
        Ok(done_at) => done_at,
        Err(_) => return 0,
    };

    let mut finished: Vec<(u64, String)> = done_at
        .into_iter()
        .zip(ids)
        .map(|(done_at, id)| (done_at.unwrap_or(0), id))
        .collect();
    finished.sort_unstable();
    let cutoff = Utc::now()
        .timestamp()
        .cast_unsigned()
        .saturating_sub(max_age_secs);
    let done_at: Vec<u64> = finished.iter().map(|(done_at, _)| *done_at).collect();
    let prune = prune_count(&done_at, cutoff, max_count);
    if prune == 0 {
        return 0;
    }

    let ids: Vec<&str> = finished[..prune]
        .iter()
        .map(|(_, id)| id.as_str())
        .collect();
    let meta_keys: Vec<String> = ids.iter().map(|id| meta_key(id)).collect();
    let _result: Result<(), _> = redis::pipe()
        .atomic()
        .srem(set_key, &ids)
        .hdel(config.job_data_hash(), &ids)
        .del(meta_keys)
        .query_async(redis)
        .await;
    prune
}

/// How many of `done_at` (oldest first) to prune: everything finished at or
/// before `cutoff`, and then the oldest of the rest beyond `max_count`.
fn prune_count(done_at: &[u64], cutoff: u64, max_count: usize) -> usize {
    let expired = done_at.partition_point(|done_at| *done_at <= cutoff);
    expired.max(done_at.len().saturating_sub(max_count))
}

#[cfg(test)]
mod tests {
    use super::prune_count;

    #[test]
    fn history_past_its_age_is_pruned() {
        assert_eq!(prune_count(&[10, 20, 30, 40], 25, 100), 2);
        assert_eq!(prune_count(&[10, 20], 5, 100), 0);
    }

    #[test]
    fn a_burst_beyond_the_cap_loses_its_oldest() {
        assert_eq!(prune_count(&[10, 20, 30, 40, 50], 0, 3), 2);
        assert_eq!(prune_count(&[10, 20, 30, 40, 50], 35, 3), 3);
    }
}
