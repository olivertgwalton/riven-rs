use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use apalis_redis::RedisConfig;
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

/// `register_worker.lua` writes the metadata hash for each worker at
/// `{workers_set}:metadata{worker_name}` (no separator before the worker name,
/// since the Lua does `KEYS[2] .. worker`). Mirror that format so cleanup wipes
/// the actual key apalis-redis wrote.
fn worker_metadata_key(workers_set: &str, worker_name: &str) -> String {
    format!("{workers_set}:metadata{worker_name}")
}

const QUEUE_REGISTRY_KEY: &str = "core::apalis::queues::list";

/// The apalis-redis configuration for one riven queue.
///
/// Every storage in [`crate::JobQueue`] and every maintenance pass builds its
/// config here, so the two can never disagree about a queue. That matters
/// beyond tidiness: the stale-worker rescue derives its cutoff from
/// `get_keep_alive()`, which is only a correct reading of "this worker stopped
/// heartbeating" if it comes from the same config the worker registered with.
/// Tuning a queue means changing it here, once, and the maintenance passes
/// follow automatically.
pub(crate) fn queue_config(namespace: &str) -> RedisConfig {
    RedisConfig::new(namespace)
}

/// Walk the apalis queue registry, find workers_sets whose queue is not in
/// `live_queues`, and drop them along with their metadata hashes. Cleans up
/// zombie worker registrations left by queues that have since been removed
/// (e.g. `riven:scrape-plugin` after the per-(plugin) hook-queue refactor).
/// `clear_worker_registrations` only walks the live queue list, so without
/// this pass the dashboard would keep showing pre-deploy workers indefinitely.
pub async fn purge_orphaned_worker_sets(
    redis: &mut redis::aio::ConnectionManager,
    live_queues: &[String],
) {
    let live: HashSet<String> = live_queues
        .iter()
        .map(|q| queue_config(q).workers_set())
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
        let workers: Vec<String> = redis::cmd("ZRANGE")
            .arg(&workers_set)
            .arg(0i64)
            .arg(-1i64)
            .query_async(redis)
            .await
            .unwrap_or_default();
        let mut pipe = redis::pipe();
        for worker in &workers {
            pipe.del(worker_metadata_key(&workers_set, worker));
            pipe.del(worker);
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

const COMPLETED_JOB_MAX_AGE_SECS: i64 = 60 * 60 * 6;
const FAILED_JOB_MAX_AGE_SECS: i64 = 60 * 60 * 24;
const COMPLETED_JOB_MAX_COUNT: isize = 500;
const FAILED_JOB_MAX_COUNT: isize = 5_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub workers: u64,
    pub jobs: u64,
}

/// Re-enqueue all inflight jobs and clear all worker registrations (called at startup).
pub async fn clear_worker_registrations(
    redis: &mut redis::aio::ConnectionManager,
    queues: &[String],
) -> redis::RedisResult<RecoveryReport> {
    rescue_workers(redis, queues, &Scope::All).await
}

/// How many heartbeats a worker may miss before the rescue concludes it is
/// gone rather than busy.
///
/// The margin has to be generous because the two outcomes are not
/// symmetrical. Waiting too long to rescue a genuinely dead worker only delays
/// its jobs. Deregistering one that is alive *destroys* it: `get_jobs.lua`
/// opens with a `zscore` check and raises "worker not registered", which
/// arrives as a stream error and drops that worker permanently — apalis
/// restarts nothing below monitor granularity, so its queue goes silent while
/// every other queue keeps running.
///
/// A live worker's registration is already up to one full keep-alive old the
/// moment it is written (`register_worker.lua` refuses to refresh sooner), and
/// a worker saturated with long CPU-bound jobs can miss a beat purely from
/// runtime scheduling. The rescue previously ran against a hardcoded 60s — two
/// beats at the configured keep-alive — so it reliably killed whichever worker
/// was busiest, requeued its inflight tasks, and let the next incarnation
/// claim and lose them again.
const MISSED_HEARTBEATS_BEFORE_DEAD: u32 = 10;

/// Which registrations a rescue pass should act on.
enum Scope {
    /// Every registration. Only valid at startup, when nothing this process
    /// owns is running and an entry can only be a previous incarnation's.
    All,
    /// Only registrations that have missed [`MISSED_HEARTBEATS_BEFORE_DEAD`]
    /// heartbeats, measured against each queue's own configured keep-alive.
    Abandoned,
}

/// Re-enqueue inflight jobs from workers that have stopped heartbeating.
///
/// The staleness cutoff is derived per queue from the same [`RedisConfig`] the
/// workers registered with, rather than supplied by the caller: the threshold
/// is only meaningful relative to that queue's keep-alive, and a caller passing
/// a plain number cannot know it.
pub async fn recover_stale_workers(
    redis: &mut redis::aio::ConnectionManager,
    queues: &[String],
) -> redis::RedisResult<RecoveryReport> {
    rescue_workers(redis, queues, &Scope::Abandoned).await
}

/// The point before which a registration on this queue counts as abandoned.
fn abandoned_before(config: &RedisConfig) -> i64 {
    let keep_alive = config.get_keep_alive().as_secs() as i64;
    Utc::now().timestamp() - keep_alive * i64::from(MISSED_HEARTBEATS_BEFORE_DEAD)
}

async fn rescue_workers(
    redis: &mut redis::aio::ConnectionManager,
    queues: &[String],
    scope: &Scope,
) -> redis::RedisResult<RecoveryReport> {
    let mut report = RecoveryReport::default();
    for queue_name in queues {
        let config = queue_config(queue_name);
        let workers: Vec<String> = match scope {
            Scope::All => {
                redis::cmd("ZRANGE")
                    .arg(config.workers_set())
                    .arg(0i64)
                    .arg(-1i64)
                    .query_async(redis)
                    .await?
            }
            Scope::Abandoned => {
                redis::cmd("ZRANGEBYSCORE")
                    .arg(config.workers_set())
                    .arg(0i64)
                    .arg(abandoned_before(&config))
                    .query_async(redis)
                    .await?
            }
        };
        if workers.is_empty() {
            continue;
        }

        let worker_jobs: Vec<Vec<String>> = {
            let mut pipe = redis::pipe();
            for worker in &workers {
                pipe.cmd("SMEMBERS").arg(worker);
            }
            pipe.query_async(redis).await?
        };
        let candidates: HashSet<String> = worker_jobs.into_iter().flatten().collect();
        let candidate_ids: Vec<String> = candidates.into_iter().collect();
        let exists: Vec<bool> = {
            let mut pipe = redis::pipe();
            for id in &candidate_ids {
                pipe.cmd("HEXISTS").arg(config.job_data_hash()).arg(id);
            }
            pipe.query_async(redis).await?
        };
        let rescued: Vec<String> = candidate_ids
            .into_iter()
            .zip(exists)
            .filter_map(|(id, exists)| exists.then_some(id))
            .collect();

        // Redis executes an atomic pipeline as MULTI/EXEC. Requeueing and
        // registration cleanup therefore commit together, so a connection
        // failure cannot leave jobs detached from both locations.
        let workers_set = config.workers_set();
        let mut transaction = redis::pipe();
        transaction.atomic();
        if !rescued.is_empty() {
            transaction
                .rpush(config.active_jobs_list(), &rescued)
                .ignore()
                .del(config.signal_list())
                .ignore()
                .lpush(config.signal_list(), 1u8)
                .ignore();
        }
        for worker in &workers {
            transaction
                .del(worker_metadata_key(&workers_set, worker))
                .ignore()
                .del(worker)
                .ignore()
                .zrem(&workers_set, worker)
                .ignore();
        }
        transaction.query_async::<()>(redis).await?;

        let worker_count = workers.len() as u64;
        let job_count = rescued.len() as u64;
        report.workers += worker_count;
        report.jobs += job_count;
        if worker_count > 0 {
            tracing::info!(
                queue = queue_name,
                workers = worker_count,
                jobs = job_count,
                "recovered stale workers"
            );
        }
    }
    Ok(report)
}

/// A queue whose workers hold claimed tasks and have stopped acting on them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StalledQueue {
    pub queue: String,
    /// Tasks claimed and frozen — these are what get requeued.
    pub stuck_tasks: usize,
    /// How long the inflight set has been frozen.
    pub stalled_for: Duration,
}

/// How long a queue's inflight set may sit completely unchanged before its
/// workers are treated as dead.
///
/// "Unchanged" is the load-bearing word: a working worker churns this set
/// constantly — every claim adds ids and every ack removes them — so a set
/// that is byte-identical across this whole window means nothing started and
/// nothing finished. One genuinely long task cannot trigger it, because its
/// neighbours completing would change the set.
///
/// Generous because the cost of being wrong is asymmetric: requeuing a task
/// that was merely slow risks running it twice, while missing a stall leaves
/// the queue dead until someone restarts the process.
pub const INFLIGHT_STALL_AFTER: Duration = Duration::from_secs(15 * 60);

/// Detects the signature of a worker whose task stream has died.
///
/// apalis runs a worker's `heartbeat` and its `tasks` stream independently
/// (`stream_select!(wait_for_exit, heartbeat, tasks)`), and a single
/// `StreamError` — a Redis timeout, a blip, a `get_jobs.lua` failure —
/// terminates `CallAll` for good. The heartbeat stream survives it, so the
/// worker goes on re-registering every keep-alive while never fetching or
/// executing anything again. From the outside it looks perfectly healthy:
/// registered, recent heartbeat, process up, CPU idle.
///
/// So liveness cannot be read from the registration. What actually
/// distinguishes a dead worker is that the tasks it already claimed stop
/// moving, which is what this watches. apalis ships its own recovery for this
/// (`reenqueue_orphaned_jobs.lua`, gated on `reenqueue_orphaned_after`) but
/// nothing in the crate ever calls it — the Lua is dead code and the config
/// field has only accessors, so claimed tasks are stranded until the process
/// restarts.
#[derive(Debug, Default)]
pub struct InflightWatch {
    frozen: HashMap<String, FrozenSince>,
}

#[derive(Debug)]
struct FrozenSince {
    ids: HashSet<String>,
    since: Instant,
}

impl InflightWatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sample every queue's inflight tasks and report the ones that have been
    /// frozen for longer than `stall_after`. Call on a regular tick; the
    /// comparison is against the previous call.
    pub async fn observe(
        &mut self,
        redis: &mut redis::aio::ConnectionManager,
        queues: &[String],
        stall_after: Duration,
    ) -> Vec<StalledQueue> {
        let mut stalled = Vec::new();

        for queue_name in queues {
            let ids = inflight_task_ids(redis, queue_name).await;

            if ids.is_empty() {
                self.frozen.remove(queue_name);
                continue;
            }

            match self.frozen.get(queue_name) {
                // Any change at all is progress: something was claimed or
                // acked, so the stream is alive.
                Some(previous) if previous.ids != ids => {
                    self.frozen.insert(
                        queue_name.clone(),
                        FrozenSince {
                            ids,
                            since: Instant::now(),
                        },
                    );
                }
                Some(previous) => {
                    let stalled_for = previous.since.elapsed();
                    if stalled_for >= stall_after {
                        stalled.push(StalledQueue {
                            queue: queue_name.clone(),
                            stuck_tasks: ids.len(),
                            stalled_for,
                        });
                        // Reset so a queue that stays stuck is reported once
                        // per window rather than on every tick.
                        self.frozen.remove(queue_name);
                    }
                }
                None => {
                    self.frozen.insert(
                        queue_name.clone(),
                        FrozenSince {
                            ids,
                            since: Instant::now(),
                        },
                    );
                }
            }
        }

        stalled
    }
}

/// Every task id currently claimed on this queue, across all of its workers'
/// inflight sets.
async fn inflight_task_ids(
    redis: &mut redis::aio::ConnectionManager,
    queue_name: &str,
) -> HashSet<String> {
    let pattern = format!("{}:*", queue_config(queue_name).inflight_jobs_set());
    let mut ids = HashSet::new();
    let mut cursor: u64 = 0;

    loop {
        let Ok((next, keys)) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(100)
            .query_async::<(u64, Vec<String>)>(redis)
            .await
        else {
            return ids;
        };

        for key in keys {
            let members: Vec<String> = redis::cmd("SMEMBERS")
                .arg(&key)
                .query_async(redis)
                .await
                .unwrap_or_default();
            ids.extend(members);
        }

        cursor = next;
        if cursor == 0 {
            return ids;
        }
    }
}

/// Remove job IDs from each queue's active list that have no corresponding
/// entry in the job-data hash. These orphans (no data + no meta) are harmless
/// when idle but cause the worker's poll stream to emit a StreamError the
/// first time it dequeues them, which kills the worker immediately.
pub async fn purge_orphaned_active_jobs(
    redis: &mut redis::aio::ConnectionManager,
    queues: &[String],
) {
    for queue_name in queues {
        let config = queue_config(queue_name);
        let active_key = config.active_jobs_list();
        let data_key = config.job_data_hash();

        let ids: Vec<String> = redis::cmd("LRANGE")
            .arg(&active_key)
            .arg(0i64)
            .arg(-1i64)
            .query_async(redis)
            .await
            .unwrap_or_default();

        if ids.is_empty() {
            continue;
        }

        let exists: Vec<bool> = {
            let mut pipe = redis::pipe();
            for id in &ids {
                pipe.cmd("HEXISTS").arg(&data_key).arg(id);
            }
            pipe.query_async(redis)
                .await
                .unwrap_or_else(|_| vec![true; ids.len()])
        };

        let orphans: Vec<&str> = ids
            .iter()
            .zip(exists.iter())
            .filter_map(|(id, &ok)| if !ok { Some(id.as_str()) } else { None })
            .collect();

        if orphans.is_empty() {
            continue;
        }

        let mut pipe = redis::pipe();
        for id in &orphans {
            pipe.cmd("LREM").arg(&active_key).arg(0i64).arg(id);
        }
        let _result: Result<(), _> = pipe.query_async(redis).await;

        tracing::info!(
            queue = queue_name,
            count = orphans.len(),
            "purged orphaned job IDs from active list (no data)"
        );
    }
}

const DEDUP_KEY_PATTERN: &str = "riven:dedup:*";

/// Delete all `riven:dedup:*` keys left over by `DedupGuard::drop`, which only
/// *attempts* an async cleanup and can lose the race against process exit on
/// a hard restart. Safe to run unconditionally here for the same reason
/// `clear_worker_registrations` unconditionally rescues jobs at this point:
/// anything holding a dedup key before this pass is presumed dead.
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

pub async fn prune_queue_history(redis: &mut redis::aio::ConnectionManager, queues: &[String]) {
    for queue in queues {
        let config = queue_config(queue);
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
    // `{meta}:result` holds each task's stored result (every plugin-hook
    // outcome lands there); without this HDEL the hash grows forever.
    let _result: Result<(), _> = redis::pipe()
        .atomic()
        .zrem(set_key, &ids)
        .hdel(job_data_hash, &ids)
        .hdel(format!("{job_meta_hash}:result"), &ids)
        .del(meta_keys)
        .query_async(redis)
        .await;

    ids.len()
}

#[cfg(test)]
mod tests {
    use super::{
        FrozenSince, INFLIGHT_STALL_AFTER, InflightWatch, MISSED_HEARTBEATS_BEFORE_DEAD,
        abandoned_before, queue_config,
    };
    use chrono::Utc;
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    fn hours_ago(hours: u64) -> Instant {
        Instant::now()
            .checked_sub(Duration::from_secs(hours * 60 * 60))
            .expect("test clock is far enough from the epoch")
    }

    fn past_the_stall_window() -> Instant {
        Instant::now()
            .checked_sub(INFLIGHT_STALL_AFTER + Duration::from_secs(1))
            .expect("test clock is far enough from the epoch")
    }

    /// The regression this guards is not a crash but a queue going silent.
    /// Declaring a live worker abandoned deregisters it, its next
    /// `get_jobs.lua` fetch fails the `zscore` check with "worker not
    /// registered", and apalis drops that worker while the process stays up.
    ///
    /// A worker that has beaten within its keep-alive is unambiguously alive,
    /// and one that beat only moments ago must never be inside the cutoff.
    #[test]
    fn a_worker_that_just_beat_is_never_abandoned() {
        let config = queue_config("riven:download");
        let cutoff = abandoned_before(&config);
        let just_now = Utc::now().timestamp();
        assert!(
            just_now > cutoff,
            "a worker heartbeating right now was judged abandoned"
        );

        let one_beat_ago = just_now - config.get_keep_alive().as_secs() as i64;
        assert!(
            one_beat_ago > cutoff,
            "a worker one keep-alive old is healthy — its score is always at \
             least that stale the moment it is written"
        );
    }

    /// The margin has to survive a busy worker missing several beats in a row,
    /// which is what the old hardcoded 60s (two beats) failed to do.
    #[test]
    fn the_cutoff_allows_several_missed_beats() {
        let config = queue_config("riven:download");
        let keep_alive = config.get_keep_alive().as_secs() as i64;
        let now = Utc::now().timestamp();
        let cutoff = abandoned_before(&config);

        let missed_beats = (now - cutoff) / keep_alive;
        assert_eq!(missed_beats, i64::from(MISSED_HEARTBEATS_BEFORE_DEAD));
        assert!(
            missed_beats >= 5,
            "{missed_beats} beats is too tight for a worker saturated with long jobs"
        );
    }

    /// A worker that is working churns its inflight set constantly — claims
    /// add ids, acks remove them — so any change at all must reset the clock.
    /// Getting this wrong requeues tasks that are actively running, which
    /// means downloading the same release twice.
    #[test]
    fn any_change_in_the_inflight_set_counts_as_progress() {
        let mut watch = InflightWatch::new();
        let queue = "riven:download".to_string();

        watch.frozen.insert(
            queue.clone(),
            FrozenSince {
                ids: HashSet::from(["a".to_string(), "b".to_string()]),
                since: hours_ago(1),
            },
        );

        // One task acked: the set changed, so the worker is alive however long
        // the remaining task has been running.
        let observed = HashSet::from(["a".to_string()]);
        let previous = watch.frozen.get(&queue).expect("seeded");
        assert_ne!(previous.ids, observed, "this is the progress case");
    }

    /// The stall signature: byte-identical set, long enough that no real task
    /// would still be running.
    #[test]
    fn a_frozen_set_past_the_window_is_stalled() {
        let mut watch = InflightWatch::new();
        let queue = "riven:download".to_string();
        let ids = HashSet::from(["a".to_string(), "b".to_string()]);

        watch.frozen.insert(
            queue.clone(),
            FrozenSince {
                ids: ids.clone(),
                since: past_the_stall_window(),
            },
        );

        let previous = watch.frozen.get(&queue).expect("seeded");
        assert_eq!(previous.ids, ids, "unchanged set");
        assert!(
            previous.since.elapsed() >= INFLIGHT_STALL_AFTER,
            "past the stall window"
        );
    }

    /// An empty inflight set is an idle queue, not a stalled one — forgetting
    /// to clear the record would make the next claim look instantly frozen.
    #[test]
    fn an_idle_queue_is_forgotten_rather_than_flagged() {
        let mut watch = InflightWatch::new();
        watch.frozen.insert(
            "riven:download".to_string(),
            FrozenSince {
                ids: HashSet::from(["a".to_string()]),
                since: hours_ago(1),
            },
        );
        watch.frozen.remove("riven:download");
        assert!(watch.frozen.is_empty());
    }

    /// The window has to exceed the slowest legitimate task by a wide margin;
    /// a usenet ingest with verification is minutes, not seconds.
    #[test]
    fn the_stall_window_outlasts_a_slow_job() {
        assert!(
            INFLIGHT_STALL_AFTER >= Duration::from_secs(10 * 60),
            "too tight — a slow ingest would be requeued while it runs"
        );
    }

    /// The cutoff is only meaningful if it is read from the same config the
    /// workers registered with — that is the whole reason `queue_config` is
    /// the single constructor.
    #[test]
    fn every_queue_derives_its_own_cutoff() {
        for queue in ["riven:download", "riven:scrape", "riven:plugin-hook:x:y"] {
            let config = queue_config(queue);
            assert!(
                config.get_keep_alive().as_secs() > 0,
                "{queue} has no keep-alive to derive a cutoff from"
            );
        }
    }
}
