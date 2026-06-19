pub mod application;
pub mod context;
pub mod dedup;
pub mod discovery;
pub mod indexing;
pub mod jobs;
pub mod lifecycle;
pub mod main_orchestrator;
pub mod maintenance;
pub mod worker;
pub mod workers;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};

use anyhow::Result;
use apalis::prelude::{TaskBuilder, TaskSink};
use apalis_redis::{RedisConfig, RedisStorage};
use chrono::{DateTime, Utc};
use futures::future;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{RwLock, broadcast};
use ulid::Ulid;

pub use riven_core::downloader::DownloaderConfig;
use riven_core::events::{DispatchStrategy, EventType, RivenEvent};
use riven_core::plugin::PluginRegistry;
use riven_core::reindex::ReindexConfig;
use riven_core::settings::FilesystemSettings;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_rank::ResolutionRanks;

pub use dedup::DedupGuard;
pub use jobs::{
    DownloadJob, IndexJob, ParseScrapeResultsJob, PluginHookJob, ProcessMediaItemJob, ProcessStep,
    RankStreamsJob, ScrapeJob,
};
pub use maintenance::{
    clear_worker_registrations, prune_queue_history, purge_orphaned_active_jobs,
    purge_orphaned_worker_sets, recover_stale_workers,
};
pub use workers::start_workers;

/// Per-command response timeout for every Redis connection. The socket itself
/// is reconnected by `ConnectionManager` in the background; this bounds the
/// wait for a *reply* so a command in flight across a blip fails fast instead
/// of hanging indefinitely.
const REDIS_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Bound on establishing/re-establishing the connection itself.
const REDIS_CONNECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Build a `ConnectionManager` with production timeouts. Used for both the
/// apalis storage connection and the maintenance connection so neither can hang
/// forever on a lost reply after a Redis blip. `ConnectionManager` is the same
/// type `apalis_redis::connect` returns, so the apalis `RedisStorage` backends
/// accept it unchanged.
pub async fn connect_managed(redis_url: &str) -> Result<redis::aio::ConnectionManager> {
    let client = redis::Client::open(redis_url)?;
    let config = redis::aio::ConnectionManagerConfig::new()
        .set_connection_timeout(Some(REDIS_CONNECTION_TIMEOUT))
        .set_response_timeout(Some(REDIS_RESPONSE_TIMEOUT));
    Ok(redis::aio::ConnectionManager::new_with_config(client, config).await?)
}

pub struct JobQueue {
    pub index_storage: RedisStorage<IndexJob>,
    pub scrape_storage: RedisStorage<ScrapeJob>,
    pub parse_storage: RedisStorage<ParseScrapeResultsJob>,
    pub download_storage: RedisStorage<DownloadJob>,
    pub rank_streams_storage: RedisStorage<RankStreamsJob>,
    pub process_media_item_storage: RedisStorage<ProcessMediaItemJob>,
    pub plugin_hook_storages: HashMap<(String, EventType), RedisStorage<PluginHookJob>>,
    pub redis: redis::aio::ConnectionManager,
    pub registry: Arc<PluginRegistry>,
    pub event_tx: broadcast::Sender<RivenEvent>,
    pub notification_tx: broadcast::Sender<String>,
    pub downloader_config: Arc<RwLock<DownloaderConfig>>,
    pub reindex_config: Arc<RwLock<ReindexConfig>>,
    pub filesystem_settings: Arc<RwLock<FilesystemSettings>>,
    pub vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    pub filesystem_settings_revision: Arc<AtomicU64>,
    pub retry_interval_secs: Arc<AtomicU64>,
    /// Hard ceiling on consecutive scrape failures before an item is marked
    /// `Failed`. `0` disables the ceiling.
    pub maximum_scrape_attempts: Arc<AtomicU32>,
    /// Cached resolution ranks — loaded once at startup and reloaded on settings save.
    pub resolution_ranks: Arc<RwLock<ResolutionRanks>>,
}

impl JobQueue {
    pub async fn new(
        redis_url: &str,
        registry: Arc<PluginRegistry>,
        notification_tx: broadcast::Sender<String>,
        downloader_config: DownloaderConfig,
        reindex_config: ReindexConfig,
        filesystem_settings: FilesystemSettings,
        retry_interval_secs: u64,
        maximum_scrape_attempts: u32,
    ) -> Result<Self> {
        let apalis_conn = connect_managed(redis_url).await?;

        let index_storage =
            RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:index"));
        let scrape_storage =
            RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:scrape"));
        let parse_storage =
            RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:parse"));
        let download_storage =
            RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:download"));
        let rank_streams_storage = RedisStorage::new_with_config(
            apalis_conn.clone(),
            RedisConfig::new("riven:rank-streams"),
        );
        let process_media_item_storage = RedisStorage::new_with_config(
            apalis_conn.clone(),
            RedisConfig::new("riven:process-media-item"),
        );

        let mut plugin_hook_storages: HashMap<(String, EventType), RedisStorage<PluginHookJob>> =
            HashMap::new();
        for (plugin_name, event_type) in registry.subscribed_event_pairs().await {
            if matches!(event_type.dispatch_strategy(), DispatchStrategy::Inline) {
                continue;
            }
            let namespace = format!("riven:plugin-hook:{}:{plugin_name}", event_type.slug());
            let storage =
                RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new(&namespace));
            plugin_hook_storages.insert((plugin_name, event_type), storage);
        }

        let redis = connect_managed(redis_url).await?;

        let resolution_ranks = riven_db::repo::load_resolution_ranks().await;
        let (event_tx, _) = broadcast::channel(4096);

        Ok(Self {
            index_storage,
            scrape_storage,
            parse_storage,
            download_storage,
            rank_streams_storage,
            process_media_item_storage,
            plugin_hook_storages,
            redis,
            registry,
            event_tx,
            notification_tx,
            downloader_config: Arc::new(RwLock::new(downloader_config)),
            reindex_config: Arc::new(RwLock::new(reindex_config)),
            vfs_layout: Arc::new(RwLock::new(VfsLibraryLayout::new(
                filesystem_settings.clone(),
            ))),
            filesystem_settings: Arc::new(RwLock::new(filesystem_settings)),
            filesystem_settings_revision: Arc::new(AtomicU64::new(0)),
            retry_interval_secs: Arc::new(AtomicU64::new(retry_interval_secs)),
            maximum_scrape_attempts: Arc::new(AtomicU32::new(maximum_scrape_attempts)),
            resolution_ranks: Arc::new(RwLock::new(resolution_ranks)),
        })
    }

    /// Every apalis-redis queue this `JobQueue` owns — fixed orchestrator queues
    /// plus the dynamic per-(plugin, event) hook queues. Maintenance routines
    /// (orphan purge, stale-worker rescue, history prune) iterate this so a new
    /// queue added in `JobQueue::new` is automatically covered. Missing one
    /// here causes orphaned active job IDs to kill its worker on first poll.
    pub fn queue_names(&self) -> Vec<String> {
        let mut names = vec![
            "riven:index".to_string(),
            "riven:scrape".to_string(),
            "riven:parse".to_string(),
            "riven:download".to_string(),
            "riven:rank-streams".to_string(),
            "riven:process-media-item".to_string(),
        ];
        for (plugin_name, event_type) in self.plugin_hook_storages.keys() {
            names.push(format!(
                "riven:plugin-hook:{}:{plugin_name}",
                event_type.slug()
            ));
        }
        names
    }

    pub async fn push_index(&self, job: IndexJob) {
        self.push_deduped("index", job.id, "IndexJob", || async {
            self.index_storage.clone().push(job).await
        })
        .await;
    }
    pub async fn push_scrape(&self, job: ScrapeJob) {
        self.push_deduped("scrape", job.id, "ScrapeJob", || async {
            self.scrape_storage.clone().push(job).await
        })
        .await;
    }

    /// Push a `ScrapeJob` to run after `delay` via apalis's native `run_at`
    /// scheduling. Bypasses `push_deduped` since the dedup key only covers the
    /// in-flight orchestrator phase.
    pub async fn push_scrape_after(&self, job: ScrapeJob, delay: std::time::Duration) {
        let task = TaskBuilder::new(job).run_after(delay).build();
        if let Err(e) = self.scrape_storage.clone().push_task(task).await {
            tracing::error!(error = %e, "failed to push delayed ScrapeJob");
        }
    }
    pub async fn push_parse_scrape_results(&self, job: ParseScrapeResultsJob) {
        self.push_deduped("parse", job.id, "ParseScrapeResultsJob", || async {
            self.parse_storage.clone().push(job).await
        })
        .await;
    }
    pub async fn push_download(&self, job: DownloadJob) {
        self.push_deduped("download", job.id, "DownloadJob", || async {
            self.download_storage.clone().push(job).await
        })
        .await;
    }

    /// Entry point for the download flow. Pushes a `RankStreamsJob` which loads
    /// streams, runs the cache check, builds ranked candidates, hands off to
    /// `DownloadJob` (find-valid-torrent + persist).
    pub async fn push_rank_streams(&self, job: RankStreamsJob) {
        self.push_deduped("rank-streams", job.id, "RankStreamsJob", || async {
            self.rank_streams_storage.clone().push(job).await
        })
        .await;
    }

    /// Resolve subscribers for `event`, initialise its fan-in flow, and push a
    /// plugin-hook child job to each subscriber's queue. Returns the number of
    /// children enqueued — `0` means no plugin subscribed, which the caller
    /// usually treats as "skip straight to finalize".
    ///
    /// Caller-provided `scope` namespaces the flow's Redis keys
    /// (`riven:flow:<prefix>:<scope>:...`); for per-item events use the media
    /// item id, for singletons use a fixed value.
    pub async fn fan_out_plugin_hook(&self, event: RivenEvent, scope: i64) -> usize {
        let event_type = event.event_type();
        let DispatchStrategy::FanIn { prefix } = event_type.dispatch_strategy() else {
            tracing::error!(
                ?event_type,
                "fan_out_plugin_hook called for non-FanIn event"
            );
            return 0;
        };
        let subscribers = self.registry.subscriber_names(event_type).await;
        if subscribers.is_empty() {
            return 0;
        }
        self.init_flow(prefix, scope, subscribers.len()).await;
        future::join_all(subscribers.iter().map(|plugin| {
            let event = event.clone();
            async move { self.push_plugin_hook(plugin, event, Some(scope)).await }
        }))
        .await;
        subscribers.len()
    }

    /// Push a per-plugin hook job onto the queue dedicated to
    /// `(plugin_name, event.event_type())`. The plugin-hook worker dispatches
    /// the event to that single plugin and — for fan-in events — stores the
    /// response under the `scope` flow keys, then triggers finalize / signals
    /// the awaiting caller when the last sibling completes.
    pub async fn push_plugin_hook(&self, plugin_name: &str, event: RivenEvent, scope: Option<i64>) {
        let event_type = event.event_type();
        let key = (plugin_name.to_string(), event_type);
        let Some(storage) = self.plugin_hook_storages.get(&key) else {
            tracing::warn!(
                plugin = plugin_name,
                ?event_type,
                "no plugin-hook storage registered for (plugin, event); skipping push"
            );
            return;
        };
        let job = PluginHookJob {
            plugin_name: plugin_name.to_string(),
            event,
            scope,
        };
        if let Err(e) = storage.clone().push(job).await {
            tracing::error!(
                plugin = plugin_name,
                ?event_type,
                error = %e,
                "failed to push plugin-hook job"
            );
        }
    }

    /// Enqueue a `ProcessMediaItemJob`. Bypasses `push_deduped` because the
    /// dedup key is per-step (`process-media-item:{step}:{id}`) — the job
    /// re-pushes itself with a different step at every transition, and we
    /// always want the new step to land. Inter-step protection comes from
    /// each child flow's own dedup (`scrape:{id}`, `download:{id}`, …).
    pub async fn push_process_media_item(&self, job: ProcessMediaItemJob) {
        if let Err(e) = self.process_media_item_storage.clone().push(job).await {
            tracing::error!(error = %e, "failed to push ProcessMediaItemJob");
        }
    }

    /// Re-acquire a media item: delete its media filesystem entries so it is no
    /// longer "completed" (state is derived from having a media entry), recompute
    /// state, then re-process. The re-scrape's ingest availability probe skips
    /// any incomplete/dead release, so a complete one is picked. Shared by the
    /// manual "Re-grab" mutation and the usenet auto-repair worker.
    pub async fn regrab_media_item(&self, media_item_id: i64) -> anyhow::Result<()> {
        use riven_core::entities::filesystem_entries;
        let entries: Vec<(i64, Option<String>)> = filesystem_entries::Entity::find()
            .filter(filesystem_entries::Column::MediaItemId.eq(media_item_id))
            .filter(
                filesystem_entries::Column::EntryType
                    .eq(riven_core::types::FileSystemEntryType::Media),
            )
            .select_only()
            .column(filesystem_entries::Column::Id)
            .column(filesystem_entries::Column::UsenetInfoHash)
            .into_tuple::<(i64, Option<String>)>()
            .all(riven_db::orm())
            .await?;

        for (_, info_hash) in &entries {
            if let Some(info_hash) = info_hash
                && let Err(error) = riven_db::repo::blacklist_stream_permanent_by_hash(
                    media_item_id,
                    info_hash,
                )
                .await
            {
                tracing::warn!(%error, info_hash, "regrab: failed to blacklist release");
            }
        }

        for (id, _) in &entries {
            if let Err(error) = riven_db::repo::delete_filesystem_entry(*id).await {
                tracing::warn!(%error, entry_id = *id, "regrab: failed to delete filesystem entry");
            }
        }

        riven_db::repo::recompute(&[media_item_id]).await?;
        self.push_process_media_item(ProcessMediaItemJob::new(media_item_id))
            .await;
        Ok(())
    }

    /// Re-push a `ProcessMediaItemJob` with a future `run_at`. Used by the
    /// `Scrape` step when `next_scrape_attempt_at` is in the future.
    pub async fn push_process_media_item_at(
        &self,
        job: ProcessMediaItemJob,
        run_at: DateTime<Utc>,
    ) {
        let now = Utc::now();
        if run_at <= now {
            self.push_process_media_item(job).await;
            return;
        }
        let delay = (run_at - now).to_std().unwrap_or_default();
        let task = TaskBuilder::new(job).run_after(delay).build();
        if let Err(e) = self
            .process_media_item_storage
            .clone()
            .push_task(task)
            .await
        {
            tracing::error!(error = %e, "failed to push delayed ProcessMediaItemJob");
        }
    }

    /// Enqueue the download flow starting at rank-streams, if at least one
    /// non-blacklisted stream exists. Returns `true` when enqueued.
    pub async fn push_download_from_best_stream(&self, id: i64) -> bool {
        let ranks = self.resolution_ranks.read().await.clone();
        let has_any = riven_db::repo::get_best_stream(id, &ranks)
            .await
            .ok()
            .flatten()
            .is_some();
        if !has_any {
            return false;
        }
        self.push_rank_streams(RankStreamsJob {
            id,
            preferred_info_hash: None,
        })
        .await;
        true
    }

    /// Release the dedup key for a job, allowing it to be re-queued.
    pub async fn release_dedup(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        if let Err(e) = redis::cmd("DEL")
            .arg(dedup_key(prefix, id))
            .query_async::<()>(&mut conn)
            .await
        {
            tracing::error!(error = %e, prefix, id, "failed to release dedup key");
        }
    }

    /// SET NX with a 30-min safety TTL. Returns `true` if the key was acquired.
    /// TTL fires only on hard process kill; normal path is `DedupGuard::drop`.
    async fn set_nx(&self, key: &str) -> bool {
        let mut conn = self.redis.clone();
        redis::cmd("SET")
            .arg(key)
            .arg(1u8)
            .arg("NX")
            .arg("EX")
            .arg(dedup::DEDUP_KEY_TTL_SECS)
            .query_async::<Option<String>>(&mut conn)
            .await
            .ok()
            .flatten()
            .is_some()
    }

    async fn push_deduped<F, Fut, E>(&self, prefix: &str, id: i64, label: &'static str, push: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Display,
    {
        if self.set_nx(&dedup_key(prefix, id)).await
            && let Err(e) = push().await
        {
            self.release_dedup(prefix, id).await;
            tracing::error!(error = %e, label, "failed to push job");
        }
    }

    pub async fn schedule_index_at(&self, job: IndexJob, run_at: DateTime<Utc>) {
        if run_at <= Utc::now() {
            self.clear_scheduled_index(job.id).await;
            self.push_index(job).await;
            return;
        }
        let id = job.id;
        let task_id = scheduled_index_task_id(id).to_string();
        self.schedule_apalis_task(
            "index",
            id,
            self.index_storage.get_config(),
            &task_id,
            &job,
            run_at,
        )
        .await;
    }

    pub async fn clear_scheduled_index(&self, id: i64) {
        let task_id = scheduled_index_task_id(id).to_string();
        self.clear_apalis_scheduled_task("index", id, self.index_storage.get_config(), &task_id)
            .await;
    }

    /// Force-overwrite an apalis-redis scheduled task: write payload, reset
    /// metadata, ZADD into the scheduled set, and remove any prior entry for
    /// this task_id from done/dead/failed/active. The deterministic task_id
    /// per item gives us "latest call wins" semantics.
    async fn schedule_apalis_task<Args: Serialize>(
        &self,
        kind: &'static str,
        id: i64,
        config: &apalis_redis::RedisConfig,
        task_id: &str,
        job: &Args,
        run_at: DateTime<Utc>,
    ) {
        let payload = match serialize_job_payload(job) {
            Ok(p) => p,
            Err(error) => {
                tracing::error!(id, kind, %error, "failed to serialize scheduled job");
                return;
            }
        };
        let mut conn = self.redis.clone();

        let existing: Option<i64> = redis::cmd("ZSCORE")
            .arg(config.scheduled_jobs_set())
            .arg(task_id)
            .query_async(&mut conn)
            .await
            .ok()
            .flatten();
        if let Some(existing_ts) = existing
            && existing_ts <= run_at.timestamp()
        {
            tracing::debug!(
                id,
                kind,
                existing_run_at = existing_ts,
                requested_run_at = run_at.timestamp(),
                "scheduled task already pending earlier; keeping existing schedule"
            );
            return;
        }

        let meta_key = format!("{}:{}", config.job_meta_hash(), task_id);
        let result: redis::RedisResult<()> = redis::pipe()
            .atomic()
            .hset(config.job_data_hash(), task_id, payload)
            .del(&meta_key)
            .hset_multiple(
                &meta_key,
                &[
                    ("attempts", "0"),
                    ("max_attempts", "5"),
                    ("status", "Pending"),
                ],
            )
            .zrem(config.scheduled_jobs_set(), task_id)
            .zrem(config.done_jobs_set(), task_id)
            .zrem(config.dead_jobs_set(), task_id)
            .zrem(config.failed_jobs_set(), task_id)
            .lrem(config.active_jobs_list(), 0, task_id)
            .zadd(config.scheduled_jobs_set(), task_id, run_at.timestamp())
            .query_async(&mut conn)
            .await;
        match result {
            Ok(()) => tracing::info!(id, kind, run_at = %run_at, "scheduled delayed job"),
            Err(error) => tracing::error!(id, kind, %error, "failed to schedule delayed job"),
        }
    }

    async fn clear_apalis_scheduled_task(
        &self,
        kind: &'static str,
        id: i64,
        config: &apalis_redis::RedisConfig,
        task_id: &str,
    ) {
        let meta_key = format!("{}:{}", config.job_meta_hash(), task_id);
        let mut conn = self.redis.clone();
        let result: redis::RedisResult<()> = redis::pipe()
            .atomic()
            .zrem(config.scheduled_jobs_set(), task_id)
            .hdel(config.job_data_hash(), task_id)
            .del(&meta_key)
            .query_async(&mut conn)
            .await;
        if let Err(error) = result {
            tracing::error!(id, kind, %error, "failed to clear scheduled job");
        }
    }

    /// Begin a fan-in flow expecting `expected` children. Records the expected
    /// total and clears any leftover state for this scope so a re-run (e.g. a
    /// re-scrape of the same item) starts clean. The pending key now holds the
    /// immutable expected count; per-child progress lives in the done set.
    pub async fn init_flow(&self, prefix: &str, id: i64, expected: usize) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::pipe()
            .del(flow_results_key(prefix, id))
            .del(flow_done_key(prefix, id))
            .del(flow_rate_limited_key(prefix, id))
            .cmd("SET")
            .arg(flow_pending_key(prefix, id))
            .arg(expected)
            .arg("EX")
            .arg(3600i64)
            .query_async(&mut conn)
            .await;
    }

    pub async fn flow_store_result<T: Serialize>(
        &self,
        prefix: &str,
        id: i64,
        field: &str,
        value: &T,
    ) {
        let Ok(payload) = serde_json::to_string(value) else {
            tracing::error!(prefix, id, field, "failed to serialize flow result");
            return;
        };
        let key = flow_results_key(prefix, id);
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::pipe()
            .hset(&key, field, &payload)
            .expire(&key, 3600i64)
            .query_async(&mut conn)
            .await;
    }

    /// Mark `child` (the plugin name) done for this flow and report whether it
    /// was the final outstanding child — the signal to run `finalize`.
    ///
    /// apalis redelivers jobs at least once, so completion is tracked in a
    /// Redis set rather than a counter: a retried child re-adds the same member
    /// (a no-op), so it can neither finalize twice nor push the total past the
    /// expected count and skip the finalize entirely (the old `DECR` counter
    /// could go negative on a retry and strand the item). The add-and-compare
    /// runs in one Lua script so exactly one caller observes completion even
    /// when siblings finish concurrently.
    pub async fn flow_complete_child(&self, prefix: &str, id: i64, child: &str) -> bool {
        let script = redis::Script::new(
            r"
            local added = redis.call('SADD', KEYS[1], ARGV[1])
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            redis.call('EXPIRE', KEYS[2], ARGV[2])
            if added == 0 then return 0 end
            local expected = tonumber(redis.call('GET', KEYS[2]))
            if not expected or expected <= 0 then return 0 end
            if redis.call('SCARD', KEYS[1]) == expected then return 1 else return 0 end
            ",
        );
        let mut conn = self.redis.clone();
        let last: i64 = script
            .key(flow_done_key(prefix, id))
            .key(flow_pending_key(prefix, id))
            .arg(child)
            .arg(3600i64)
            .invoke_async(&mut conn)
            .await
            .unwrap_or(0);
        last == 1
    }

    /// Atomically read and clear the flow results hash. Use this when the
    /// caller is the sole consumer of the results and should not leave the
    /// key behind on bail-out paths.
    pub async fn drain_flow_results<T: DeserializeOwned>(&self, prefix: &str, id: i64) -> Vec<T> {
        let key = flow_results_key(prefix, id);
        let mut conn = self.redis.clone();
        let (raw, _): (Vec<String>, i64) = redis::pipe()
            .cmd("HVALS")
            .arg(&key)
            .cmd("DEL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap_or_default();
        deserialize_flow_results(prefix, id, raw)
    }

    pub async fn clear_flow(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::cmd("DEL")
            .arg(flow_pending_key(prefix, id))
            .arg(flow_done_key(prefix, id))
            .query_async(&mut conn)
            .await;
    }

    /// Drop every Redis key associated with a flow in a single round-trip.
    /// The DEL is a no-op for keys that don't exist, so this is safe to call
    /// from any bail-out path regardless of which keys have been written.
    pub async fn clear_flow_all(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::cmd("DEL")
            .arg(flow_pending_key(prefix, id))
            .arg(flow_done_key(prefix, id))
            .arg(flow_results_key(prefix, id))
            .arg(flow_rate_limited_key(prefix, id))
            .query_async(&mut conn)
            .await;
    }

    /// Increment the count of rate-limited plugin completions for this flow.
    /// Called instead of (and before) `flow_complete_child` when a 429 is received
    /// so `finalize` can distinguish "every scraper was rate-limited" from
    /// "scrapers ran but found nothing".
    pub async fn flow_increment_rate_limited(&self, prefix: &str, id: i64) {
        let key = flow_rate_limited_key(prefix, id);
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::pipe()
            .cmd("INCR")
            .arg(&key)
            .cmd("EXPIRE")
            .arg(&key)
            .arg(3600i64)
            .query_async(&mut conn)
            .await;
    }

    /// Return the number of rate-limited plugin completions recorded for this flow.
    pub async fn flow_rate_limited_count(&self, prefix: &str, id: i64) -> i64 {
        let mut conn = self.redis.clone();
        redis::cmd("GET")
            .arg(flow_rate_limited_key(prefix, id))
            .query_async::<Option<i64>>(&mut conn)
            .await
            .unwrap_or(None)
            .unwrap_or(0)
    }

    /// Delete the rate-limited counter for this flow (called in `finalize`).
    pub async fn clear_flow_rate_limited(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::cmd("DEL")
            .arg(flow_rate_limited_key(prefix, id))
            .query_async(&mut conn)
            .await;
    }

    pub async fn flow_result_count(&self, prefix: &str, id: i64) -> i64 {
        let mut conn = self.redis.clone();
        redis::cmd("HLEN")
            .arg(flow_results_key(prefix, id))
            .query_async(&mut conn)
            .await
            .unwrap_or(0)
    }

    /// Persist orchestrator parent state (e.g. the original `ScrapeJob`) so
    /// `finalize` — invoked on the last child completion in a different
    /// worker — can recover the rate-limit retry counter and any other
    /// fields not encoded in the per-plugin event payload.
    pub async fn flow_set_context<T: Serialize>(&self, prefix: &str, scope: i64, ctx: &T) {
        let Ok(payload) = serde_json::to_string(ctx) else {
            tracing::error!(prefix, scope, "failed to serialize flow context");
            return;
        };
        let key = flow_context_key(prefix, scope);
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::pipe()
            .cmd("SET")
            .arg(&key)
            .arg(payload)
            .arg("EX")
            .arg(3600i64)
            .query_async(&mut conn)
            .await;
    }

    pub async fn flow_get_context<T: DeserializeOwned>(
        &self,
        prefix: &str,
        scope: i64,
    ) -> Option<T> {
        let key = flow_context_key(prefix, scope);
        let mut conn = self.redis.clone();
        let raw: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .ok()
            .flatten();
        raw.and_then(|s| serde_json::from_str(&s).ok())
    }

    pub async fn flow_clear_context(&self, prefix: &str, scope: i64) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::cmd("DEL")
            .arg(flow_context_key(prefix, scope))
            .query_async(&mut conn)
            .await;
    }

    /// Returns true if `cancel_items` was called for this id recently. In-flight
    /// download handlers poll this between candidates so deleting an item
    /// stops debrid churn immediately, not only after the whole candidate list
    /// has been walked.
    pub async fn is_cancelled(&self, id: i64) -> bool {
        let mut conn = self.redis.clone();
        redis::cmd("SISMEMBER")
            .arg(CANCELLED_ITEMS_SET)
            .arg(id)
            .query_async::<bool>(&mut conn)
            .await
            .unwrap_or(false)
    }

    /// Purge any queued or scheduled apalis jobs whose payload references one
    /// of the given media item ids. Also clears dedup keys and flow state so
    /// the deleted item leaves no debris.
    ///
    /// Called from the `remove_items` mutation so deleting a request from the
    /// UI immediately stops its jobs from churning the debrid service.
    pub async fn cancel_items(&self, ids: &[i64]) {
        if ids.is_empty() {
            return;
        }
        let id_set: std::collections::HashSet<i64> = ids.iter().copied().collect();

        let mut conn = self.redis.clone();
        let mut pipe = redis::pipe();
        for id in ids {
            pipe.cmd("SADD").arg(CANCELLED_ITEMS_SET).arg(*id).ignore();
        }
        pipe.cmd("EXPIRE")
            .arg(CANCELLED_ITEMS_SET)
            .arg(600i64)
            .ignore();
        let _result: Result<(), _> = pipe.query_async(&mut conn).await;

        let configs: [apalis_redis::RedisConfig; 5] = [
            self.index_storage.get_config().clone(),
            self.scrape_storage.get_config().clone(),
            self.parse_storage.get_config().clone(),
            self.download_storage.get_config().clone(),
            self.rank_streams_storage.get_config().clone(),
        ];

        for config in &configs {
            if let Err(error) = self.purge_queue_for_ids(config, &id_set).await {
                tracing::warn!(error = %error, queue = %config.job_data_hash(), "failed to purge queue");
            }
        }

        for ((_plugin, event_type), storage) in &self.plugin_hook_storages {
            if !matches!(
                event_type,
                EventType::MediaItemScrapeRequested | EventType::MediaItemIndexRequested
            ) {
                continue;
            }
            let config = storage.get_config().clone();
            if let Err(error) = self.purge_plugin_hook_queue_for_ids(&config, &id_set).await {
                tracing::warn!(error = %error, queue = %config.job_data_hash(), "failed to purge plugin-hook queue");
            }
        }

        let mut conn = self.redis.clone();
        for id in ids {
            for prefix in ["index", "scrape", "parse", "download", "rank-streams"] {
                let _result: Result<(), _> = redis::cmd("DEL")
                    .arg(dedup_key(prefix, *id))
                    .query_async(&mut conn)
                    .await;
            }
            for prefix in ["scrape", "parse", "index"] {
                let _result: Result<(), _> = redis::pipe()
                    .cmd("DEL")
                    .arg(flow_pending_key(prefix, *id))
                    .cmd("DEL")
                    .arg(flow_done_key(prefix, *id))
                    .cmd("DEL")
                    .arg(flow_results_key(prefix, *id))
                    .cmd("DEL")
                    .arg(flow_rate_limited_key(prefix, *id))
                    .query_async(&mut conn)
                    .await;
            }
        }
    }

    /// Same as `purge_queue_for_ids` but reads the media item id from
    /// `event.id` instead of the job's top-level `id`. Used for the
    /// per-(plugin, event) hook queues whose payload is `PluginHookJob`.
    async fn purge_plugin_hook_queue_for_ids(
        &self,
        config: &apalis_redis::RedisConfig,
        ids: &std::collections::HashSet<i64>,
    ) -> redis::RedisResult<()> {
        self.purge_queue_with_id_extractor(config, ids, |value| {
            value
                .get("event")
                .and_then(|e| e.get("id"))
                .and_then(serde_json::Value::as_i64)
        })
        .await
    }

    async fn purge_queue_for_ids(
        &self,
        config: &apalis_redis::RedisConfig,
        ids: &std::collections::HashSet<i64>,
    ) -> redis::RedisResult<()> {
        self.purge_queue_with_id_extractor(config, ids, |value| {
            value.get("id").and_then(serde_json::Value::as_i64)
        })
        .await
    }

    async fn purge_queue_with_id_extractor<F>(
        &self,
        config: &apalis_redis::RedisConfig,
        ids: &std::collections::HashSet<i64>,
        extract_id: F,
    ) -> redis::RedisResult<()>
    where
        F: Fn(&serde_json::Value) -> Option<i64>,
    {
        let mut conn = self.redis.clone();
        let data_hash = config.job_data_hash();
        let active_list = config.active_jobs_list();
        let scheduled_set = config.scheduled_jobs_set();
        let inflight_set = config.inflight_jobs_set();
        let done_set = config.done_jobs_set();
        let dead_set = config.dead_jobs_set();
        let failed_set = config.failed_jobs_set();
        let meta_hash_prefix = config.job_meta_hash();

        let mut cursor: u64 = 0;
        let mut matching_task_ids: Vec<String> = Vec::new();

        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("HSCAN")
                .arg(&data_hash)
                .arg(cursor)
                .arg("COUNT")
                .arg(200u32)
                .query_async(&mut conn)
                .await?;

            let mut iter = batch.into_iter();
            while let (Some(task_id), Some(payload)) = (iter.next(), iter.next()) {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) else {
                    continue;
                };
                let Some(id) = extract_id(&value) else {
                    continue;
                };
                if ids.contains(&id) {
                    matching_task_ids.push(task_id);
                }
            }

            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        if matching_task_ids.is_empty() {
            return Ok(());
        }

        tracing::info!(
            queue = %data_hash,
            count = matching_task_ids.len(),
            "purging queued jobs for cancelled items"
        );

        let mut pipe = redis::pipe();
        pipe.atomic();
        for task_id in &matching_task_ids {
            pipe.cmd("HDEL").arg(&data_hash).arg(task_id).ignore();
            pipe.cmd("LREM")
                .arg(&active_list)
                .arg(0)
                .arg(task_id)
                .ignore();
            pipe.cmd("ZREM").arg(&scheduled_set).arg(task_id).ignore();
            pipe.cmd("ZREM").arg(&inflight_set).arg(task_id).ignore();
            pipe.cmd("ZREM").arg(&done_set).arg(task_id).ignore();
            pipe.cmd("ZREM").arg(&dead_set).arg(task_id).ignore();
            pipe.cmd("ZREM").arg(&failed_set).arg(task_id).ignore();
            pipe.cmd("DEL")
                .arg(format!("{meta_hash_prefix}:{task_id}"))
                .ignore();
        }
        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    pub async fn notify(&self, event: RivenEvent) {
        drop(self.event_tx.send(event.clone()));

        let event_type = event.event_type();
        if event_type.is_ui_streamed()
            && let Ok(json) = serde_json::to_string(&event)
        {
            drop(self.notification_tx.send(json));
        }

        let subscribers = self.registry.subscriber_names(event_type).await;
        for plugin_name in subscribers {
            let key = (plugin_name.clone(), event_type);
            let Some(storage) = self.plugin_hook_storages.get(&key) else {
                tracing::warn!(
                    plugin = %plugin_name,
                    event = ?event_type,
                    "no hook storage registered for (plugin, event); skipping fan-out"
                );
                continue;
            };
            let job = PluginHookJob {
                plugin_name: plugin_name.clone(),
                event: event.clone(),
                scope: None,
            };
            if let Err(error) = storage.clone().push(job).await {
                tracing::error!(
                    plugin = %plugin_name,
                    event = ?event_type,
                    %error,
                    "failed to enqueue plugin hook job"
                );
            }
        }
    }

}

/// Serialize a job payload into a single pre-sized heap allocation.
///
/// `serde_json::to_vec` starts with an empty `Vec` and grows as bytes are
/// written (multiple reallocations for typical ~256-byte job payloads).
/// Preallocating once avoids the growth doubling pattern.
fn serialize_job_payload<T: Serialize>(job: &T) -> serde_json::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    serde_json::to_writer(&mut buf, job)?;
    Ok(buf)
}

#[inline]
fn flow_pending_key(prefix: &str, id: i64) -> String {
    format!("riven:flow:{prefix}:{id}:pending")
}

#[inline]
fn flow_results_key(prefix: &str, id: i64) -> String {
    format!("riven:flow:{prefix}:{id}:results")
}

/// Set of children (plugin names) that have completed this flow. Replaces the
/// old decrementing counter so completion is idempotent under apalis's
/// at-least-once redelivery — see [`JobQueue::flow_complete_child`].
#[inline]
fn flow_done_key(prefix: &str, id: i64) -> String {
    format!("riven:flow:{prefix}:{id}:done")
}

#[inline]
fn flow_rate_limited_key(prefix: &str, id: i64) -> String {
    format!("riven:flow:{prefix}:{id}:rate_limited")
}

#[inline]
fn flow_context_key(prefix: &str, id: i64) -> String {
    format!("riven:flow:{prefix}:{id}:context")
}

fn deserialize_flow_results<T: DeserializeOwned>(
    prefix: &str,
    id: i64,
    raw: Vec<String>,
) -> Vec<T> {
    raw.into_iter()
        .filter_map(|s| match serde_json::from_str(&s) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::error!(prefix, id, error = %e, "failed to deserialize flow result");
                None
            }
        })
        .collect()
}

const CANCELLED_ITEMS_SET: &str = "riven:cancelled-items";

#[inline]
fn dedup_key(prefix: &str, id: i64) -> String {
    format!("riven:dedup:{prefix}:{id}")
}

const SCHEDULED_INDEX_TASK_NAMESPACE: u128 = 0x524956454e494e44_0000000000000000;

fn scheduled_index_task_id(id: i64) -> Ulid {
    Ulid::from(SCHEDULED_INDEX_TASK_NAMESPACE | u128::from(id.cast_unsigned()))
}
