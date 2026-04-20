pub mod application;
pub mod context;
pub mod dedup;
pub mod discovery;
pub mod flows;
pub mod indexing;
pub mod jobs;
pub mod maintenance;
pub mod orchestrator;
pub mod worker;
pub mod workers;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::Result;
use apalis::prelude::TaskSink;
use apalis_redis::{RedisConfig, RedisStorage};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{RwLock, broadcast};
use ulid::Ulid;

pub use riven_core::downloader::DownloaderConfig;
use riven_core::events::RivenEvent;
use riven_core::plugin::PluginRegistry;
use riven_core::reindex::ReindexConfig;
use riven_core::settings::FilesystemSettings;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_rank::ResolutionRanks;

pub use dedup::DedupGuard;
pub use jobs::{
    ContentServiceJob, DownloadJob, IndexJob, IndexPluginJob, ParseScrapeResultsJob,
    RankStreamsJob, ScrapeJob, ScrapePluginJob,
};
pub use maintenance::{
    clear_worker_registrations, prune_queue_history, purge_orphaned_active_jobs,
    recover_stale_workers,
};
pub use workers::start_workers;

// ── JobQueue ──────────────────────────────────────────────────────────────────

pub struct JobQueue {
    pub index_storage: RedisStorage<IndexJob>,
    pub index_plugin_storage: RedisStorage<IndexPluginJob>,
    pub scrape_storage: RedisStorage<ScrapeJob>,
    pub scrape_plugin_storage: RedisStorage<ScrapePluginJob>,
    pub parse_storage: RedisStorage<ParseScrapeResultsJob>,
    pub download_storage: RedisStorage<DownloadJob>,
    pub rank_streams_storage: RedisStorage<RankStreamsJob>,
    pub content_storage: RedisStorage<ContentServiceJob>,
    pub redis: redis::aio::ConnectionManager,
    pub registry: Arc<PluginRegistry>,
    pub event_tx: broadcast::Sender<RivenEvent>,
    pub notification_tx: broadcast::Sender<String>,
    pub db_pool: sqlx::PgPool,
    pub downloader_config: Arc<RwLock<DownloaderConfig>>,
    pub reindex_config: Arc<RwLock<ReindexConfig>>,
    pub filesystem_settings: Arc<RwLock<FilesystemSettings>>,
    pub vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    pub filesystem_settings_revision: Arc<AtomicU64>,
    pub retry_interval_secs: Arc<AtomicU64>,
    /// Cached resolution ranks — loaded once at startup and reloaded on settings save.
    pub resolution_ranks: Arc<RwLock<ResolutionRanks>>,
}

impl JobQueue {
    pub async fn new(
        redis_url: &str,
        registry: Arc<PluginRegistry>,
        notification_tx: broadcast::Sender<String>,
        db_pool: sqlx::PgPool,
        downloader_config: DownloaderConfig,
        reindex_config: ReindexConfig,
        filesystem_settings: FilesystemSettings,
        retry_interval_secs: u64,
    ) -> Result<Self> {
        let apalis_conn = apalis_redis::connect(redis_url).await?;

        let index_storage =
            RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:index"));
        let index_plugin_storage = RedisStorage::new_with_config(
            apalis_conn.clone(),
            RedisConfig::new("riven:index-plugin"),
        );
        let scrape_storage =
            RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:scrape"));
        let scrape_plugin_storage = RedisStorage::new_with_config(
            apalis_conn.clone(),
            RedisConfig::new("riven:scrape-plugin"),
        );
        let parse_storage =
            RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:parse"));
        let download_storage =
            RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:download"));
        let rank_streams_storage = RedisStorage::new_with_config(
            apalis_conn.clone(),
            RedisConfig::new("riven:rank-streams"),
        );
        let content_storage =
            RedisStorage::new_with_config(apalis_conn, RedisConfig::new("riven:content"));

        let redis_client = redis::Client::open(redis_url)?;
        let redis = redis::aio::ConnectionManager::new(redis_client).await?;

        let resolution_ranks = riven_db::repo::load_resolution_ranks(&db_pool).await;
        let (event_tx, _) = broadcast::channel(256);

        Ok(Self {
            index_storage,
            index_plugin_storage,
            scrape_storage,
            scrape_plugin_storage,
            parse_storage,
            download_storage,
            rank_streams_storage,
            content_storage,
            redis,
            registry,
            event_tx,
            notification_tx,
            db_pool,
            downloader_config: Arc::new(RwLock::new(downloader_config)),
            reindex_config: Arc::new(RwLock::new(reindex_config)),
            vfs_layout: Arc::new(RwLock::new(VfsLibraryLayout::new(
                filesystem_settings.clone(),
            ))),
            filesystem_settings: Arc::new(RwLock::new(filesystem_settings)),
            filesystem_settings_revision: Arc::new(AtomicU64::new(0)),
            retry_interval_secs: Arc::new(AtomicU64::new(retry_interval_secs)),
            resolution_ranks: Arc::new(RwLock::new(resolution_ranks)),
        })
    }

    // ── Job push ──────────────────────────────────────────────────────────────

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
    /// `DownloadJob` (find-valid-torrent + persist). Mirrors riven-ts
    /// `enqueueDownloadItem` → rank-streams → find-valid-torrent → download-item.
    pub async fn push_rank_streams(&self, job: RankStreamsJob) {
        self.push_deduped("rank-streams", job.id, "RankStreamsJob", || async {
            self.rank_streams_storage.clone().push(job).await
        })
        .await;
    }

    pub async fn push_index_plugin(&self, job: IndexPluginJob) {
        if let Err(e) = self.index_plugin_storage.clone().push(job).await {
            tracing::error!(error = %e, "failed to push IndexPluginJob");
        }
    }
    pub async fn push_scrape_plugin(&self, job: ScrapePluginJob) {
        if let Err(e) = self.scrape_plugin_storage.clone().push(job).await {
            tracing::error!(error = %e, "failed to push ScrapePluginJob");
        }
    }
    pub async fn push_content_service(&self) {
        if let Err(e) = self.content_storage.clone().push(ContentServiceJob).await {
            tracing::error!(error = %e, "failed to push ContentServiceJob");
        }
    }

    /// Enqueue the download flow starting at rank-streams, if at least one
    /// non-blacklisted stream exists. Returns `true` when enqueued.
    pub async fn push_download_from_best_stream(&self, id: i64) -> bool {
        let ranks = self.resolution_ranks.read().await.clone();
        let has_any = riven_db::repo::get_best_stream(&self.db_pool, id, &ranks)
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

    // ── Dedup ─────────────────────────────────────────────────────────────────

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

    // ── Scheduled index ───────────────────────────────────────────────────────

    pub async fn schedule_index_at(&self, job: IndexJob, run_at: DateTime<Utc>) {
        let now = Utc::now();
        if run_at <= now {
            self.clear_scheduled_index(job.id).await;
            self.push_index(job).await;
            return;
        }

        let config = self.index_storage.get_config().clone();
        let task_id = scheduled_index_task_id(job.id).to_string();
        let meta_key = format!("{}:{}", config.job_meta_hash(), task_id);
        let payload = match serde_json::to_vec(&job) {
            Ok(p) => p,
            Err(error) => {
                tracing::error!(id = job.id, error = %error, "failed to serialize scheduled index job");
                return;
            }
        };

        let mut conn = self.redis.clone();
        let result: redis::RedisResult<()> = redis::pipe()
            .atomic()
            .hset(config.job_data_hash(), &task_id, payload)
            .del(&meta_key)
            .hset_multiple(
                &meta_key,
                &[
                    ("attempts", "0"),
                    ("max_attempts", "5"),
                    ("status", "Pending"),
                ],
            )
            .zrem(config.scheduled_jobs_set(), &task_id)
            .zrem(config.done_jobs_set(), &task_id)
            .zrem(config.dead_jobs_set(), &task_id)
            .zrem(config.failed_jobs_set(), &task_id)
            .lrem(config.active_jobs_list(), 0, &task_id)
            .zadd(config.scheduled_jobs_set(), &task_id, run_at.timestamp())
            .query_async(&mut conn)
            .await;

        match result {
            Ok(()) => tracing::info!(id = job.id, run_at = %run_at, "scheduled delayed index job"),
            Err(error) => {
                tracing::error!(id = job.id, error = %error, "failed to schedule delayed index job")
            }
        }
    }

    pub async fn clear_scheduled_index(&self, id: i64) {
        let config = self.index_storage.get_config().clone();
        let task_id = scheduled_index_task_id(id).to_string();
        let meta_key = format!("{}:{}", config.job_meta_hash(), task_id);
        let mut conn = self.redis.clone();

        let result: redis::RedisResult<()> = redis::pipe()
            .atomic()
            .zrem(config.scheduled_jobs_set(), &task_id)
            .hdel(config.job_data_hash(), &task_id)
            .del(&meta_key)
            .query_async(&mut conn)
            .await;

        if let Err(error) = result {
            tracing::error!(id, error = %error, "failed to clear scheduled index job");
        }
    }

    // ── Flow helpers ──────────────────────────────────────────────────────────

    pub async fn init_flow(&self, prefix: &str, id: i64, pending: usize) {
        let mut conn = self.redis.clone();
        // Clear any stale results from a previous run and reset the pending counter atomically.
        let _: Result<(), _> = redis::pipe()
            .del(flow_results_key(prefix, id))
            .cmd("SET")
            .arg(flow_pending_key(prefix, id))
            .arg(pending)
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
        let _: Result<(), _> = redis::pipe()
            .hset(&key, field, &payload)
            .expire(&key, 3600i64)
            .query_async(&mut conn)
            .await;
    }

    pub async fn flow_complete_child(&self, prefix: &str, id: i64) -> bool {
        let pending_key = flow_pending_key(prefix, id);
        let mut conn = self.redis.clone();
        // Pipeline DECR + EXPIRE to save a round-trip on every plugin job completion.
        let (remaining, _): (i64, i64) = redis::pipe()
            .cmd("DECR")
            .arg(&pending_key)
            .cmd("EXPIRE")
            .arg(&pending_key)
            .arg(3600i64)
            .query_async(&mut conn)
            .await
            .unwrap_or((-1, 0));
        remaining == 0
    }

    pub async fn flow_load_results<T: DeserializeOwned>(&self, prefix: &str, id: i64) -> Vec<T> {
        let key = flow_results_key(prefix, id);
        let mut conn = self.redis.clone();
        let raw: Vec<String> = redis::cmd("HVALS")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap_or_default();
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

    pub async fn clear_flow(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        let _: Result<(), _> = redis::cmd("DEL")
            .arg(flow_pending_key(prefix, id))
            .query_async(&mut conn)
            .await;
    }

    pub async fn clear_flow_results(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        let _: Result<(), _> = redis::cmd("DEL")
            .arg(flow_results_key(prefix, id))
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
        let _: Result<(), _> = redis::pipe()
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
        let _: Result<(), _> = redis::cmd("DEL")
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

    // ── Queue cancellation ────────────────────────────────────────────────────

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
    /// of the given media item ids. Also clears dedup keys, flow state, and
    /// the download rank-result hand-off so the deleted item leaves no debris.
    ///
    /// Called from the `remove_items` mutation so deleting a request from the
    /// UI immediately stops its jobs from churning the debrid service.
    pub async fn cancel_items(&self, ids: &[i64]) {
        if ids.is_empty() {
            return;
        }
        let id_set: std::collections::HashSet<i64> = ids.iter().copied().collect();

        // Tombstone set so in-flight handlers can bail at their next checkpoint.
        // Queue purge below handles jobs that haven't been dequeued; the set
        // catches the ones already executing.
        let mut conn = self.redis.clone();
        let mut pipe = redis::pipe();
        for id in ids {
            pipe.cmd("SADD").arg(CANCELLED_ITEMS_SET).arg(*id).ignore();
        }
        pipe.cmd("EXPIRE").arg(CANCELLED_ITEMS_SET).arg(600i64).ignore();
        let _: Result<(), _> = pipe.query_async(&mut conn).await;

        // Every queue that carries a `{ "id": <media_item_id>, ... }` payload.
        let configs: [apalis_redis::RedisConfig; 7] = [
            self.index_storage.get_config().clone(),
            self.index_plugin_storage.get_config().clone(),
            self.scrape_storage.get_config().clone(),
            self.scrape_plugin_storage.get_config().clone(),
            self.parse_storage.get_config().clone(),
            self.download_storage.get_config().clone(),
            self.rank_streams_storage.get_config().clone(),
        ];

        for config in &configs {
            if let Err(error) = self.purge_queue_for_ids(config, &id_set).await {
                tracing::warn!(error = %error, queue = %config.job_data_hash(), "failed to purge queue");
            }
        }

        // Dedup keys and flow state live outside apalis-managed keys.
        let mut conn = self.redis.clone();
        for id in ids {
            for prefix in ["index", "scrape", "parse", "download", "rank-streams"] {
                let _: Result<(), _> = redis::cmd("DEL")
                    .arg(dedup_key(prefix, *id))
                    .query_async(&mut conn)
                    .await;
            }
            for prefix in ["scrape", "parse", "index"] {
                let _: Result<(), _> = redis::pipe()
                    .cmd("DEL").arg(flow_pending_key(prefix, *id))
                    .cmd("DEL").arg(flow_results_key(prefix, *id))
                    .cmd("DEL").arg(flow_rate_limited_key(prefix, *id))
                    .query_async(&mut conn)
                    .await;
            }
            let _: Result<(), _> = redis::cmd("DEL")
                .arg(format!("riven:download:rank-result:{id}"))
                .query_async(&mut conn)
                .await;
        }
    }

    async fn purge_queue_for_ids(
        &self,
        config: &apalis_redis::RedisConfig,
        ids: &std::collections::HashSet<i64>,
    ) -> redis::RedisResult<()> {
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

            // HSCAN returns flat [field, value, field, value, ...].
            let mut iter = batch.into_iter();
            while let (Some(task_id), Some(payload)) = (iter.next(), iter.next()) {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) else {
                    continue;
                };
                let Some(id) = value.get("id").and_then(|v| v.as_i64()) else {
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
            pipe.cmd("LREM").arg(&active_list).arg(0).arg(task_id).ignore();
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

    // ── Domain events ─────────────────────────────────────────────────────────

    /// Publish a domain event to the in-process event bus, enabled plugins,
    /// and the UI notification stream when the event is user-visible.
    pub async fn notify(&self, event: RivenEvent) {
        let _ = self.event_tx.send(event.clone());

        if event.event_type().is_ui_streamed()
            && let Ok(json) = serde_json::to_string(&event)
        {
            let _ = self.notification_tx.send(json);
        }

        let registry = Arc::clone(&self.registry);
        tokio::spawn(async move {
            let results = registry.dispatch(&event).await;
            for (plugin_name, result) in results {
                if let Err(error) = result {
                    tracing::error!(plugin = plugin_name, error = %error, "plugin hook failed");
                }
            }
        });
    }

    /// Reload the resolution ranks cache from the DB (call after settings are saved).
    pub async fn reload_resolution_ranks(&self) {
        let ranks = riven_db::repo::load_resolution_ranks(&self.db_pool).await;
        *self.resolution_ranks.write().await = ranks;
    }
}

// ── Redis key helpers ─────────────────────────────────────────────────────────

#[inline]
fn flow_pending_key(prefix: &str, id: i64) -> String {
    format!("riven:flow:{prefix}:{id}:pending")
}

#[inline]
fn flow_results_key(prefix: &str, id: i64) -> String {
    format!("riven:flow:{prefix}:{id}:results")
}

#[inline]
fn flow_rate_limited_key(prefix: &str, id: i64) -> String {
    format!("riven:flow:{prefix}:{id}:rate_limited")
}

const CANCELLED_ITEMS_SET: &str = "riven:cancelled-items";

#[inline]
fn dedup_key(prefix: &str, id: i64) -> String {
    format!("riven:dedup:{prefix}:{id}")
}

// ── Scheduled index task ID ───────────────────────────────────────────────────

const SCHEDULED_INDEX_TASK_NAMESPACE: u128 = 0x524956454e494e44_0000000000000000;

fn scheduled_index_task_id(id: i64) -> Ulid {
    Ulid::from(SCHEDULED_INDEX_TASK_NAMESPACE | id as u64 as u128)
}
