pub mod discovery;
pub mod flows;
pub mod indexing;
pub mod orchestrator;
pub mod worker;

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;
use apalis_redis::{RedisConfig, RedisStorage};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use ulid::Ulid;

pub use riven_core::downloader::DownloaderConfig;
use riven_core::events::RivenEvent;
use riven_core::plugin::PluginRegistry;
use riven_core::reindex::ReindexConfig;
use riven_core::settings::FilesystemSettings;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;
use riven_rank::ResolutionRanks;

// ── Job types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContentServiceJob {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexJob {
    pub id: i64,
    pub item_type: MediaItemType,
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
}

impl IndexJob {
    pub fn from_item(item: &MediaItem) -> Self {
        Self {
            id: item.id,
            item_type: item.item_type,
            imdb_id: item.imdb_id.clone(),
            tvdb_id: item.tvdb_id.clone(),
            tmdb_id: item.tmdb_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapeJob {
    pub id: i64,
    pub item_type: MediaItemType,
    pub imdb_id: Option<String>,
    pub title: String,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    #[serde(default = "default_true")]
    pub auto_download: bool,
}

const fn default_true() -> bool {
    true
}

impl ScrapeJob {
    pub fn for_movie(item: &MediaItem) -> Self {
        Self {
            id: item.id,
            item_type: item.item_type,
            imdb_id: item.imdb_id.clone(),
            title: item.title.clone(),
            season: None,
            episode: None,
            auto_download: true,
        }
    }

    pub fn for_season(
        season: &MediaItem,
        show_title: String,
        show_imdb_id: Option<String>,
    ) -> Self {
        Self {
            id: season.id,
            item_type: season.item_type,
            imdb_id: show_imdb_id,
            title: show_title,
            season: season.season_number,
            episode: None,
            auto_download: true,
        }
    }

    pub fn for_episode(ep: &MediaItem, show_title: String, show_imdb_id: Option<String>) -> Self {
        Self {
            id: ep.id,
            item_type: ep.item_type,
            imdb_id: show_imdb_id,
            title: show_title,
            season: ep.season_number,
            episode: ep.episode_number,
            auto_download: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadJob {
    pub id: i64,
    pub info_hash: String,
    pub magnet: String,
    #[serde(default)]
    pub preferred_info_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseScrapeResultsJob {
    pub id: i64,
    #[serde(default = "default_true")]
    pub auto_download: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexPluginJob {
    pub id: i64,
    pub plugin_name: String,
    pub item_type: MediaItemType,
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapePluginJob {
    pub id: i64,
    pub plugin_name: String,
    pub item_type: MediaItemType,
    pub imdb_id: Option<String>,
    pub title: String,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    #[serde(default = "default_true")]
    pub auto_download: bool,
}

// ── JobQueue ─────────────────────────────────────────────────────────────────

pub struct JobQueue {
    pub index_storage: RedisStorage<IndexJob>,
    pub index_plugin_storage: RedisStorage<IndexPluginJob>,
    pub scrape_storage: RedisStorage<ScrapeJob>,
    pub scrape_plugin_storage: RedisStorage<ScrapePluginJob>,
    pub parse_storage: RedisStorage<ParseScrapeResultsJob>,
    pub download_storage: RedisStorage<DownloadJob>,
    pub content_storage: RedisStorage<ContentServiceJob>,
    pub redis: redis::aio::ConnectionManager,
    pub registry: Arc<PluginRegistry>,
    pub notification_tx: broadcast::Sender<String>,
    pub db_pool: sqlx::PgPool,
    pub downloader_config: Arc<RwLock<DownloaderConfig>>,
    pub reindex_config: Arc<RwLock<ReindexConfig>>,
    pub filesystem_settings: Arc<RwLock<FilesystemSettings>>,
    /// Cached resolution ranks — loaded once at startup and reloaded on
    /// settings save. Avoids a DB round-trip on every stream fetch.
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
    ) -> Result<Self> {
        // apalis-redis uses its own ConnectionManager for storages
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
        let content_storage =
            RedisStorage::new_with_config(apalis_conn, RedisConfig::new("riven:content"));

        // Separate redis ConnectionManager for dedup SET NX operations
        let redis_client = redis::Client::open(redis_url)?;
        let redis = redis::aio::ConnectionManager::new(redis_client).await?;

        let resolution_ranks = riven_db::repo::load_resolution_ranks(&db_pool).await;

        Ok(Self {
            index_storage,
            index_plugin_storage,
            scrape_storage,
            scrape_plugin_storage,
            parse_storage,
            download_storage,
            content_storage,
            redis,
            registry,
            notification_tx,
            db_pool,
            downloader_config: Arc::new(RwLock::new(downloader_config)),
            reindex_config: Arc::new(RwLock::new(reindex_config)),
            filesystem_settings: Arc::new(RwLock::new(filesystem_settings)),
            resolution_ranks: Arc::new(RwLock::new(resolution_ranks)),
        })
    }

    pub async fn push_index(&self, job: IndexJob) {
        self.push_deduped_job("index", job.id, "IndexJob", || async {
            self.index_storage.clone().push(job).await
        })
        .await;
    }

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
            Ok(payload) => payload,
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
            Ok(()) => tracing::info!(
                id = job.id,
                run_at = %run_at,
                "scheduled delayed index job"
            ),
            Err(error) => tracing::error!(
                id = job.id,
                error = %error,
                "failed to schedule delayed index job"
            ),
        }
    }

    pub async fn push_scrape(&self, job: ScrapeJob) {
        self.push_deduped_job("scrape", job.id, "ScrapeJob", || async {
            self.scrape_storage.clone().push(job).await
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

    pub async fn push_parse_scrape_results(&self, job: ParseScrapeResultsJob) {
        self.push_deduped_job("parse", job.id, "ParseScrapeResultsJob", || async {
            self.parse_storage.clone().push(job).await
        })
        .await;
    }

    pub async fn push_download(&self, job: DownloadJob) {
        self.push_deduped_job("download", job.id, "DownloadJob", || async {
            self.download_storage.clone().push(job).await
        })
        .await;
    }

    pub async fn push_content_service(&self) {
        if let Err(e) = self
            .content_storage
            .clone()
            .push(ContentServiceJob::default())
            .await
        {
            tracing::error!(error = %e, "failed to push ContentServiceJob");
        }
    }

    /// Release the dedup key for a job, allowing it to be re-queued.
    pub async fn release_dedup(&self, prefix: &str, id: i64) {
        let key = format!("riven:dedup:{}:{}", prefix, id);
        let mut conn = self.redis.clone();
        let _: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
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

    /// Dispatch a notification event to plugins and (if notable) to the UI broadcast channel.
    pub async fn notify(&self, event: RivenEvent) {
        let results = self.registry.dispatch(&event).await;
        for (plugin_name, result) in results {
            if let Err(e) = result {
                tracing::error!(plugin = plugin_name, error = %e, "plugin hook failed");
            }
        }
        if event.event_type().is_ui_streamed() {
            if let Ok(json) = serde_json::to_string(&event) {
                let _ = self.notification_tx.send(json);
            }
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Reload the resolution ranks cache from the DB (call after settings are saved).
    pub async fn reload_resolution_ranks(&self) {
        let ranks = riven_db::repo::load_resolution_ranks(&self.db_pool).await;
        *self.resolution_ranks.write().await = ranks;
    }

    /// Fetch the best non-blacklisted stream for `id` and push a DownloadJob.
    /// Returns `true` if a stream was found, `false` if none remain.
    pub async fn push_download_from_best_stream(&self, id: i64) -> bool {
        let ranks = self.resolution_ranks.read().await.clone();
        if let Some(stream) = riven_db::repo::get_best_stream(&self.db_pool, id, &ranks)
            .await
            .ok()
            .flatten()
        {
            self.push_download(DownloadJob {
                id,
                magnet: format!("magnet:?xt=urn:btih:{}", stream.info_hash),
                info_hash: stream.info_hash,
                preferred_info_hash: None,
            })
            .await;
            true
        } else {
            false
        }
    }

    /// SET NX with EX. Returns true if the key was set (acquired), false if already set (duplicate).
    async fn set_nx(&self, key: &str, ttl_secs: usize) -> bool {
        let mut conn = self.redis.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .unwrap_or(None);
        result.is_some()
    }

    pub async fn init_flow(&self, prefix: &str, id: i64, pending: usize) {
        let pending_key = format!("riven:flow:{prefix}:{id}:pending");
        if let Err(error) = riven_db::repo::clear_flow_artifacts(&self.db_pool, prefix, id).await {
            tracing::error!(prefix, id, error = %error, "failed to clear stale flow artifacts");
        }
        let mut conn = self.redis.clone();
        let _: Result<(), _> = redis::pipe()
            .atomic()
            .set(&pending_key, pending)
            .expire(&pending_key, 3600)
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
        let Ok(payload) = serde_json::to_value(value) else {
            tracing::error!(prefix, id, field, "failed to serialize flow result");
            return;
        };

        if let Err(error) =
            riven_db::repo::upsert_flow_artifact(&self.db_pool, prefix, id, field, payload).await
        {
            tracing::error!(prefix, id, field, error = %error, "failed to store flow result");
        }
    }

    pub async fn flow_complete_child(&self, prefix: &str, id: i64) -> bool {
        let pending_key = format!("riven:flow:{prefix}:{id}:pending");
        let mut conn = self.redis.clone();
        let remaining: i64 = redis::cmd("DECR")
            .arg(&pending_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(-1);
        let _: Result<(), _> = redis::cmd("EXPIRE")
            .arg(&pending_key)
            .arg(3600)
            .query_async(&mut conn)
            .await;
        remaining == 0
    }

    pub async fn flow_load_results<T: DeserializeOwned>(&self, prefix: &str, id: i64) -> Vec<T> {
        let values = match riven_db::repo::load_flow_artifacts(&self.db_pool, prefix, id).await {
            Ok(values) => values,
            Err(error) => {
                tracing::error!(prefix, id, error = %error, "failed to load flow results");
                vec![]
            }
        };

        values
            .into_iter()
            .filter_map(|value| match serde_json::from_value(value) {
                Ok(parsed) => Some(parsed),
                Err(e) => {
                    tracing::error!(prefix, id, error = %e, "failed to deserialize flow result");
                    None
                }
            })
            .collect()
    }

    pub async fn clear_flow(&self, prefix: &str, id: i64) {
        let pending_key = format!("riven:flow:{prefix}:{id}:pending");
        let mut conn = self.redis.clone();
        let _: Result<(), _> = redis::pipe()
            .atomic()
            .del(&pending_key)
            .query_async(&mut conn)
            .await;
    }

    pub async fn clear_flow_results(&self, prefix: &str, id: i64) {
        if let Err(error) = riven_db::repo::clear_flow_artifacts(&self.db_pool, prefix, id).await {
            tracing::error!(prefix, id, error = %error, "failed to clear flow results");
        }
    }

    pub async fn flow_result_count(&self, prefix: &str, id: i64) -> i64 {
        match riven_db::repo::count_flow_artifacts(&self.db_pool, prefix, id).await {
            Ok(count) => count,
            Err(error) => {
                tracing::error!(prefix, id, error = %error, "failed to count flow results");
                0
            }
        }
    }

    async fn push_deduped_job<F, Fut, E>(&self, prefix: &str, id: i64, label: &'static str, push: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Display,
    {
        if self
            .set_nx(&format!("riven:dedup:{prefix}:{id}"), 300)
            .await
        {
            if let Err(e) = push().await {
                tracing::error!(error = %e, label, "failed to push job");
            }
        }
    }
}

// ── Apalis worker handlers ────────────────────────────────────────────────────

async fn handle_index_job(job: IndexJob, queue: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    flows::index_item::run(&job, &queue).await;
    Ok(())
}

async fn handle_index_plugin_job(
    job: IndexPluginJob,
    queue: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    flows::index_item::run_plugin(&job, &queue).await;
    Ok(())
}

async fn handle_scrape_job(job: ScrapeJob, queue: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    flows::scrape_item::run(job.id, &job, &queue).await;
    Ok(())
}

async fn handle_scrape_plugin_job(
    job: ScrapePluginJob,
    queue: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    flows::scrape_item::run_plugin(&job, &queue).await;
    Ok(())
}

async fn handle_parse_scrape_results_job(
    job: ParseScrapeResultsJob,
    queue: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    run_deduped_job("parse", job.id, queue, move |queue| async move {
        flows::parse_scrape_results::run(job.id, &job, &queue).await;
    })
    .await
}

async fn handle_download_job(
    job: DownloadJob,
    queue: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    run_deduped_job("download", job.id, queue, move |queue| async move {
        flows::download_item::run(job.id, &job, &queue).await;
    })
    .await
}

async fn handle_content_service_job(
    _job: ContentServiceJob,
    queue: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    flows::request_content::run(&queue).await;
    Ok(())
}

async fn run_deduped_job<F, Fut>(
    prefix: &'static str,
    id: i64,
    queue: Data<Arc<JobQueue>>,
    run: F,
) -> Result<(), BoxDynError>
where
    F: FnOnce(Arc<JobQueue>) -> Fut,
    Fut: Future<Output = ()>,
{
    run(Arc::clone(&queue)).await;
    queue.release_dedup(prefix, id).await;
    Ok(())
}

const SCHEDULED_INDEX_TASK_NAMESPACE: u128 = 0x524956454e494e44_0000000000000000;

fn scheduled_index_task_id(id: i64) -> Ulid {
    Ulid::from(SCHEDULED_INDEX_TASK_NAMESPACE | id as u64 as u128)
}

// ── Monitor factory ───────────────────────────────────────────────────────────

/// Queue names used by apalis-redis (must match `RedisConfig::new` calls).
const QUEUE_NAMES: &[&str] = &[
    "riven:index",
    "riven:index-plugin",
    "riven:scrape",
    "riven:scrape-plugin",
    "riven:parse",
    "riven:download",
    "riven:content",
];

/// Remove stale worker registrations and dedup keys from Redis so the monitor
/// can restart cleanly. Clearing dedup keys ensures the scheduler can
/// immediately re-queue any jobs that were inflight when the monitor exited.
pub async fn clear_worker_registrations(redis: &mut redis::aio::ConnectionManager) {
    for queue in QUEUE_NAMES {
        let workers_set = format!("{queue}:workers");
        let members: Vec<String> = redis::cmd("ZRANGE")
            .arg(&workers_set)
            .arg(0i64)
            .arg(-1i64)
            .query_async(redis)
            .await
            .unwrap_or_default();

        if !members.is_empty() {
            for member in &members {
                let meta_key = format!("core::apalis::workers:metadata::{member}");
                let inflight_key = format!("{queue}:inflight:{member}");
                let _: Result<(), _> = redis::pipe()
                    .del(&meta_key)
                    .del(&inflight_key)
                    .query_async(redis)
                    .await;
            }
            let _: Result<(), _> = redis::cmd("DEL").arg(&workers_set).query_async(redis).await;

            tracing::info!(
                queue = queue,
                "cleared {} stale worker registrations",
                members.len()
            );
        }
    }

    // Clear all dedup keys. Inflight jobs whose workers just died will have
    // their dedup keys expire normally (300s TTL) unless we clear them here,
    // blocking the scheduler from re-queuing for up to 5 minutes.
    let dedup_keys: Vec<String> = {
        let mut cursor = 0u64;
        let mut keys = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("riven:dedup:*")
                .arg("COUNT")
                .arg(100u32)
                .query_async(redis)
                .await
                .unwrap_or((0, vec![]));
            keys.extend(batch);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        keys
    };
    if !dedup_keys.is_empty() {
        let _: Result<(), _> = redis::cmd("DEL").arg(&dedup_keys).query_async(redis).await;
        tracing::info!("cleared {} stale dedup keys", dedup_keys.len());
    }
}

const COMPLETED_JOB_MAX_AGE_SECS: i64 = 60 * 60 * 6;
const FAILED_JOB_MAX_AGE_SECS: i64 = 60 * 60 * 24;
const COMPLETED_JOB_MAX_COUNT: isize = 500;
const FAILED_JOB_MAX_COUNT: isize = 5_000;

async fn prune_queue_set(
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

pub async fn prune_queue_history(redis: &mut redis::aio::ConnectionManager) {
    for queue in QUEUE_NAMES {
        let config = RedisConfig::new(queue);
        let done = prune_queue_set(
            redis,
            &config.done_jobs_set(),
            &config.job_data_hash(),
            &config.job_meta_hash(),
            COMPLETED_JOB_MAX_AGE_SECS,
            COMPLETED_JOB_MAX_COUNT,
        )
        .await;
        let failed = prune_queue_set(
            redis,
            &config.failed_jobs_set(),
            &config.job_data_hash(),
            &config.job_meta_hash(),
            FAILED_JOB_MAX_AGE_SECS,
            FAILED_JOB_MAX_COUNT,
        )
        .await;
        let dead = prune_queue_set(
            redis,
            &config.dead_jobs_set(),
            &config.job_data_hash(),
            &config.job_meta_hash(),
            FAILED_JOB_MAX_AGE_SECS,
            FAILED_JOB_MAX_COUNT,
        )
        .await;

        if done + failed + dead > 0 {
            tracing::info!(queue, done, failed, dead, "pruned redis job history");
        }
    }
}

pub fn start_workers(queue: Arc<JobQueue>) -> Monitor {
    // Mirror riven-ts: scale worker concurrency to the number of logical CPUs,
    // matching `os.availableParallelism()` used in createFlowWorker /
    // createSandboxedWorker. Content is a singleton scheduler job so stays at 1.
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    Monitor::new()
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-index")
                    .backend(q.index_storage.clone())
                    .concurrency(parallelism)
                    .data(q.clone())
                    .build(handle_index_job)
            }
        })
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-index-plugin")
                    .backend(q.index_plugin_storage.clone())
                    .concurrency(parallelism)
                    .data(q.clone())
                    .build(handle_index_plugin_job)
            }
        })
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-scrape")
                    .backend(q.scrape_storage.clone())
                    .concurrency(parallelism)
                    .data(q.clone())
                    .build(handle_scrape_job)
            }
        })
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-scrape-plugin")
                    .backend(q.scrape_plugin_storage.clone())
                    .concurrency(parallelism)
                    .data(q.clone())
                    .build(handle_scrape_plugin_job)
            }
        })
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-parse")
                    .backend(q.parse_storage.clone())
                    .concurrency(parallelism)
                    .data(q.clone())
                    .build(handle_parse_scrape_results_job)
            }
        })
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-download")
                    .backend(q.download_storage.clone())
                    .concurrency(parallelism)
                    .data(q.clone())
                    .build(handle_download_job)
            }
        })
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-content")
                    .backend(q.content_storage.clone())
                    .concurrency(1)
                    .data(q.clone())
                    .build(handle_content_service_job)
            }
        })
}
