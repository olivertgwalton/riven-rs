pub mod flows;
pub mod worker;

use std::sync::Arc;

use anyhow::Result;
use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;
use apalis_redis::{RedisConfig, RedisStorage};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};

pub use riven_core::downloader::DownloaderConfig;
use riven_core::events::RivenEvent;
use riven_core::plugin::PluginRegistry;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;

// ── Job types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentServiceJob {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _dummy: Option<bool>,
}

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
        }
    }

    pub fn for_season(season: &MediaItem, show_title: String, show_imdb_id: Option<String>) -> Self {
        Self {
            id: season.id,
            item_type: season.item_type,
            imdb_id: show_imdb_id,
            title: show_title,
            season: season.season_number,
            episode: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadJob {
    pub id: i64,
    pub info_hash: String,
    pub magnet: String,
}

// ── JobQueue ─────────────────────────────────────────────────────────────────

pub struct JobQueue {
    pub index_storage: RedisStorage<IndexJob>,
    pub scrape_storage: RedisStorage<ScrapeJob>,
    pub download_storage: RedisStorage<DownloadJob>,
    pub content_storage: RedisStorage<ContentServiceJob>,
    pub redis: redis::aio::ConnectionManager,
    pub registry: Arc<PluginRegistry>,
    pub notification_tx: broadcast::Sender<String>,
    pub db_pool: sqlx::PgPool,
    pub downloader_config: Arc<RwLock<DownloaderConfig>>,
}

impl JobQueue {
    pub async fn new(
        redis_url: &str,
        registry: Arc<PluginRegistry>,
        notification_tx: broadcast::Sender<String>,
        db_pool: sqlx::PgPool,
        downloader_config: DownloaderConfig,
    ) -> Result<Self> {
        // apalis-redis uses its own ConnectionManager for storages
        let apalis_conn = apalis_redis::connect(redis_url).await?;

        let index_storage    = RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:index"));
        let scrape_storage   = RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:scrape"));
        let download_storage = RedisStorage::new_with_config(apalis_conn.clone(), RedisConfig::new("riven:download"));
        let content_storage  = RedisStorage::new_with_config(apalis_conn,         RedisConfig::new("riven:content"));

        // Separate redis ConnectionManager for dedup SET NX operations
        let redis_client = redis::Client::open(redis_url)?;
        let redis = redis::aio::ConnectionManager::new(redis_client).await?;

        Ok(Self {
            index_storage,
            scrape_storage,
            download_storage,
            content_storage,
            redis,
            registry,
            notification_tx,
            db_pool,
            downloader_config: Arc::new(RwLock::new(downloader_config)),
        })
    }

    /// Push an IndexJob with Redis SET NX dedup (TTL 300s).
    pub async fn push_index(&self, job: IndexJob) {
        let key = format!("riven:dedup:index:{}", job.id);
        if self.set_nx(&key, 300).await {
            let mut storage = self.index_storage.clone();
            if let Err(e) = storage.push(job).await {
                tracing::error!(error = %e, "failed to push IndexJob");
            }
        }
    }

    /// Push a ScrapeJob with Redis SET NX dedup (TTL 300s).
    pub async fn push_scrape(&self, job: ScrapeJob) {
        let key = format!("riven:dedup:scrape:{}", job.id);
        if self.set_nx(&key, 300).await {
            let mut storage = self.scrape_storage.clone();
            if let Err(e) = storage.push(job).await {
                tracing::error!(error = %e, "failed to push ScrapeJob");
            }
        }
    }

    /// Push a DownloadJob with Redis SET NX dedup (TTL 300s).
    pub async fn push_download(&self, job: DownloadJob) {
        let key = format!("riven:dedup:download:{}", job.id);
        if self.set_nx(&key, 300).await {
            let mut storage = self.download_storage.clone();
            if let Err(e) = storage.push(job).await {
                tracing::error!(error = %e, "failed to push DownloadJob");
            }
        }
    }

    /// Push a ContentServiceJob (no dedup needed).
    pub async fn push_content_service(&self) {
        let mut storage = self.content_storage.clone();
        if let Err(e) = storage.push(ContentServiceJob { _dummy: None }).await {
            tracing::error!(error = %e, "failed to push ContentServiceJob");
        }
    }

    /// Release the dedup key for a job, allowing it to be re-queued.
    pub async fn release_dedup(&self, prefix: &str, id: i64) {
        let key = format!("riven:dedup:{}:{}", prefix, id);
        let mut conn = self.redis.clone();
        let _: Result<(), _> = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut conn)
            .await;
    }

    /// Dispatch a notification event to plugins and (if notable) to the UI broadcast channel.
    pub async fn notify(&self, event: RivenEvent) {
        let results = self.registry.dispatch(&event).await;
        for (plugin_name, result) in results {
            if let Err(e) = result {
                tracing::error!(plugin = plugin_name, error = %e, "plugin hook failed");
            }
        }
        if event.is_notable() {
            if let Ok(json) = serde_json::to_string(&event) {
                let _ = self.notification_tx.send(json);
            }
        }
    }

    /// Fan out to re-scrape at a lower level when scraping/downloading fails.
    pub async fn fan_out_download(&self, id: i64) {
        let item = match riven_db::repo::get_media_item(&self.db_pool, id).await {
            Ok(Some(item)) => item,
            _ => return,
        };

        match item.item_type {
            riven_core::types::MediaItemType::Show => {
                let show_imdb_id = item.imdb_id.clone();
                if let Ok(seasons) =
                    riven_db::repo::get_requested_seasons_for_show(&self.db_pool, id).await
                {
                    for season in seasons {
                        self.push_scrape(ScrapeJob {
                            id: season.id,
                            item_type: season.item_type,
                            imdb_id: show_imdb_id.clone(),
                            title: season.title.clone(),
                            season: season.season_number,
                            episode: None,
                        })
                        .await;
                    }
                }
            }
            riven_core::types::MediaItemType::Season => {
                let show_imdb_id = if let Some(show_id) = item.parent_id {
                    riven_db::repo::get_media_item(&self.db_pool, show_id)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|s| s.imdb_id)
                } else {
                    None
                };
                if let Ok(episodes) =
                    riven_db::repo::get_incomplete_episodes_for_season(&self.db_pool, id).await
                {
                    for ep in episodes {
                        self.push_scrape(ScrapeJob {
                            id: ep.id,
                            item_type: ep.item_type,
                            imdb_id: show_imdb_id.clone(),
                            title: ep.title.clone(),
                            season: ep.season_number,
                            episode: ep.episode_number,
                        })
                        .await;
                    }
                }
            }
            _ => {}
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Fetch the best non-blacklisted stream for `id` and push a DownloadJob.
    /// Returns `true` if a stream was found, `false` if none remain.
    pub async fn push_download_from_best_stream(&self, id: i64) -> bool {
        if let Some(stream) = riven_db::repo::get_best_stream(&self.db_pool, id)
            .await
            .ok()
            .flatten()
        {
            self.push_download(DownloadJob {
                id,
                magnet: format!("magnet:?xt=urn:btih:{}", stream.info_hash),
                info_hash: stream.info_hash,
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
}

// ── Apalis worker handlers ────────────────────────────────────────────────────

async fn handle_index_job(job: IndexJob, queue: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let id = job.id;
    flows::index_item::run(id, &queue).await;
    queue.release_dedup("index", id).await;
    Ok(())
}

async fn handle_scrape_job(job: ScrapeJob, queue: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let id = job.id;
    flows::scrape_item::run(id, &job, &queue).await;
    queue.release_dedup("scrape", id).await;
    Ok(())
}

async fn handle_download_job(job: DownloadJob, queue: Data<Arc<JobQueue>>) -> Result<(), BoxDynError> {
    let id = job.id;
    flows::download_item::run(id, &job, &queue).await;
    queue.release_dedup("download", id).await;
    Ok(())
}

async fn handle_content_service_job(
    _job: ContentServiceJob,
    queue: Data<Arc<JobQueue>>,
) -> Result<(), BoxDynError> {
    flows::request_content::run(&queue).await;
    Ok(())
}

// ── Monitor factory ───────────────────────────────────────────────────────────

pub fn start_workers(queue: Arc<JobQueue>) -> Monitor {
    Monitor::new()
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-index")
                    .backend(q.index_storage.clone())
                    .concurrency(10)
                    .data(q.clone())
                    .build(handle_index_job)
            }
        })
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-scrape")
                    .backend(q.scrape_storage.clone())
                    .concurrency(20)
                    .data(q.clone())
                    .build(handle_scrape_job)
            }
        })
        .register({
            let q = queue.clone();
            move |_| {
                WorkerBuilder::new("riven-download")
                    .backend(q.download_storage.clone())
                    .concurrency(10)
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
