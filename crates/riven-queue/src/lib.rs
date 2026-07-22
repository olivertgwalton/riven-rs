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

mod cancellation;
mod dispatch;
mod flow;
mod scheduling;
mod storage;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};

use anyhow::Result;
use apalis::prelude::{TaskBuilder, TaskId, TaskSink};
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
    RecoveryReport, clear_worker_registrations, prune_queue_history, purge_orphaned_active_jobs,
    purge_orphaned_worker_sets, purge_stale_dedup_keys, reconcile_library_profiles,
    recover_stale_workers,
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
