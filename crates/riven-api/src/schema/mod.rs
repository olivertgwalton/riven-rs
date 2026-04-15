use async_graphql::{MergedObject, Schema};
use plugin_calendar::CalendarQuery;
use plugin_dashboard::{DashboardQuery, PlaybackSessionsCache};
use plugin_logs::{LogControl, LogDirectory, LogsQuery};
use riven_core::downloader::DownloaderConfig;
use riven_core::plugin::PluginRegistry;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod discovery;
mod event_controller;
mod helpers;
mod mutations;
pub mod plugins;
mod queries;
mod subscriptions;
pub mod typed_items;
pub mod types;
mod vfs;

pub use event_controller::start as start_event_controller;
pub use mutations::MutationRoot;
pub use queries::CoreQuery;
pub use subscriptions::SubscriptionRoot;
pub use vfs::VfsQuery;

// ── Merged query root ──

#[derive(MergedObject, Default)]
pub struct QueryRoot(
    CoreQuery,
    DashboardQuery,
    LogsQuery,
    CalendarQuery,
    VfsQuery,
    plugins::PluginsQuery,
);

pub type AppSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub fn build_schema(
    db_pool: sqlx::PgPool,
    registry: Arc<PluginRegistry>,
    job_queue: Arc<riven_queue::JobQueue>,
    log_directory: String,
    downloader_config: Arc<RwLock<DownloaderConfig>>,
    log_control: Arc<LogControl>,
    log_tx: tokio::sync::broadcast::Sender<String>,
) -> AppSchema {
    Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot,
    )
    .data(db_pool)
    .data(registry)
    .data(job_queue)
    .data(LogDirectory(log_directory))
    .data(Arc::new(PlaybackSessionsCache::default()))
    .data(downloader_config)
    .data(log_control)
    .data(log_tx)
    .finish()
}
