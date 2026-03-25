use async_graphql::{EmptySubscription, MergedObject, Schema};
use plugin_calendar::CalendarQuery;
use plugin_dashboard::DashboardQuery;
use plugin_logs::{LogDirectory, LogsQuery};
use riven_core::plugin::PluginRegistry;
use riven_core::downloader::DownloaderConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

mod helpers;
mod mutations;
mod queries;
mod types;

pub use mutations::MutationRoot;
pub use queries::CoreQuery;

// ── Merged query root ──

#[derive(MergedObject, Default)]
pub struct QueryRoot(CoreQuery, DashboardQuery, LogsQuery, CalendarQuery);

pub type AppSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub fn build_schema(
    db_pool: sqlx::PgPool,
    registry: Arc<PluginRegistry>,
    job_queue: Arc<riven_queue::JobQueue>,
    log_directory: String,
    downloader_config: Arc<RwLock<DownloaderConfig>>,
) -> AppSchema {
    Schema::build(QueryRoot::default(), MutationRoot, EmptySubscription)
        .data(db_pool)
        .data(registry)
        .data(job_queue)
        .data(LogDirectory(log_directory))
        .data(downloader_config)
        .finish()
}
