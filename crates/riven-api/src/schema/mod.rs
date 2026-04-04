use async_graphql::{MergedObject, Schema};
use plugin_calendar::CalendarQuery;
use plugin_dashboard::DashboardQuery;
use plugin_logs::{LogDirectory, LogsQuery};
use riven_core::downloader::DownloaderConfig;
use riven_core::plugin::PluginRegistry;
use std::sync::Arc;
use tokio::sync::RwLock;

mod helpers;
mod mutations;
mod queries;
mod subscriptions;
mod types;

pub use mutations::MutationRoot;
pub use queries::CoreQuery;
pub use subscriptions::SubscriptionRoot;

// ── Merged query root ──

#[derive(MergedObject, Default)]
pub struct QueryRoot(CoreQuery, DashboardQuery, LogsQuery, CalendarQuery);

pub type AppSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub fn build_schema(
    db_pool: sqlx::PgPool,
    registry: Arc<PluginRegistry>,
    job_queue: Arc<riven_queue::JobQueue>,
    log_directory: String,
    downloader_config: Arc<RwLock<DownloaderConfig>>,
) -> AppSchema {
    Schema::build(QueryRoot::default(), MutationRoot, SubscriptionRoot)
        .data(db_pool)
        .data(registry)
        .data(job_queue)
        .data(LogDirectory(log_directory))
        .data(downloader_config)
        .finish()
}
