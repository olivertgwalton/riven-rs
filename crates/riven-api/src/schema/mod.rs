use crate::vfs_mount::VfsMountManager;
use async_graphql::{MergedObject, Schema};
use plugin_calendar::CalendarQuery;
use plugin_dashboard::DashboardQuery;
use riven_core::downloader::DownloaderConfig;
use riven_core::http::HttpClient;
use riven_core::logging::LogControl;
use riven_core::plugin::PluginRegistry;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) mod auth;
pub mod discovery;
mod helpers;
mod metadata;
mod mutations;
mod queries;
mod subscriptions;
pub mod typed_items;
pub mod types;
mod vfs;

pub use mutations::MutationRoot;
pub use queries::CoreQuery;
pub use subscriptions::SubscriptionRoot;
pub use vfs::VfsQuery;

#[derive(MergedObject, Default)]
pub struct QueryRoot(CoreQuery, DashboardQuery, CalendarQuery, VfsQuery);

pub type AppSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub fn build_schema(
    registry: Arc<PluginRegistry>,
    job_queue: Arc<riven_queue::JobQueue>,
    http_client: HttpClient,
    log_directory: String,
    downloader_config: Arc<RwLock<DownloaderConfig>>,
    log_control: Arc<LogControl>,
    log_tx: tokio::sync::broadcast::Sender<String>,
    vfs_mount_manager: Arc<VfsMountManager>,
) -> AppSchema {
    let builder = Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .data(registry)
    .data(job_queue)
    .data(http_client)
    .data(downloader_config)
    .data(log_control)
    .data(log_tx)
    .data(vfs_mount_manager);
    let builder = queries::logs::register_with_schema(builder, log_directory);
    let builder = plugin_dashboard::register_with_schema(builder);
    builder.finish()
}
