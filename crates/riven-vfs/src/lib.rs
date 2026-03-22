pub mod chunks;
pub mod detect;
pub mod filesystem;
pub mod path_info;
pub mod prefetch;
pub mod stream;

use std::path::Path;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::filesystem::RivenFs;

/// Request to resolve a stream link for a file.
#[derive(Debug)]
pub struct LinkRequest {
    pub download_url: String,
    pub response_tx: tokio::sync::oneshot::Sender<Option<String>>,
}

pub struct FuseSession {
    session: fuser::BackgroundSession,
}

impl FuseSession {
    pub fn join(self) {
        self.session.join();
    }
}

/// Start the FUSE virtual filesystem.
pub fn mount(
    mount_path: &str,
    db_pool: sqlx::PgPool,
    http_client: reqwest::Client,
    link_request_tx: mpsc::Sender<LinkRequest>,
    debug_logging: bool,
    cache_max_size_mb: u64,
) -> Result<FuseSession> {
    let mount_path = Path::new(mount_path);

    if !mount_path.exists() {
        std::fs::create_dir_all(mount_path)?;
    }

    let fs = RivenFs::new(db_pool, http_client, link_request_tx, debug_logging, cache_max_size_mb);

    let options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("riven".to_string()),
        fuser::MountOption::AllowOther,
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::DefaultPermissions,
    ];

    let session = fuser::spawn_mount2(fs, mount_path, &options)?;
    tracing::info!(path = %mount_path.display(), "VFS mounted");

    Ok(FuseSession { session })
}
