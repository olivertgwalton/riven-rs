pub mod cache;
pub mod chunks;
pub mod detect;
pub mod filesystem;
pub mod link;
pub mod media_stream;
pub mod path_info;
pub mod prefetch;
pub mod readdir;
pub mod stream;

use std::path::Path;

use anyhow::Result;
use riven_core::vfs_layout::VfsLibraryLayout;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::RwLock;
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
    vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    filesystem_settings_revision: Arc<AtomicU64>,
    db_pool: sqlx::PgPool,
    stream_client: reqwest::Client,
    link_request_tx: mpsc::Sender<LinkRequest>,
    debug_logging: bool,
    cache_max_size_mb: u64,
) -> Result<FuseSession> {
    let mount_path = Path::new(mount_path);

    if !mount_path.exists() {
        std::fs::create_dir_all(mount_path)?;
    } else {
        // Attempt lazy unmount only if a FUSE filesystem is already mounted here,
        // to avoid accidentally unmounting a legitimate bind mount (e.g. Docker's
        // rshared bind mount), which would break mount propagation to the host.
        let path_str = mount_path.to_str().unwrap_or_default();
        let is_fuse_mounted = std::fs::read_to_string("/proc/self/mounts")
            .map(|m| {
                m.lines().any(|line| {
                    let mut parts = line.splitn(4, ' ');
                    let _ = parts.next(); // device
                    let mountpoint = parts.next().unwrap_or("");
                    let fstype = parts.next().unwrap_or("");
                    mountpoint == path_str && fstype.starts_with("fuse")
                })
            })
            .unwrap_or(false);

        if is_fuse_mounted {
            let ok = std::process::Command::new("fusermount")
                .args(["-u", "-z", path_str])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !ok {
                let _ = std::process::Command::new("umount")
                    .args(["-l", path_str])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        }
    }

    let fs = RivenFs::new(
        vfs_layout,
        filesystem_settings_revision,
        db_pool,
        stream_client,
        link_request_tx,
        debug_logging,
        cache_max_size_mb,
    );

    let options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("riven".to_string()),
        fuser::MountOption::AllowOther,
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::DefaultPermissions,
        // Allow the kernel to issue up to 1 MB reads per FUSE call instead of
        // the default 128 KB. Combined with the large readahead set in init(),
        // this reduces block_on() call frequency ~8x for sequential playback.
        fuser::MountOption::CUSTOM("max_read=1048576".to_string()),
    ];

    let session = fuser::spawn_mount2(fs, mount_path, &options)?;
    tracing::info!(path = %mount_path.display(), "VFS mounted");

    Ok(FuseSession { session })
}
