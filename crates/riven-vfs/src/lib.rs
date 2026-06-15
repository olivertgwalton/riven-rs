pub mod cache;
pub mod chunks;
pub mod detect;
pub mod filesystem;
pub mod media_stream;
pub mod path_info;
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

pub struct FuseSession {
    session: fuser::BackgroundSession,
}

impl FuseSession {
    pub fn join(self) {
        let _result = self.session.join();
    }
}

/// Start the FUSE virtual filesystem.
///
/// Returns `Ok(None)` if `mount_path` does not exist — the caller treats this
/// as "skip VFS for now" rather than auto-creating a directory that may be a
/// host-managed bind mount not yet ready.
pub fn mount(
    mount_path: &str,
    vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    filesystem_settings_revision: Arc<AtomicU64>,
    db_pool: sqlx::PgPool,
    stream_client: reqwest::Client,
    link_request_tx: mpsc::Sender<riven_core::stream_link::LinkRequest>,
    cache_max_size_mb: u64,
    local_source: Option<Arc<dyn riven_core::local_source::LocalByteSource>>,
) -> Result<Option<FuseSession>> {
    let mount_path = Path::new(mount_path);

    if !mount_path.exists() {
        tracing::warn!(
            path = %mount_path.display(),
            "VFS mount path does not exist; skipping VFS mount"
        );
        return Ok(None);
    }

    {
        // Lazy-unmount only if a FUSE filesystem is already mounted here:
        // unmounting a legitimate bind mount (e.g. Docker rshared) would break
        // mount propagation to the host.
        let path_str = mount_path.to_str().unwrap_or_default();
        let is_fuse_mounted = std::fs::read_to_string("/proc/self/mounts").is_ok_and(|m| {
            m.lines().any(|line| {
                let mut parts = line.splitn(4, ' ');
                let _ = parts.next();
                let mountpoint = parts.next().unwrap_or("");
                let fstype = parts.next().unwrap_or("");
                mountpoint == path_str && fstype.starts_with("fuse")
            })
        });

        if is_fuse_mounted {
            let ok = std::process::Command::new("fusermount")
                .args(["-u", "-z", path_str])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            if !ok {
                drop(
                    std::process::Command::new("umount")
                        .args(["-l", path_str])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status(),
                );
            }
        } else if mount_path.read_dir()?.next().is_some() {
            anyhow::bail!(
                "refusing to mount VFS over non-empty directory {}; choose an empty mount directory such as /mnt/riven",
                mount_path.display()
            );
        }
    }

    let fs = RivenFs::new(
        vfs_layout,
        filesystem_settings_revision,
        db_pool,
        stream_client,
        link_request_tx,
        cache_max_size_mb,
        local_source,
    );

    let mut config = fuser::Config::default();
    config.acl = fuser::SessionACL::All;
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("riven".to_string()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::CUSTOM("max_read=4194304".to_string()),
    ];
    let session = fuser::spawn_mount2(fs, mount_path, &config)?;
    tracing::info!(path = %mount_path.display(), "VFS mounted");

    Ok(Some(FuseSession { session }))
}
