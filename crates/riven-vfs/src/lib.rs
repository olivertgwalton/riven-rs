pub mod filesystem;
pub mod path_info;
pub mod prefetch;
pub mod query;
pub mod readdir;
pub mod source;
mod state;
pub mod symlink;

use std::path::Path;

use anyhow::Result;
use riven_core::vfs_layout::VfsLibraryLayout;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use crate::filesystem::RivenFs;

/// Whether a FUSE filesystem already occupies `path`.
///
/// A live mount is listed in `/proc/self/mounts`. One left behind by a run that
/// died is listed too, but answers every read with `ENOTCONN` -- and where the
/// mount was made in another namespace and reaches this process through a bind,
/// it may not be listed at all. So the directory is asked as well as the table.
fn occupied_by_fuse(path: &Path, path_str: &str) -> bool {
    let listed = std::fs::read_to_string("/proc/self/mounts").is_ok_and(|mounts| {
        mounts.lines().any(|line| {
            let mut parts = line.splitn(4, ' ');
            let _ = parts.next();
            let mountpoint = parts.next().unwrap_or("");
            let fstype = parts.next().unwrap_or("");
            mountpoint == path_str && fstype.starts_with("fuse")
        })
    });
    listed || is_disconnected(path)
}

/// `ENOTCONN` is what a FUSE mount whose server has gone answers to everything.
fn is_disconnected(path: &Path) -> bool {
    std::fs::read_dir(path)
        .err()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotConnected)
}

/// Release a FUSE mount left at `path`.
///
/// The unmount helper is `fusermount3` under libfuse 3 and `fusermount` under
/// 2, and which one exists is the image's business rather than this crate's --
/// asking only for the latter is how this came to do nothing at all on an
/// Alpine image built against fuse3. `umount` is the last resort and needs
/// privileges this process, which drops to a non-root user, will usually not
/// have.
///
/// A failure is reported rather than swallowed. Mounting over a mountpoint
/// something else still occupies fails afterwards with `user has no write
/// access to mountpoint`, which points at permissions and says nothing about
/// the mount that is actually in the way.
fn release_stale_mount(path: &str) -> Result<()> {
    let attempts: [(&str, &[&str]); 3] = [
        ("fusermount3", &["-u", "-z", path]),
        ("fusermount", &["-u", "-z", path]),
        ("umount", &["-l", path]),
    ];

    let mut failures = Vec::new();
    for (program, args) in attempts {
        match std::process::Command::new(program)
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
        {
            Ok(output) if output.status.success() => {
                tracing::info!(path, program, "released a previous VFS mount");
                return Ok(());
            }
            Ok(output) => failures.push(format!(
                "{program}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )),
            Err(error) => failures.push(format!("{program}: {error}")),
        }
    }

    anyhow::bail!(
        "a previous VFS mount is still attached at {path} and could not be released ({}). \
         It belongs to a run that has gone; clear it on the host with \
         `umount -l {path}` and start again.",
        failures.join("; ")
    )
}

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
    stream_client: reqwest::Client,
    link_request_tx: mpsc::Sender<riven_core::stream_link::LinkRequest>,
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
        // Clear a previous mount before mounting over it, but only a FUSE one:
        // unmounting a legitimate bind mount (e.g. Docker rshared) would break
        // mount propagation to the host.
        let path_str = mount_path.to_str().unwrap_or_default();
        if occupied_by_fuse(mount_path, path_str) {
            release_stale_mount(path_str)?;
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
        stream_client,
        link_request_tx,
        local_source,
    );

    let mut config = fuser::Config::default();
    config.acl = fuser::SessionACL::All;
    // Network-backed reads may spend time waiting on an upstream CDN. Keep
    // several kernel request loops available so metadata and unrelated files
    // continue to make progress while a read is outstanding. A cloned FUSE
    // fd avoids contention between those loops on Linux 4.5+.
    config.n_threads = Some(4);
    config.clone_fd = cfg!(target_os = "linux");
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("riven".to_string()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::DefaultPermissions,
        // Cap one kernel request at 1 MiB. This is deliberately *below* the
        // prefetcher's cache unit (8 MiB for HTTP sources): a request larger
        // than a unit would stall the reply while several units are fetched,
        // while smaller ones are served straight from cache once the unit
        // lands. Smaller still would only add avoidable FUSE round trips.
        fuser::MountOption::CUSTOM("max_read=1048576".to_string()),
    ];
    let session = fuser::spawn_mount(fs, mount_path, &config)?;
    tracing::info!(path = %mount_path.display(), "VFS mounted");

    Ok(Some(FuseSession { session }))
}
