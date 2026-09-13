use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::Result;
use riven_core::stream_link::LinkRequest;
use riven_core::vfs_layout::VfsLibraryLayout;
use tokio::sync::{Mutex, RwLock, mpsc};

struct MountedVfs {
    path: String,
    session: riven_vfs::BackgroundSession,
}

struct VfsMountConfig {
    vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    filesystem_settings_revision: Arc<AtomicU64>,
    stream_client: reqwest::Client,
    link_request_tx: mpsc::Sender<LinkRequest>,
    usenet: Option<riven_usenet::UsenetStreamer>,
}

/// Owns the active FUSE session and lets runtime settings changes remount it.
pub struct VfsMountManager {
    config: VfsMountConfig,
    mounted: Mutex<Option<MountedVfs>>,
}

impl VfsMountManager {
    pub fn new(
        initial_path: &str,
        vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
        filesystem_settings_revision: Arc<AtomicU64>,
        stream_client: reqwest::Client,
        link_request_tx: mpsc::Sender<LinkRequest>,
        usenet: Option<riven_usenet::UsenetStreamer>,
    ) -> Result<Self> {
        let config = VfsMountConfig {
            vfs_layout,
            filesystem_settings_revision,
            stream_client,
            link_request_tx,
            usenet,
        };
        let mounted = mount_with_config(initial_path, &config)?;

        Ok(Self {
            config,
            mounted: Mutex::new(mounted),
        })
    }

    pub async fn set_mount_path(&self, mount_path: &str) -> Result<()> {
        let mut mounted = self.mounted.lock().await;
        let mount_path = mount_path.trim();
        if mounted
            .as_ref()
            .is_some_and(|active| active.path == mount_path)
        {
            return Ok(());
        }

        let next = mount_with_config(mount_path, &self.config)?;
        let previous = mounted.take().map(|active| active.path);
        *mounted = next;
        if let Some(previous) = previous {
            tracing::info!(path = previous, "unmounted previous VFS");
        }
        Ok(())
    }

    pub async fn unmount(&self) {
        if let Some(active) = self.mounted.lock().await.take() {
            tracing::info!(path = active.path, "unmounting VFS");
            drop(active.session);
        }
    }
}

fn mount_with_config(mount_path: &str, config: &VfsMountConfig) -> Result<Option<MountedVfs>> {
    if mount_path.is_empty() {
        tracing::info!("VFS mount path not configured, skipping VFS");
        return Ok(None);
    }

    let Some(session) = riven_vfs::mount(
        mount_path,
        config.vfs_layout.clone(),
        config.filesystem_settings_revision.clone(),
        config.stream_client.clone(),
        config.link_request_tx.clone(),
        config.usenet.clone(),
    )?
    else {
        return Ok(None);
    };
    Ok(Some(MountedVfs {
        path: mount_path.to_string(),
        session,
    }))
}
