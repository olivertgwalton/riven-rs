//! Keeps the symlink tree in step with the library.
//!
//! Two things move the tree: the library changing, and the reader changing the
//! filesystem settings. Both already bump `filesystem_settings_revision`, so
//! that counter is the signal -- the same one the VFS refreshes its caches
//! from. A download burst bumps it once per file, so a change is allowed to
//! settle before the tree is walked.
//!
//! A slow sweep runs underneath that, because not every way an entry leaves the
//! library bumps the counter: the usenet health check deletes a corrupt entry
//! straight through the repository, with no queue in reach to report it. The
//! sweep is one query and a walk of a tree that is almost always already
//! correct, so it is cheap enough to be the backstop rather than a thing to
//! reason about.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use riven_core::settings::FilesystemSettings;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_vfs::symlink;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// How often the revision is read.
const POLL: Duration = Duration::from_secs(5);
/// Ticks a revision must hold still before the tree is walked, so a season
/// landing file by file is one reconcile rather than ten.
const SETTLE_TICKS: u32 = 2;
/// Ticks between sweeps that run whether or not the revision moved.
const SWEEP_TICKS: u32 = 360; // 30 minutes at a 5 s poll.

pub struct SymlinkSyncConfig {
    pub filesystem_settings: Arc<RwLock<FilesystemSettings>>,
    pub vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    pub filesystem_settings_revision: Arc<AtomicU64>,
    pub cancel: CancellationToken,
}

/// Reconcile once, now. Returns `Ok(None)` when the feature is switched off.
pub async fn reconcile_once(
    filesystem_settings: &RwLock<FilesystemSettings>,
    vfs_layout: &RwLock<VfsLibraryLayout>,
) -> anyhow::Result<Option<symlink::SymlinkStats>> {
    let (symlink_path, mount_path) = {
        let settings = filesystem_settings.read().await;
        (
            settings.symlink_path.trim().to_owned(),
            settings.mount_path.trim().to_owned(),
        )
    };
    if symlink_path.is_empty() {
        return Ok(None);
    }
    // Cloned rather than held: the reconcile awaits a query and a blocking
    // walk, and holding a read guard across those would block every settings
    // write for the length of a tree walk.
    let layout = vfs_layout.read().await.clone();
    let stats = symlink::reconcile(&layout, &symlink_path, &mount_path).await?;
    Ok(Some(stats))
}

/// Run the reconciler until cancelled.
pub async fn run(config: SymlinkSyncConfig) {
    let SymlinkSyncConfig {
        filesystem_settings,
        vfs_layout,
        filesystem_settings_revision,
        cancel,
    } = config;

    // The tree is built from the database rather than remembered, so a first
    // pass at boot is what recovers from anything missed while the process was
    // not running.
    report(
        reconcile_once(&filesystem_settings, &vfs_layout).await,
        "startup",
    );

    let mut last_reconciled = filesystem_settings_revision.load(Ordering::SeqCst);
    let mut pending: Option<u64> = None;
    let mut settled_for = 0u32;
    let mut since_sweep = 0u32;

    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(POLL) => {}
        }

        since_sweep += 1;
        let revision = filesystem_settings_revision.load(Ordering::SeqCst);

        if revision != last_reconciled {
            if pending == Some(revision) {
                settled_for += 1;
            } else {
                pending = Some(revision);
                settled_for = 0;
            }
            if settled_for < SETTLE_TICKS {
                continue;
            }
            report(
                reconcile_once(&filesystem_settings, &vfs_layout).await,
                "library changed",
            );
            last_reconciled = revision;
            pending = None;
            settled_for = 0;
            since_sweep = 0;
        } else if since_sweep >= SWEEP_TICKS {
            report(
                reconcile_once(&filesystem_settings, &vfs_layout).await,
                "sweep",
            );
            since_sweep = 0;
        }
    }
}

/// A pass that changed nothing is not worth a line; steady state is most of
/// them.
fn report(result: anyhow::Result<Option<symlink::SymlinkStats>>, reason: &str) {
    match result {
        Ok(None) => {}
        Ok(Some(stats)) if stats.is_noop() => {
            tracing::debug!(reason, "symlink tree already in step");
        }
        Ok(Some(stats)) => tracing::info!(
            reason,
            created = stats.links_created,
            repointed = stats.links_repointed,
            removed = stats.links_removed,
            dirs_created = stats.dirs_created,
            dirs_removed = stats.dirs_removed,
            conflicts = stats.conflicts,
            orphans_kept = stats.orphans_kept,
            "reconciled symlink tree"
        ),
        Err(error) => tracing::error!(reason, %error, "failed to reconcile symlink tree"),
    }
}
