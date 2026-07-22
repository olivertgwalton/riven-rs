use std::sync::Arc;

use async_graphql::*;
use chrono::{DateTime, Utc};
use riven_db::entities::FileSystemEntry;
use riven_queue::JobQueue;
use riven_vfs::query::{self, EntryStat};

/// Filesystem stat metadata for a VFS path.
#[derive(SimpleObject)]
pub struct VfsEntryStat {
    pub mtime: DateTime<Utc>,
    pub ctime: DateTime<Utc>,
    pub atime: DateTime<Utc>,
    /// Unix file mode (e.g. 0o040755 for directory, 0o100644 for regular file).
    pub mode: i32,
    /// Number of hard links.
    pub nlink: i32,
    /// File size in bytes (0 for directories).
    pub size: i64,
    pub uid: i32,
    pub gid: i32,
}

impl From<EntryStat> for VfsEntryStat {
    fn from(stat: EntryStat) -> Self {
        Self {
            mtime: stat.mtime,
            ctime: stat.ctime,
            atime: stat.atime,
            mode: stat.mode,
            nlink: stat.nlink,
            size: stat.size,
            uid: stat.uid,
            gid: stat.gid,
        }
    }
}

#[derive(Default)]
pub struct VfsQuery;

#[Object]
impl VfsQuery {
    /// Get filesystem stat info for a VFS path (file or directory).
    async fn vfs_entry_stat(&self, ctx: &Context<'_>, path: String) -> Result<VfsEntryStat> {
        let layout = ctx.data::<Arc<JobQueue>>()?.vfs_layout.read().await.clone();
        query::entry_stat(&layout, &path)
            .await
            .map(Into::into)
            .map_err(|error| Error::new(error.to_string()))
    }

    /// Get the filesystem entry (media file record) for a VFS file path.
    async fn vfs_entry(&self, ctx: &Context<'_>, path: String) -> Result<Option<FileSystemEntry>> {
        let layout = ctx.data::<Arc<JobQueue>>()?.vfs_layout.read().await.clone();
        query::entry(&layout, &path)
            .await
            .map_err(|error| Error::new(error.to_string()))
    }

    /// List child entry names (file or directory names) directly under a VFS path.
    async fn vfs_directory_entry_paths(
        &self,
        ctx: &Context<'_>,
        path: String,
    ) -> Result<Vec<String>> {
        let layout = ctx.data::<Arc<JobQueue>>()?.vfs_layout.read().await.clone();
        query::directory_entry_paths(&layout, &path)
            .await
            .map_err(|error| Error::new(error.to_string()))
    }
}
