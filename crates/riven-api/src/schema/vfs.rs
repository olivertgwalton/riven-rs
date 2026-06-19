use async_graphql::*;
use chrono::{DateTime, Utc};
use riven_db::entities::FileSystemEntry;
use riven_db::repo;

/// Unix mode for a readable directory (drwxr-xr-x).
const MODE_DIR: i32 = 0o040755;
/// Unix mode for a readable regular file (-rw-r--r--).
const MODE_FILE: i32 = 0o100644;

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

#[derive(Default)]
pub struct VfsQuery;

#[Object]
impl VfsQuery {
    /// Get filesystem stat info for a VFS path (file or directory).
    async fn vfs_entry_stat(&self, _ctx: &Context<'_>, path: String) -> Result<VfsEntryStat> {
        get_vfs_entry_stat(&path).await
    }

    /// Get the filesystem entry (media file record) for a VFS file path.
    async fn vfs_entry(&self, _ctx: &Context<'_>, path: String) -> Result<Option<FileSystemEntry>> {
        Ok(repo::get_media_entry_by_path(&path).await?)
    }

    /// List child entry names (file or directory names) directly under a VFS path.
    async fn vfs_directory_entry_paths(
        &self,
        _ctx: &Context<'_>,
        path: String,
    ) -> Result<Vec<String>> {
        get_vfs_directory_entry_paths(&path).await
    }
}

fn path_segments(path: &str) -> Vec<&str> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect()
}

fn file_stat(entry: &FileSystemEntry) -> VfsEntryStat {
    let mtime = entry.updated_at.unwrap_or(entry.created_at);
    VfsEntryStat {
        mtime,
        ctime: entry.created_at,
        atime: mtime,
        mode: MODE_FILE,
        nlink: 1,
        size: entry.file_size,
        uid: 0,
        gid: 0,
    }
}

async fn get_vfs_entry_stat(path: &str) -> async_graphql::Result<VfsEntryStat> {
    let now = Utc::now();
    let segments = path_segments(path);

    match segments.as_slice() {
        [] => {
            let stat = repo::get_vfs_dir_stat("").await?;
            Ok(VfsEntryStat {
                mtime: stat.mtime.unwrap_or(now),
                ctime: stat.ctime.unwrap_or(now),
                atime: stat.mtime.unwrap_or(now),
                mode: MODE_DIR,
                nlink: 4,
                size: 0,
                uid: 0,
                gid: 0,
            })
        }

        ["movies"] => {
            let stat = repo::get_vfs_dir_stat("/movies").await?;
            let count = repo::count_vfs_distinct_dirs("/movies/%/%", 3).await?;
            Ok(VfsEntryStat {
                mtime: stat.mtime.unwrap_or(now),
                ctime: stat.ctime.unwrap_or(now),
                atime: stat.mtime.unwrap_or(now),
                mode: MODE_DIR,
                nlink: 2 + count as i32,
                size: 0,
                uid: 0,
                gid: 0,
            })
        }

        ["shows"] => {
            let stat = repo::get_vfs_dir_stat("/shows").await?;
            let count = repo::count_vfs_distinct_dirs("/shows/%/%/%", 3).await?;
            Ok(VfsEntryStat {
                mtime: stat.mtime.unwrap_or(now),
                ctime: stat.ctime.unwrap_or(now),
                atime: stat.mtime.unwrap_or(now),
                mode: MODE_DIR,
                nlink: 2 + count as i32,
                size: 0,
                uid: 0,
                gid: 0,
            })
        }

        ["movies", dir] => {
            let prefix = format!("/movies/{dir}");
            let stat = repo::get_vfs_dir_stat(&prefix).await?;
            if stat.entry_count == 0 {
                return Err(Error::new("Entry not found"));
            }
            Ok(VfsEntryStat {
                mtime: stat.mtime.unwrap_or(now),
                ctime: stat.ctime.unwrap_or(now),
                atime: stat.mtime.unwrap_or(now),
                mode: MODE_DIR,
                nlink: 2,
                size: 0,
                uid: 0,
                gid: 0,
            })
        }

        ["movies", _, _] => {
            let entry = repo::get_media_entry_by_path(path)
                .await?
                .ok_or_else(|| Error::new("Entry not found"))?;
            Ok(file_stat(&entry))
        }

        ["shows", dir] => {
            let prefix = format!("/shows/{dir}");
            let stat = repo::get_vfs_dir_stat(&prefix).await?;
            if stat.entry_count == 0 {
                return Err(Error::new("Entry not found"));
            }
            let season_count =
                repo::count_vfs_distinct_dirs(&format!("{prefix}/%/%"), 4).await?;
            Ok(VfsEntryStat {
                mtime: stat.mtime.unwrap_or(now),
                ctime: stat.ctime.unwrap_or(now),
                atime: stat.mtime.unwrap_or(now),
                mode: MODE_DIR,
                nlink: 2 + season_count as i32,
                size: 0,
                uid: 0,
                gid: 0,
            })
        }

        ["shows", dir, season] => {
            let prefix = format!("/shows/{dir}/{season}");
            let stat = repo::get_vfs_dir_stat(&prefix).await?;
            if stat.entry_count == 0 {
                return Err(Error::new("Entry not found"));
            }
            Ok(VfsEntryStat {
                mtime: stat.mtime.unwrap_or(now),
                ctime: stat.ctime.unwrap_or(now),
                atime: stat.mtime.unwrap_or(now),
                mode: MODE_DIR,
                nlink: 2,
                size: 0,
                uid: 0,
                gid: 0,
            })
        }

        ["shows", _, _, _] => {
            let entry = repo::get_media_entry_by_path(path)
                .await?
                .ok_or_else(|| Error::new("Entry not found"))?;
            Ok(file_stat(&entry))
        }

        _ => Err(Error::new("Invalid path")),
    }
}

async fn get_vfs_directory_entry_paths(path: &str) -> async_graphql::Result<Vec<String>> {
    let segments = path_segments(path);

    match segments.as_slice() {
        [] => Ok(vec!["movies".to_string(), "shows".to_string()]),

        ["movies"] => {
            let entries = repo::list_vfs_dir_names("/movies/%/%", 3).await?;
            let mut seen = std::collections::HashSet::new();
            Ok(entries
                .into_iter()
                .filter_map(|e| e.name)
                .filter(|name| seen.insert(name.clone()))
                .collect())
        }

        ["movies", dir] => {
            let dir_path = format!("/movies/{dir}");
            let entries = repo::list_vfs_file_names(&dir_path).await?;
            Ok(entries.into_iter().filter_map(|e| e.name).collect())
        }

        ["shows"] => {
            let entries = repo::list_vfs_dir_names("/shows/%/%/%", 3).await?;
            let mut seen = std::collections::HashSet::new();
            Ok(entries
                .into_iter()
                .filter_map(|e| e.name)
                .filter(|name| seen.insert(name.clone()))
                .collect())
        }

        ["shows", dir] => {
            let pattern = format!("/shows/{dir}/%/%");
            let entries = repo::list_vfs_dir_names(&pattern, 4).await?;
            let mut seen = std::collections::HashSet::new();
            Ok(entries
                .into_iter()
                .filter_map(|e| e.name)
                .filter(|name| seen.insert(name.clone()))
                .collect())
        }

        ["shows", dir, season] => {
            let dir_path = format!("/shows/{dir}/{season}");
            let entries = repo::list_vfs_file_names(&dir_path).await?;
            Ok(entries.into_iter().filter_map(|e| e.name).collect())
        }

        ["shows", _, _, file] => Ok(vec![file.to_string()]),

        _ => Err(Error::new("Invalid path")),
    }
}
