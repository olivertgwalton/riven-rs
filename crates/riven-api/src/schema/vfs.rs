use async_graphql::*;
use chrono::{DateTime, Utc};
use riven_db::entities::FileSystemEntry;
use riven_db::repo;
use sqlx::PgPool;

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
    async fn vfs_entry_stat(&self, ctx: &Context<'_>, path: String) -> Result<VfsEntryStat> {
        let pool = ctx.data::<PgPool>()?;
        get_vfs_entry_stat(pool, &path).await
    }

    /// Get the filesystem entry (media file record) for a VFS file path.
    async fn vfs_entry(&self, ctx: &Context<'_>, path: String) -> Result<Option<FileSystemEntry>> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::get_media_entry_by_path(pool, &path).await?)
    }

    /// List child entry names (file or directory names) directly under a VFS path.
    async fn vfs_directory_entry_paths(
        &self,
        ctx: &Context<'_>,
        path: String,
    ) -> Result<Vec<String>> {
        let pool = ctx.data::<PgPool>()?;
        get_vfs_directory_entry_paths(pool, &path).await
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

async fn get_vfs_entry_stat(pool: &PgPool, path: &str) -> async_graphql::Result<VfsEntryStat> {
    let now = Utc::now();
    let segments = path_segments(path);

    match segments.as_slice() {
        // Root "/"
        [] => {
            let stat = repo::get_vfs_dir_stat(pool, "").await?;
            Ok(VfsEntryStat {
                mtime: stat.mtime.unwrap_or(now),
                ctime: stat.ctime.unwrap_or(now),
                atime: stat.mtime.unwrap_or(now),
                mode: MODE_DIR,
                nlink: 4, // 2 + 2 (movies dir + shows dir)
                size: 0,
                uid: 0,
                gid: 0,
            })
        }

        // /movies
        ["movies"] => {
            let stat = repo::get_vfs_dir_stat(pool, "/movies").await?;
            let count = repo::count_vfs_distinct_dirs(pool, "/movies/%/%", 3).await?;
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

        // /shows
        ["shows"] => {
            let stat = repo::get_vfs_dir_stat(pool, "/shows").await?;
            let count = repo::count_vfs_distinct_dirs(pool, "/shows/%/%/%", 3).await?;
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

        // /movies/{dir}
        ["movies", dir] => {
            let prefix = format!("/movies/{dir}");
            let stat = repo::get_vfs_dir_stat(pool, &prefix).await?;
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

        // /movies/{dir}/{file}
        ["movies", _, _] => {
            let entry = repo::get_media_entry_by_path(pool, path)
                .await?
                .ok_or_else(|| Error::new("Entry not found"))?;
            Ok(file_stat(&entry))
        }

        // /shows/{dir}
        ["shows", dir] => {
            let prefix = format!("/shows/{dir}");
            let stat = repo::get_vfs_dir_stat(pool, &prefix).await?;
            if stat.entry_count == 0 {
                return Err(Error::new("Entry not found"));
            }
            let season_count =
                repo::count_vfs_distinct_dirs(pool, &format!("{prefix}/%/%"), 4).await?;
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

        // /shows/{dir}/{season}
        ["shows", dir, season] => {
            let prefix = format!("/shows/{dir}/{season}");
            let stat = repo::get_vfs_dir_stat(pool, &prefix).await?;
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

        // /shows/{dir}/{season}/{file}
        ["shows", _, _, _] => {
            let entry = repo::get_media_entry_by_path(pool, path)
                .await?
                .ok_or_else(|| Error::new("Entry not found"))?;
            Ok(file_stat(&entry))
        }

        _ => Err(Error::new("Invalid path")),
    }
}

async fn get_vfs_directory_entry_paths(
    pool: &PgPool,
    path: &str,
) -> async_graphql::Result<Vec<String>> {
    let segments = path_segments(path);

    match segments.as_slice() {
        // Root "/" → always ["movies", "shows"]
        [] => Ok(vec!["movies".to_string(), "shows".to_string()]),

        // /movies → list of movie directory names (e.g. "Inception (2010) {tmdb-27205}")
        ["movies"] => {
            let entries = repo::list_vfs_dir_names(pool, "/movies/%/%", 3).await?;
            let mut seen = std::collections::HashSet::new();
            Ok(entries
                .into_iter()
                .filter_map(|e| e.name)
                .filter(|name| seen.insert(name.clone()))
                .collect())
        }

        // /movies/{dir} → list of filenames within that movie directory
        ["movies", dir] => {
            let dir_path = format!("/movies/{dir}");
            let entries = repo::list_vfs_file_names(pool, &dir_path).await?;
            Ok(entries.into_iter().filter_map(|e| e.name).collect())
        }

        // /shows → list of show directory names
        ["shows"] => {
            let entries = repo::list_vfs_dir_names(pool, "/shows/%/%/%", 3).await?;
            let mut seen = std::collections::HashSet::new();
            Ok(entries
                .into_iter()
                .filter_map(|e| e.name)
                .filter(|name| seen.insert(name.clone()))
                .collect())
        }

        // /shows/{dir} → list of season directory names
        ["shows", dir] => {
            let pattern = format!("/shows/{dir}/%/%");
            let entries = repo::list_vfs_dir_names(pool, &pattern, 4).await?;
            let mut seen = std::collections::HashSet::new();
            Ok(entries
                .into_iter()
                .filter_map(|e| e.name)
                .filter(|name| seen.insert(name.clone()))
                .collect())
        }

        // /shows/{dir}/{season} → list of episode filenames
        ["shows", dir, season] => {
            let dir_path = format!("/shows/{dir}/{season}");
            let entries = repo::list_vfs_file_names(pool, &dir_path).await?;
            Ok(entries.into_iter().filter_map(|e| e.name).collect())
        }

        // /shows/{dir}/{season}/{file} → the file itself
        ["shows", _, _, file] => Ok(vec![file.to_string()]),

        _ => Err(Error::new("Invalid path")),
    }
}
