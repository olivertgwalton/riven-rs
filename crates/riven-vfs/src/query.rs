use std::collections::HashSet;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use riven_core::settings::LibraryProfileMembership;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::entities::FileSystemEntry;
use riven_db::repo;

use crate::path_info::{CanonicalPath, PathTarget, parse_path};

const MODE_DIR: i32 = 0o040755;
const MODE_FILE: i32 = 0o100644;

/// Filesystem metadata independent of either FUSE or GraphQL transports.
pub struct EntryStat {
    pub mtime: DateTime<Utc>,
    pub ctime: DateTime<Utc>,
    pub atime: DateTime<Utc>,
    pub mode: i32,
    pub nlink: i32,
    pub size: i64,
    pub uid: i32,
    pub gid: i32,
}

pub async fn entry_stat(layout: &VfsLibraryLayout, path: &str) -> Result<EntryStat> {
    let now = Utc::now();
    match parse_path(layout, path) {
        PathTarget::Root => {
            let stat = repo::get_vfs_dir_stat("").await?;
            Ok(directory_stat(
                &stat,
                now,
                2 + layout.root_entries().len() as i32,
            ))
        }
        PathTarget::ProfilePrefixDir => {
            let children = layout.profile_prefix_children(path);
            if children.is_empty() && layout.match_profile(path).is_none() {
                bail!("Entry not found");
            }
            let stat = repo::get_vfs_dir_stat("").await?;
            Ok(directory_stat(&stat, now, 2 + children.len() as i32))
        }
        PathTarget::Canonical { profile_key, path } => {
            canonical_stat(layout, profile_key.as_deref(), path, now).await
        }
        PathTarget::Invalid => bail!("Invalid path"),
    }
}

pub async fn directory_entry_paths(layout: &VfsLibraryLayout, path: &str) -> Result<Vec<String>> {
    match parse_path(layout, path) {
        PathTarget::Root => Ok(layout.root_entries()),
        PathTarget::ProfilePrefixDir => Ok(layout.profile_prefix_children(path)),
        PathTarget::Canonical { profile_key, path } => {
            canonical_directory_entries(layout, profile_key.as_deref(), path).await
        }
        PathTarget::Invalid => bail!("Invalid path"),
    }
}

pub async fn entry(layout: &VfsLibraryLayout, path: &str) -> Result<Option<FileSystemEntry>> {
    match parse_path(layout, path) {
        PathTarget::Canonical {
            profile_key,
            path:
                CanonicalPath::MovieFile { actual_path } | CanonicalPath::EpisodeFile { actual_path },
        } => {
            let entry = repo::get_media_entry_by_path(&actual_path).await?;
            Ok(entry.filter(|entry| is_visible(layout, entry, profile_key.as_deref())))
        }
        PathTarget::Root | PathTarget::ProfilePrefixDir | PathTarget::Canonical { .. } => Ok(None),
        PathTarget::Invalid => bail!("Invalid path"),
    }
}

async fn canonical_stat(
    layout: &VfsLibraryLayout,
    profile_key: Option<&str>,
    path: CanonicalPath,
    now: DateTime<Utc>,
) -> Result<EntryStat> {
    match path {
        CanonicalPath::AllMovies => {
            let stat = repo::get_vfs_dir_stat("/movies").await?;
            let children = movie_directories(layout, profile_key).await?;
            Ok(directory_stat(&stat, now, 2 + children.len() as i32))
        }
        CanonicalPath::AllShows => {
            let stat = repo::get_vfs_dir_stat("/shows").await?;
            let children = show_directories(layout, profile_key).await?;
            Ok(directory_stat(&stat, now, 2 + children.len() as i32))
        }
        CanonicalPath::MovieDir { actual_dir }
        | CanonicalPath::ShowDir { actual_dir }
        | CanonicalPath::SeasonDir { actual_dir } => {
            let stat = repo::get_vfs_dir_stat(&actual_dir).await?;
            if stat.entry_count == 0 {
                bail!("Entry not found");
            }
            let children = canonical_directory_entries(
                layout,
                profile_key,
                match actual_dir.split('/').filter(|s| !s.is_empty()).count() {
                    2 if actual_dir.starts_with("/movies/") => {
                        CanonicalPath::MovieDir { actual_dir }
                    }
                    2 => CanonicalPath::ShowDir { actual_dir },
                    _ => CanonicalPath::SeasonDir { actual_dir },
                },
            )
            .await?;
            Ok(directory_stat(&stat, now, 2 + children.len() as i32))
        }
        CanonicalPath::MovieFile { actual_path } | CanonicalPath::EpisodeFile { actual_path } => {
            let entry = repo::get_media_entry_by_path(&actual_path)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Entry not found"))?;
            if !is_visible(layout, &entry, profile_key) {
                bail!("Entry not found");
            }
            Ok(file_stat(&entry))
        }
        CanonicalPath::Root | CanonicalPath::Invalid => bail!("Invalid path"),
    }
}

async fn canonical_directory_entries(
    layout: &VfsLibraryLayout,
    profile_key: Option<&str>,
    path: CanonicalPath,
) -> Result<Vec<String>> {
    match path {
        CanonicalPath::AllMovies => movie_directories(layout, profile_key).await,
        CanonicalPath::AllShows => show_directories(layout, profile_key).await,
        CanonicalPath::MovieDir { actual_dir } | CanonicalPath::SeasonDir { actual_dir } => {
            visible_names(
                layout,
                profile_key,
                repo::list_vfs_file_names(&actual_dir)
                    .await?
                    .into_iter()
                    .map(|entry| (entry.name, entry.library_profiles)),
            )
        }
        CanonicalPath::ShowDir { actual_dir } => visible_names(
            layout,
            profile_key,
            repo::list_vfs_dir_names(&format!("{actual_dir}/%/%"), 4)
                .await?
                .into_iter()
                .map(|entry| (entry.name, entry.library_profiles)),
        ),
        CanonicalPath::Root
        | CanonicalPath::MovieFile { .. }
        | CanonicalPath::EpisodeFile { .. }
        | CanonicalPath::Invalid => bail!("Invalid path"),
    }
}

async fn movie_directories(
    layout: &VfsLibraryLayout,
    profile_key: Option<&str>,
) -> Result<Vec<String>> {
    visible_names(
        layout,
        profile_key,
        repo::list_vfs_dir_names("/movies/%/%", 3)
            .await?
            .into_iter()
            .map(|entry| (entry.name, entry.library_profiles)),
    )
}

async fn show_directories(
    layout: &VfsLibraryLayout,
    profile_key: Option<&str>,
) -> Result<Vec<String>> {
    visible_names(
        layout,
        profile_key,
        repo::list_vfs_dir_names("/shows/%/%/%", 3)
            .await?
            .into_iter()
            .map(|entry| (entry.name, entry.library_profiles)),
    )
}

fn visible_names(
    layout: &VfsLibraryLayout,
    profile_key: Option<&str>,
    entries: impl IntoIterator<Item = (Option<String>, Option<serde_json::Value>)>,
) -> Result<Vec<String>> {
    let exclusive_keys = if profile_key.is_none() {
        layout.exclusive_profile_keys()
    } else {
        Vec::new()
    };
    let mut seen = HashSet::new();
    Ok(entries
        .into_iter()
        .filter(|(_, profiles)| {
            let membership = LibraryProfileMembership::from_json(profiles.as_ref());
            profile_visible(&membership, profile_key, &exclusive_keys)
        })
        .filter_map(|(name, _)| name)
        .filter(|name| seen.insert(name.clone()))
        .collect())
}

fn is_visible(
    layout: &VfsLibraryLayout,
    entry: &FileSystemEntry,
    profile_key: Option<&str>,
) -> bool {
    let membership = LibraryProfileMembership::from_json(entry.library_profiles.as_ref());
    let exclusive_keys = if profile_key.is_none() {
        layout.exclusive_profile_keys()
    } else {
        Vec::new()
    };
    profile_visible(&membership, profile_key, &exclusive_keys)
}

fn profile_visible(
    membership: &LibraryProfileMembership,
    profile_key: Option<&str>,
    exclusive_keys: &[&str],
) -> bool {
    match profile_key {
        Some(key) => membership.contains(key),
        None => !exclusive_keys.iter().any(|key| membership.contains(key)),
    }
}

fn file_stat(entry: &FileSystemEntry) -> EntryStat {
    let mtime = entry.updated_at.unwrap_or(entry.created_at);
    EntryStat {
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

fn directory_stat(
    stat: &repo::VfsDirStatResult,
    fallback_time: DateTime<Utc>,
    nlink: i32,
) -> EntryStat {
    let mtime = stat.mtime.unwrap_or(fallback_time);
    EntryStat {
        mtime,
        ctime: stat.ctime.unwrap_or(fallback_time),
        atime: mtime,
        mode: MODE_DIR,
        nlink,
        size: 0,
        uid: 0,
        gid: 0,
    }
}
