use std::collections::HashSet;

use fuser::FileType;
use riven_core::settings::LibraryProfileMembership;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::repo::{self, VfsEntryPath};

use crate::path_info::{CanonicalPath, PathTarget, parse_path};

/// A directory entry ready to hand back to FUSE.
pub type DirEntry = (u64, FileType, String);

/// Callback used to assign / retrieve an inode number for a given path.
pub type GetOrCreateIno<'a> = &'a mut dyn FnMut(&str) -> u64;

pub fn populate_entries(
    ino: u64,
    ino_to_path: Option<&str>,
    pool: &sqlx::PgPool,
    runtime: &tokio::runtime::Handle,
    layout: &VfsLibraryLayout,
    entries: &mut Vec<DirEntry>,
    get_ino: GetOrCreateIno<'_>,
) {
    let path = if ino == 1 {
        "/"
    } else {
        ino_to_path.unwrap_or("/")
    };

    match parse_path(layout, path) {
        PathTarget::Root => {
            for name in layout.root_entries() {
                push_dir_entry(entries, get_ino, path, &name);
            }
        }
        PathTarget::ProfilePrefixDir => {
            for name in layout.profile_prefix_children(path) {
                push_dir_entry(entries, get_ino, path, &name);
            }
        }
        PathTarget::Canonical {
            profile_key,
            path: canonical,
        } => match canonical {
            CanonicalPath::AllMovies => {
                push_item_dirs(
                    pool,
                    runtime,
                    entries,
                    get_ino,
                    path,
                    "/movies/%/%",
                    1,
                    profile_key.as_deref(),
                );
            }
            CanonicalPath::AllShows => {
                push_item_dirs(
                    pool,
                    runtime,
                    entries,
                    get_ino,
                    path,
                    "/shows/%/%/%",
                    1,
                    profile_key.as_deref(),
                );
            }
            CanonicalPath::MovieDir { actual_dir } => {
                push_file_entries(
                    pool,
                    runtime,
                    entries,
                    get_ino,
                    path,
                    &actual_dir,
                    profile_key.as_deref(),
                );
            }
            CanonicalPath::ShowDir { actual_dir } => {
                push_item_dirs(
                    pool,
                    runtime,
                    entries,
                    get_ino,
                    path,
                    &format!("{actual_dir}/%/%"),
                    2,
                    profile_key.as_deref(),
                );
            }
            CanonicalPath::SeasonDir { actual_dir } => {
                push_file_entries(
                    pool,
                    runtime,
                    entries,
                    get_ino,
                    path,
                    &actual_dir,
                    profile_key.as_deref(),
                );
            }
            CanonicalPath::Root
            | CanonicalPath::MovieFile { .. }
            | CanonicalPath::EpisodeFile { .. }
            | CanonicalPath::Invalid => {}
        },
        PathTarget::Invalid => {}
    }
}

fn push_item_dirs(
    pool: &sqlx::PgPool,
    runtime: &tokio::runtime::Handle,
    entries: &mut Vec<DirEntry>,
    get_ino: GetOrCreateIno<'_>,
    virtual_parent: &str,
    pattern: &str,
    dir_index: usize,
    profile_key: Option<&str>,
) {
    let Ok(paths) = runtime.block_on(repo::list_vfs_entry_paths(pool, pattern)) else {
        return;
    };
    let mut seen = HashSet::new();
    for entry in paths {
        if !matches_profile(&entry, profile_key) {
            continue;
        }
        let Some(name) = entry.path.trim_matches('/').split('/').nth(dir_index) else {
            continue;
        };
        if seen.insert(name.to_string()) {
            push_dir_entry(entries, get_ino, virtual_parent, name);
        }
    }
}

fn push_file_entries(
    pool: &sqlx::PgPool,
    runtime: &tokio::runtime::Handle,
    entries: &mut Vec<DirEntry>,
    get_ino: GetOrCreateIno<'_>,
    virtual_parent: &str,
    actual_dir: &str,
    profile_key: Option<&str>,
) {
    let pattern = format!("{actual_dir}/%");
    let Ok(paths) = runtime.block_on(repo::list_vfs_entry_paths(pool, &pattern)) else {
        return;
    };
    for entry in paths {
        if !matches_profile(&entry, profile_key) {
            continue;
        }
        let Some((_, name)) = entry.path.rsplit_once('/') else {
            continue;
        };
        push_file_entry(entries, get_ino, virtual_parent, name);
    }
}

fn matches_profile(entry: &VfsEntryPath, profile_key: Option<&str>) -> bool {
    let Some(profile_key) = profile_key else {
        return true;
    };
    LibraryProfileMembership::from_json(entry.library_profiles.as_ref()).contains(profile_key)
}

fn push_dir_entry(
    entries: &mut Vec<DirEntry>,
    get_ino: GetOrCreateIno<'_>,
    parent_path: &str,
    name: &str,
) {
    let child = child_path(parent_path, name);
    let ino = get_ino(&child);
    entries.push((ino, FileType::Directory, name.to_string()));
}

fn push_file_entry(
    entries: &mut Vec<DirEntry>,
    get_ino: GetOrCreateIno<'_>,
    parent_path: &str,
    name: &str,
) {
    let child = child_path(parent_path, name);
    let ino = get_ino(&child);
    entries.push((ino, FileType::RegularFile, name.to_string()));
}

fn child_path(parent_path: &str, name: &str) -> String {
    if parent_path == "/" {
        format!("/{name}")
    } else {
        format!("{parent_path}/{name}")
    }
}
