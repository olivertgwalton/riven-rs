use std::collections::HashSet;

use fuser::FileType;
use riven_core::settings::LibraryProfileMembership;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::entities::{VfsDirName, VfsFileName};
use riven_db::repo;

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
        } => {
            let exclusive_keys = if profile_key.is_none() {
                layout.exclusive_profile_keys()
            } else {
                vec![]
            };
            match canonical {
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
                        &exclusive_keys,
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
                        &exclusive_keys,
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
                        &exclusive_keys,
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
                        &exclusive_keys,
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
                        &exclusive_keys,
                    );
                }
                CanonicalPath::Root
                | CanonicalPath::MovieFile { .. }
                | CanonicalPath::EpisodeFile { .. }
                | CanonicalPath::Invalid => {}
            }
        }
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
    exclusive_keys: &[&str],
) {
    let Ok(paths) = runtime.block_on(repo::list_vfs_dir_names(
        pool,
        pattern,
        (dir_index + 2) as u32,
    )) else {
        return;
    };
    let mut seen = HashSet::new();
    for entry in paths {
        if !matches_dir_profile(&entry, profile_key, exclusive_keys) {
            continue;
        }
        let Some(name) = entry.name.as_deref() else {
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
    exclusive_keys: &[&str],
) {
    let Ok(paths) = runtime.block_on(repo::list_vfs_file_names(pool, actual_dir)) else {
        return;
    };
    for entry in paths {
        if !matches_file_profile(&entry, profile_key, exclusive_keys) {
            continue;
        }
        let Some(name) = entry.name.as_deref() else {
            continue;
        };
        push_file_entry(entries, get_ino, virtual_parent, name);
    }
}

fn matches_dir_profile(
    entry: &VfsDirName,
    profile_key: Option<&str>,
    exclusive_keys: &[&str],
) -> bool {
    let membership = LibraryProfileMembership::from_json(entry.library_profiles.as_ref());
    match profile_key {
        Some(key) => membership.contains(key),
        None => !exclusive_keys.iter().any(|k| membership.contains(k)),
    }
}

fn matches_file_profile(
    entry: &VfsFileName,
    profile_key: Option<&str>,
    exclusive_keys: &[&str],
) -> bool {
    let membership = LibraryProfileMembership::from_json(entry.library_profiles.as_ref());
    match profile_key {
        Some(key) => membership.contains(key),
        None => !exclusive_keys.iter().any(|k| membership.contains(k)),
    }
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
