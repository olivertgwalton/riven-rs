use std::collections::HashSet;

use anyhow::{Result, bail};
use riven_core::settings::LibraryProfileMembership;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::repo;

use crate::path_info::{CanonicalPath, PathTarget, parse_path};

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
        CanonicalPath::MovieFile { .. } | CanonicalPath::EpisodeFile { .. } => {
            bail!("Invalid path")
        }
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
