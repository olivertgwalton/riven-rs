//! Materialise the library as real directories holding symlinks into the VFS.
//!
//! The FUSE filesystem is mounted read-only and answers only for paths the
//! database knows about, so a media server has nowhere to put the sidecars it
//! keeps beside a title -- a theme song, an `.nfo`, trickplay tiles, an
//! extracted subtitle. Pointing the server at this tree instead gives it real
//! directories to write into, while every media file is still a symlink whose
//! bytes are streamed through the VFS.
//!
//! The tree is derived from the same layout the FUSE layer serves, so a title
//! sits at the same path in both and a profile's prefix means the same thing in
//! both.
//!
//! One rule governs deletion: a regular file is never removed. Only symlinks
//! this reconciler would itself have created are pruned, and a directory left
//! holding nothing but sidecars is kept rather than tidied away -- if the title
//! is downloaded again the theme song is already there.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::fs as unix_fs;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use riven_core::settings::LibraryProfileMembership;
use riven_core::vfs_layout::VfsLibraryLayout;

/// What one reconcile pass changed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SymlinkStats {
    pub links_created: usize,
    pub links_repointed: usize,
    pub links_removed: usize,
    /// Parent directories created to hold a link. One call may create several
    /// levels; this counts the links that needed one, not the levels.
    pub dirs_created: usize,
    pub dirs_removed: usize,
    /// Link paths where a real file already sits. Never clobbered.
    pub conflicts: usize,
    /// Directories kept because they still hold files this reconciler did not
    /// create -- the sidecars the tree exists for.
    pub orphans_kept: usize,
}

impl SymlinkStats {
    /// True when the pass left the tree exactly as it found it.
    pub fn is_noop(&self) -> bool {
        self.links_created == 0
            && self.links_repointed == 0
            && self.links_removed == 0
            && self.dirs_created == 0
            && self.dirs_removed == 0
    }
}

/// Reject a configuration that cannot work before any of it is acted on.
pub fn validate_paths(symlink_path: &str, mount_path: &str) -> Result<()> {
    let root = symlink_path.trim();
    let mount = mount_path.trim();

    if root.is_empty() {
        bail!("symlink path is empty");
    }
    if mount.is_empty() {
        bail!("VFS mount path is empty, so the symlink tree would point nowhere");
    }
    if !Path::new(root).is_absolute() {
        bail!("symlink path {root} must be absolute");
    }
    if !Path::new(mount).is_absolute() {
        bail!("VFS mount path {mount} must be absolute");
    }
    // Component-wise, so `/mnt/riven-vfs` is not read as living under
    // `/mnt/riven`. Equal paths count as nested, which is the point: the tree
    // cannot be the mount.
    if Path::new(root).starts_with(mount) {
        bail!("symlink path {root} is inside the VFS mount {mount}, which is read-only");
    }
    if Path::new(mount).starts_with(root) {
        bail!(
            "VFS mount {mount} is inside the symlink path {root}, so reconciling would walk into the mount"
        );
    }
    Ok(())
}

/// The links the tree should hold: path relative to the tree root, against the
/// absolute path it points at.
///
/// Pure, and the whole of the layout decision. An entry appears in the default
/// `/movies` and `/shows` tree unless an exclusive profile has claimed it, and
/// once more under the prefix of every profile that matches it -- which is what
/// [`crate::query`] does for `readdir`, stated over the whole library at once.
pub fn build_plan(
    layout: &VfsLibraryLayout,
    entries: impl IntoIterator<Item = (String, LibraryProfileMembership)>,
    mount_path: &str,
) -> BTreeMap<String, String> {
    let mount = mount_path.trim().trim_end_matches('/');
    let exclusive = layout.exclusive_profile_keys();
    let mut plan = BTreeMap::new();

    for (path, membership) in entries {
        let relative = path.trim_start_matches('/');
        if relative.is_empty() {
            continue;
        }
        let target = format!("{mount}/{relative}");

        if !exclusive.iter().any(|key| membership.contains(key)) {
            drop(plan.insert(relative.to_owned(), target.clone()));
        }
        for profile in layout.profiles() {
            if membership.contains(&profile.key) {
                let prefix = profile.library_path.trim_matches('/');
                drop(plan.insert(format!("{prefix}/{relative}"), target.clone()));
            }
        }
    }
    plan
}

/// Bring the tree under `root` into line with `plan`. Blocking; call it off the
/// runtime.
pub fn apply(root: &Path, plan: &BTreeMap<String, String>) -> Result<SymlinkStats> {
    let mut stats = SymlinkStats::default();
    fs::create_dir_all(root)?;

    let mut desired: BTreeSet<PathBuf> = BTreeSet::new();
    for (relative, target) in plan {
        let link = root.join(relative);
        desired.insert(link.clone());

        if let Some(parent) = link.parent()
            && !parent.is_dir()
        {
            fs::create_dir_all(parent)?;
            stats.dirs_created += 1;
        }

        match fs::symlink_metadata(&link) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                unix_fs::symlink(target, &link)?;
                stats.links_created += 1;
            }
            Err(error) => return Err(error.into()),
            Ok(metadata) if metadata.file_type().is_symlink() => {
                if fs::read_link(&link)?.as_os_str() != OsStr::new(target.as_str()) {
                    fs::remove_file(&link)?;
                    unix_fs::symlink(target, &link)?;
                    stats.links_repointed += 1;
                }
            }
            Ok(_) => {
                stats.conflicts += 1;
                tracing::warn!(
                    link = %link.display(),
                    "a real file occupies a link path; left as it is"
                );
            }
        }
    }

    // The root's own emptiness is deliberately not acted on: the tree root
    // stays whether or not the library is empty.
    let _ = prune_dir(root, &desired, &mut stats)?;
    Ok(stats)
}

/// Remove stale links under `dir`, then report whether it is now empty so the
/// caller can remove it. Regular files are counted and never touched.
fn prune_dir(dir: &Path, desired: &BTreeSet<PathBuf>, stats: &mut SymlinkStats) -> Result<bool> {
    let mut kept_links = 0usize;
    let mut kept_files = 0usize;
    let mut kept_dirs = 0usize;

    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;

        if metadata.file_type().is_symlink() {
            if desired.contains(&path) {
                kept_links += 1;
            } else {
                fs::remove_file(&path)?;
                stats.links_removed += 1;
            }
        } else if metadata.is_dir() {
            if prune_dir(&path, desired, stats)? {
                fs::remove_dir(&path)?;
                stats.dirs_removed += 1;
            } else {
                kept_dirs += 1;
            }
        } else {
            kept_files += 1;
        }
    }

    if kept_files > 0 && kept_links == 0 && kept_dirs == 0 {
        stats.orphans_kept += 1;
    }
    Ok(kept_links == 0 && kept_files == 0 && kept_dirs == 0)
}

/// Read the library, plan the tree, and write it.
pub async fn reconcile(
    layout: &VfsLibraryLayout,
    symlink_path: &str,
    mount_path: &str,
) -> Result<SymlinkStats> {
    validate_paths(symlink_path, mount_path)?;

    let mount = Path::new(mount_path.trim());
    if !mount.exists() {
        bail!(
            "VFS mount {} does not exist; refusing to build a tree of links into nothing",
            mount.display()
        );
    }

    let entries = riven_db::repo::list_vfs_entry_paths().await?;
    let entries: Vec<(String, LibraryProfileMembership)> = entries
        .into_iter()
        .map(|entry| {
            let membership = LibraryProfileMembership::from_json(entry.library_profiles.as_ref());
            (entry.path, membership)
        })
        .collect();

    let plan = build_plan(layout, entries, mount_path);

    // A tree of links into a mount that is not serving is a library of broken
    // paths, and a media server asked to scan one drops the items it can no
    // longer see. The database holding entries while the mount reads as empty
    // is what an unmounted VFS looks like from here -- the mount path is a
    // bind mount in its own right, so its device number says nothing.
    if !plan.is_empty() && fs::read_dir(mount)?.next().is_none() {
        bail!(
            "VFS mount {} is empty while the library holds {} entries; refusing to build a tree of broken links",
            mount.display(),
            plan.len()
        );
    }

    let root = PathBuf::from(symlink_path.trim());

    tokio::task::spawn_blocking(move || apply(&root, &plan)).await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use riven_core::settings::{FilesystemLibraryProfile, FilesystemSettings};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    static SCRATCH: AtomicU32 = AtomicU32::new(0);

    /// A private directory per test. The suite is a value type per test in the
    /// same spirit as the rest of the workspace, so nothing is shared.
    fn scratch() -> PathBuf {
        let id = SCRATCH.fetch_add(1, Ordering::SeqCst);
        let mut path = std::env::temp_dir();
        path.push(format!("riven-symlink-{}-{id}", std::process::id()));
        drop(fs::remove_dir_all(&path));
        fs::create_dir_all(&path).expect("scratch dir");
        path
    }

    fn layout_with(profiles: Vec<(&str, &str, bool)>) -> VfsLibraryLayout {
        let mut map = HashMap::new();
        for (key, path, exclusive) in profiles {
            drop(map.insert(
                key.to_owned(),
                FilesystemLibraryProfile {
                    name: key.to_owned(),
                    library_path: path.to_owned(),
                    enabled: true,
                    exclusive,
                    filter_rules: Default::default(),
                },
            ));
        }
        VfsLibraryLayout::new(FilesystemSettings {
            mount_path: "/mnt/riven-vfs".to_owned(),
            library_profiles: map,
            ..FilesystemSettings::default()
        })
    }

    fn entry(path: &str, profiles: &[&str]) -> (String, LibraryProfileMembership) {
        (
            path.to_owned(),
            LibraryProfileMembership::new(profiles.iter().map(|s| (*s).to_owned())),
        )
    }

    #[test]
    fn plans_the_default_tree() {
        let layout = layout_with(vec![]);
        let plan = build_plan(
            &layout,
            vec![entry("/movies/Film (2019) {tmdb-1}/Film.mkv", &[])],
            "/mnt/riven-vfs",
        );
        assert_eq!(
            plan.get("movies/Film (2019) {tmdb-1}/Film.mkv")
                .map(String::as_str),
            Some("/mnt/riven-vfs/movies/Film (2019) {tmdb-1}/Film.mkv")
        );
        assert_eq!(plan.len(), 1);
    }

    #[test]
    fn a_matched_profile_adds_a_second_path_to_the_same_target() {
        let layout = layout_with(vec![("kids", "/kids", false)]);
        let plan = build_plan(
            &layout,
            vec![entry("/movies/Up (2009)/Up.mkv", &["kids"])],
            "/mnt/riven-vfs",
        );
        assert_eq!(plan.len(), 2);
        assert_eq!(
            plan.get("kids/movies/Up (2009)/Up.mkv"),
            plan.get("movies/Up (2009)/Up.mkv")
        );
    }

    #[test]
    fn an_exclusive_profile_takes_the_entry_out_of_the_default_tree() {
        let layout = layout_with(vec![("kids", "/kids", true)]);
        let plan = build_plan(
            &layout,
            vec![
                entry("/movies/Up (2009)/Up.mkv", &["kids"]),
                entry("/movies/Heat (1995)/Heat.mkv", &[]),
            ],
            "/mnt/riven-vfs",
        );
        assert!(plan.contains_key("kids/movies/Up (2009)/Up.mkv"));
        assert!(!plan.contains_key("movies/Up (2009)/Up.mkv"));
        assert!(plan.contains_key("movies/Heat (1995)/Heat.mkv"));
    }

    #[test]
    fn applying_twice_changes_nothing_the_second_time() {
        let root = scratch();
        let mut plan = BTreeMap::new();
        drop(plan.insert(
            "movies/Film/Film.mkv".to_owned(),
            "/mnt/riven-vfs/movies/Film/Film.mkv".to_owned(),
        ));

        let first = apply(&root, &plan).expect("first pass");
        assert_eq!(first.links_created, 1);

        let second = apply(&root, &plan).expect("second pass");
        assert!(second.is_noop(), "{second:?}");

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn a_stale_link_goes_and_a_sidecar_stays() {
        let root = scratch();
        let mut plan = BTreeMap::new();
        drop(plan.insert(
            "movies/Film/Film.mkv".to_owned(),
            "/mnt/riven-vfs/movies/Film/Film.mkv".to_owned(),
        ));
        apply(&root, &plan).expect("first pass");

        // What the whole feature is for: a media server writing beside the link.
        let theme = root.join("movies/Film/theme.mp3");
        fs::write(&theme, b"id3").expect("write theme");

        // The title leaves the library.
        let stats = apply(&root, &BTreeMap::new()).expect("second pass");

        assert_eq!(stats.links_removed, 1);
        assert!(!root.join("movies/Film/Film.mkv").exists());
        assert!(theme.exists(), "a regular file must never be removed");
        assert_eq!(stats.orphans_kept, 1);
        assert_eq!(stats.dirs_removed, 0, "a directory holding a sidecar stays");

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn an_emptied_directory_is_removed() {
        let root = scratch();
        let mut plan = BTreeMap::new();
        drop(plan.insert(
            "movies/Film/Film.mkv".to_owned(),
            "/mnt/riven-vfs/movies/Film/Film.mkv".to_owned(),
        ));
        apply(&root, &plan).expect("first pass");

        let stats = apply(&root, &BTreeMap::new()).expect("second pass");
        assert_eq!(stats.links_removed, 1);
        assert!(!root.join("movies/Film").exists());
        assert!(!root.join("movies").exists());
        assert!(root.exists(), "the tree root itself is never removed");

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn a_moved_mount_repoints_rather_than_duplicating() {
        let root = scratch();
        let mut before = BTreeMap::new();
        drop(before.insert(
            "movies/Film/Film.mkv".to_owned(),
            "/mnt/riven-vfs/movies/Film/Film.mkv".to_owned(),
        ));
        apply(&root, &before).expect("first pass");

        let mut after = BTreeMap::new();
        drop(after.insert(
            "movies/Film/Film.mkv".to_owned(),
            "/mnt/elsewhere/movies/Film/Film.mkv".to_owned(),
        ));
        let stats = apply(&root, &after).expect("second pass");

        assert_eq!(stats.links_repointed, 1);
        assert_eq!(stats.links_created, 0);
        assert_eq!(
            fs::read_link(root.join("movies/Film/Film.mkv")).expect("read link"),
            Path::new("/mnt/elsewhere/movies/Film/Film.mkv")
        );

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn a_real_file_at_a_link_path_is_left_alone() {
        let root = scratch();
        fs::create_dir_all(root.join("movies/Film")).expect("dirs");
        let occupied = root.join("movies/Film/Film.mkv");
        fs::write(&occupied, b"real bytes").expect("write");

        let mut plan = BTreeMap::new();
        drop(plan.insert(
            "movies/Film/Film.mkv".to_owned(),
            "/mnt/riven-vfs/movies/Film/Film.mkv".to_owned(),
        ));
        let stats = apply(&root, &plan).expect("pass");

        assert_eq!(stats.conflicts, 1);
        assert_eq!(stats.links_created, 0);
        assert_eq!(fs::read(&occupied).expect("read"), b"real bytes");

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn nesting_either_way_is_refused() {
        assert!(validate_paths("/mnt/riven/links", "/mnt/riven").is_err());
        assert!(validate_paths("/mnt/riven", "/mnt/riven/vfs").is_err());
        assert!(validate_paths("/mnt/riven", "/mnt/riven").is_err());
        assert!(validate_paths("", "/mnt/riven").is_err());
        assert!(validate_paths("relative", "/mnt/riven").is_err());
        // A shared prefix that is not a shared path is fine.
        assert!(validate_paths("/mnt/riven", "/mnt/riven-vfs").is_ok());
    }
}
