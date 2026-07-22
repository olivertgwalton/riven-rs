use fuser::FileType;
use riven_core::vfs_layout::VfsLibraryLayout;

use crate::path_info::{CanonicalPath, PathTarget, parse_path};
use crate::query::directory_entry_paths;

/// A directory entry ready to hand back to FUSE.
pub type DirEntry = (u64, FileType, String);

/// Callback used to assign / retrieve an inode number for a given path.
pub type GetOrCreateIno<'a> = &'a mut dyn FnMut(&str) -> u64;

pub fn populate_entries(
    ino: u64,
    ino_to_path: Option<&str>,
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
    let target = parse_path(layout, path);
    if matches!(target, PathTarget::Invalid) {
        return;
    }

    let file_entries = matches!(
        target,
        PathTarget::Canonical {
            path: CanonicalPath::MovieDir { .. } | CanonicalPath::SeasonDir { .. },
            ..
        }
    );
    let Ok(names) = runtime.block_on(directory_entry_paths(layout, path)) else {
        return;
    };

    let file_type = if file_entries {
        FileType::RegularFile
    } else {
        FileType::Directory
    };
    for name in names {
        let child = child_path(path, &name);
        entries.push((get_ino(&child), file_type, name));
    }
}

fn child_path(parent_path: &str, name: &str) -> String {
    if parent_path == "/" {
        format!("/{name}")
    } else {
        format!("{parent_path}/{name}")
    }
}
