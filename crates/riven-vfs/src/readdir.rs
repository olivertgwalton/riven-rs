use fuser::FileType;
use riven_db::repo;

/// A directory entry ready to hand back to FUSE.
pub type DirEntry = (u64, FileType, String);

/// Callback used to assign / retrieve an inode number for a given path.
pub type GetOrCreateIno<'a> = &'a mut dyn FnMut(&str) -> u64;

/// Populate `entries` with the children of the directory identified by `ino`.
///
/// Every case is a **single** database query:
/// - `/`              → two static entries (no DB)
/// - `/movies`        → one query: distinct movie dir names from filesystem_entries
/// - `/shows`         → one query: distinct show dir names from filesystem_entries
/// - `/movies/{name}` → one query: file paths under that prefix
/// - `/shows/{name}`  → one query: distinct season dir names under that prefix
/// - `/shows/{n}/{s}` → one query: file paths under that prefix
pub fn populate_entries(
    ino: u64,
    ino_to_path: Option<&str>,
    pool: &sqlx::PgPool,
    runtime: &tokio::runtime::Handle,
    entries: &mut Vec<DirEntry>,
    get_ino: GetOrCreateIno<'_>,
) {
    const ROOT_INO: u64 = 1;
    const MOVIES_INO: u64 = 2;
    const SHOWS_INO: u64 = 3;

    match ino {
        ROOT_INO => {
            entries.push((MOVIES_INO, FileType::Directory, "movies".into()));
            entries.push((SHOWS_INO, FileType::Directory, "shows".into()));
        }

        MOVIES_INO => {
            if let Ok(dirs) = runtime.block_on(repo::list_vfs_movie_dirs(pool)) {
                for dir_name in dirs {
                    let child_path = format!("/movies/{dir_name}");
                    let ino = get_ino(&child_path);
                    entries.push((ino, FileType::Directory, dir_name));
                }
            }
        }

        SHOWS_INO => {
            if let Ok(dirs) = runtime.block_on(repo::list_vfs_show_dirs(pool)) {
                for dir_name in dirs {
                    let child_path = format!("/shows/{dir_name}");
                    let ino = get_ino(&child_path);
                    entries.push((ino, FileType::Directory, dir_name));
                }
            }
        }

        _ => {
            let path = match ino_to_path {
                Some(p) => p,
                None => return,
            };

            let depth = path.trim_start_matches('/').split('/').count();

            match depth {
                // /movies/{dir}  or  /shows/{dir}
                2 => {
                    if path.starts_with("/shows/") {
                        // List season subdirectories.
                        if let Ok(season_dirs) =
                            runtime.block_on(repo::list_vfs_season_dirs(pool, path))
                        {
                            for season_name in season_dirs {
                                let child_path = format!("{path}/{season_name}");
                                let ino = get_ino(&child_path);
                                entries.push((ino, FileType::Directory, season_name));
                            }
                        }
                    } else {
                        // /movies/{dir}: list files.
                        push_file_entries(pool, runtime, entries, get_ino, path);
                    }
                }

                // /shows/{dir}/{season}
                3 => {
                    push_file_entries(pool, runtime, entries, get_ino, path);
                }

                _ => {}
            }
        }
    }
}

fn push_file_entries(
    pool: &sqlx::PgPool,
    runtime: &tokio::runtime::Handle,
    entries: &mut Vec<DirEntry>,
    get_ino: GetOrCreateIno<'_>,
    dir_path: &str,
) {
    if let Ok(paths) = runtime.block_on(repo::list_vfs_file_paths(pool, dir_path)) {
        for file_path in paths {
            let Some((_, fname)) = file_path.rsplit_once('/') else {
                continue;
            };
            let fname = fname.to_string();
            let ino = get_ino(&file_path);
            entries.push((ino, FileType::RegularFile, fname));
        }
    }
}
