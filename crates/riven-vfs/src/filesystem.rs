use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    Request,
};
use lru::LruCache;
use parking_lot::Mutex;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use riven_core::config::vfs::*;
use riven_core::settings::LibraryProfileMembership;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::entities::FileSystemEntry;
use riven_db::repo;

use crate::LinkRequest;
use crate::cache::RangeCache;
use crate::link::resolve_stream_url;
use crate::media_stream::{MediaStream, ReadOutcome};
use crate::path_info::{CanonicalPath, PathTarget, parse_path};
use crate::readdir::{DirEntry, populate_entries};

const TTL: Duration = Duration::from_secs(300);
const READDIR_CACHE_TTL: Duration = Duration::from_secs(30);

const ROOT_INO: u64 = 1;
const MOVIES_INO: u64 = 2;
const SHOWS_INO: u64 = 3;
const FIRST_DYNAMIC_INO: u64 = 100;

/// Reject hidden files (`.trickplay`, `.nfo`, etc.) and known ignored names
/// that media servers probe for but the VFS never serves.
fn is_ignored_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with('.')
        || lower.eq("folder.jpg")
        || lower.ends_with(".trickplay")
        || lower.ends_with(".nfo")
        || lower.ends_with(".bif")
}

fn make_attr(ino: u64, kind: FileType, size: u64, mtime: SystemTime) -> FileAttr {
    let is_dir = kind == FileType::Directory;
    FileAttr {
        ino,
        size,
        blocks: if is_dir { 0 } else { size.div_ceil(BLOCK_SIZE) },
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: UNIX_EPOCH,
        kind,
        perm: if is_dir { 0o755 } else { 0o444 },
        nlink: if is_dir { 2 } else { 1 },
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: BLOCK_SIZE as u32,
        flags: 0,
    }
}

fn dir_attr(ino: u64) -> FileAttr {
    make_attr(ino, FileType::Directory, 0, UNIX_EPOCH)
}

fn file_attr(ino: u64, size: u64, mtime: SystemTime) -> FileAttr {
    make_attr(ino, FileType::RegularFile, size, mtime)
}

fn entry_mtime(entry: &FileSystemEntry) -> SystemTime {
    // VFS file mtimes must be stable across ephemeral metadata churn such as
    // refreshed stream URLs, or media servers will treat the file as changed
    // and re-probe/prune extracted metadata. Use the creation time as the
    // content timestamp for the virtual file.
    let ts = entry.created_at;
    UNIX_EPOCH + Duration::from_secs(ts.timestamp().max(0) as u64)
}

struct FileHandle {
    path: Arc<str>,
    stream_url: Arc<str>,
    media_stream: MediaStream,
}

pub struct RivenFs {
    vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    filesystem_settings_revision: Arc<AtomicU64>,
    cache_revision: AtomicU64,
    db_pool: sqlx::PgPool,
    stream_client: reqwest::Client,
    link_request_tx: mpsc::Sender<LinkRequest>,
    debug_logging: bool,
    runtime: tokio::runtime::Handle,

    next_fd: AtomicU64,
    file_handles: DashMap<u64, FileHandle>,

    path_to_ino: DashMap<Arc<str>, u64>,
    ino_to_path: DashMap<u64, Arc<str>>,
    next_ino: AtomicU64,

    range_cache: RangeCache,
    readdir_cache: DashMap<u64, (Vec<DirEntry>, Instant)>,
    entry_cache: DashMap<String, (Option<FileSystemEntry>, Instant)>,
    // No TTL — URL is valid until the DB entry is explicitly cleared.
    stream_url_cache: DashMap<i64, String>,
}

impl RivenFs {
    pub fn new(
        vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
        filesystem_settings_revision: Arc<AtomicU64>,
        db_pool: sqlx::PgPool,
        stream_client: reqwest::Client,
        link_request_tx: mpsc::Sender<LinkRequest>,
        debug_logging: bool,
        cache_max_size_mb: u64,
    ) -> Self {
        // Convert MB budget to entry count, assuming ~CHUNK_SIZE bytes per cached range.
        let entries = if cache_max_size_mb == 0 {
            256
        } else {
            ((cache_max_size_mb * 1024 * 1024) / CHUNK_SIZE) as usize
        };
        let path_to_ino = DashMap::new();
        let ino_to_path = DashMap::new();
        let movies_path: Arc<str> = Arc::from("/movies");
        let shows_path: Arc<str> = Arc::from("/shows");
        path_to_ino.insert(Arc::clone(&movies_path), MOVIES_INO);
        path_to_ino.insert(Arc::clone(&shows_path), SHOWS_INO);
        ino_to_path.insert(MOVIES_INO, movies_path);
        ino_to_path.insert(SHOWS_INO, shows_path);

        Self {
            vfs_layout,
            filesystem_settings_revision,
            cache_revision: AtomicU64::new(0),
            db_pool,
            stream_client,
            link_request_tx,
            debug_logging,
            runtime: tokio::runtime::Handle::current(),
            next_fd: AtomicU64::new(1),
            file_handles: DashMap::new(),
            path_to_ino,
            ino_to_path,
            next_ino: AtomicU64::new(FIRST_DYNAMIC_INO),
            range_cache: Mutex::new(LruCache::new(std::num::NonZeroUsize::new(entries).unwrap())),
            readdir_cache: DashMap::new(),
            entry_cache: DashMap::new(),
            stream_url_cache: DashMap::new(),
        }
    }

    fn current_layout(&self) -> VfsLibraryLayout {
        self.vfs_layout.blocking_read().clone()
    }

    fn refresh_caches_if_needed(&self) {
        let revision = self.filesystem_settings_revision.load(Ordering::SeqCst);
        let cached = self.cache_revision.load(Ordering::SeqCst);
        if revision == cached {
            return;
        }

        self.readdir_cache.clear();
        self.entry_cache.clear();
        self.cache_revision.store(revision, Ordering::SeqCst);
    }

    fn get_or_create_ino(&self, path: &str) -> u64 {
        if let Some(ino) = self.path_to_ino.get(path) {
            return *ino;
        }
        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        let arc: Arc<str> = Arc::from(path);
        self.path_to_ino.insert(Arc::clone(&arc), ino);
        self.ino_to_path.insert(ino, arc);
        ino
    }

    fn get_entry_cached(&self, path: &str) -> Option<FileSystemEntry> {
        self.refresh_caches_if_needed();
        if let Some(cached) = self.entry_cache.get(path)
            && cached.1.elapsed() < TTL
        {
            return cached.0.clone();
        }
        let layout = self.current_layout();
        let result = match parse_path(&layout, path) {
            PathTarget::Canonical {
                profile_key,
                path: canonical,
            } => {
                let actual_path = match canonical {
                    CanonicalPath::MovieFile { actual_path }
                    | CanonicalPath::EpisodeFile { actual_path } => actual_path,
                    _ => String::new(),
                };
                if actual_path.is_empty() {
                    None
                } else {
                    self.runtime
                        .block_on(repo::get_media_entry_by_path(&self.db_pool, &actual_path))
                        .ok()
                        .flatten()
                        .filter(|entry| matches_profile(entry, profile_key.as_deref()))
                }
            }
            _ => None,
        };
        self.entry_cache
            .insert(path.to_string(), (result.clone(), Instant::now()));
        result
    }

    fn get_stream_url_cached(&self, path: &str, entry: &FileSystemEntry) -> Option<String> {
        if let Some(url) = self.stream_url_cache.get(&entry.id) {
            return Some(url.clone());
        }
        if let Some(url) = entry.stream_url.as_deref() {
            self.stream_url_cache.insert(entry.id, url.to_string());
            return Some(url.to_string());
        }
        let url = resolve_stream_url(
            entry.download_url.as_deref(),
            &self.link_request_tx,
            &self.db_pool,
            entry.id,
            &self.runtime,
        )?;
        self.stream_url_cache.insert(entry.id, url.clone());
        self.entry_cache.remove(path);
        Some(url)
    }

    fn get_stream_url_for_open(&self, path: &str, entry: &FileSystemEntry) -> Option<String> {
        // Direct debrid URLs expire, so refresh them when playback starts.
        if entry.download_url.is_some()
            && let Some(url) = resolve_stream_url(
                entry.download_url.as_deref(),
                &self.link_request_tx,
                &self.db_pool,
                entry.id,
                &self.runtime,
            )
        {
            self.stream_url_cache.insert(entry.id, url.clone());
            self.entry_cache.remove(path);
            return Some(url);
        }

        self.get_stream_url_cached(path, entry)
    }

    fn resolve_path(&self, parent_ino: u64, name: &str) -> Arc<str> {
        let parent = match parent_ino {
            ROOT_INO => Arc::<str>::from("/"),
            MOVIES_INO => Arc::<str>::from("/movies"),
            SHOWS_INO => Arc::<str>::from("/shows"),
            _ => self
                .ino_to_path
                .get(&parent_ino)
                .map_or_else(|| Arc::<str>::from("/"), |path| Arc::clone(&path)),
        };

        Arc::from(if parent.as_ref() == "/" {
            format!("/{name}")
        } else {
            format!("{parent}/{name}")
        })
    }
}

impl Filesystem for RivenFs {
    fn init(
        &mut self,
        _req: &Request,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        // Request the maximum readahead the kernel allows. This lets the kernel
        // pipeline read-ahead requests aggressively, reducing latency stalls
        // between successive FUSE reads during sequential playback.
        let _ = config.set_max_readahead(128 * 1024 * 1024);
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy();
        if is_ignored_name(&name) {
            reply.error(libc::ENOENT);
            return;
        }
        let path = self.resolve_path(parent, &name);
        if self.debug_logging {
            tracing::debug!(path = %path, "lookup");
        }
        self.refresh_caches_if_needed();
        let layout = self.current_layout();
        match parse_path(&layout, &path) {
            PathTarget::Root => reply.entry(&TTL, &dir_attr(ROOT_INO), 0),
            PathTarget::ProfilePrefixDir => {
                reply.entry(&TTL, &dir_attr(self.get_or_create_ino(&path)), 0)
            }
            PathTarget::Canonical {
                profile_key,
                path: canonical,
            } => match canonical {
                CanonicalPath::Root => reply.entry(&TTL, &dir_attr(ROOT_INO), 0),
                CanonicalPath::AllMovies => {
                    let ino = if profile_key.is_some() {
                        self.get_or_create_ino(&path)
                    } else {
                        MOVIES_INO
                    };
                    reply.entry(&TTL, &dir_attr(ino), 0);
                }
                CanonicalPath::AllShows => {
                    let ino = if profile_key.is_some() {
                        self.get_or_create_ino(&path)
                    } else {
                        SHOWS_INO
                    };
                    reply.entry(&TTL, &dir_attr(ino), 0);
                }
                CanonicalPath::MovieDir { .. }
                | CanonicalPath::ShowDir { .. }
                | CanonicalPath::SeasonDir { .. } => {
                    reply.entry(&TTL, &dir_attr(self.get_or_create_ino(&path)), 0);
                }
                CanonicalPath::MovieFile { .. } | CanonicalPath::EpisodeFile { .. } => {
                    match self.get_entry_cached(&path) {
                        Some(entry) => {
                            let ino = self.get_or_create_ino(&path);
                            reply.entry(
                                &TTL,
                                &file_attr(ino, entry.file_size as u64, entry_mtime(&entry)),
                                0,
                            );
                        }
                        None => reply.error(libc::ENOENT),
                    }
                }
                CanonicalPath::Invalid => reply.error(libc::ENOENT),
            },
            PathTarget::Invalid => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            ROOT_INO => reply.attr(&TTL, &dir_attr(ROOT_INO)),
            MOVIES_INO => reply.attr(&TTL, &dir_attr(MOVIES_INO)),
            SHOWS_INO => reply.attr(&TTL, &dir_attr(SHOWS_INO)),
            _ => {
                let Some(path) = self.ino_to_path.get(&ino) else {
                    reply.error(libc::ENOENT);
                    return;
                };
                self.refresh_caches_if_needed();
                let layout = self.current_layout();
                match parse_path(&layout, &path) {
                    PathTarget::Canonical {
                        path: CanonicalPath::MovieFile { .. } | CanonicalPath::EpisodeFile { .. },
                        ..
                    } => match self.get_entry_cached(&path) {
                        Some(entry) => reply.attr(
                            &TTL,
                            &file_attr(ino, entry.file_size as u64, entry_mtime(&entry)),
                        ),
                        None => reply.error(libc::ENOENT),
                    },
                    _ => reply.attr(&TTL, &dir_attr(ino)),
                }
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        self.refresh_caches_if_needed();
        let cached = self
            .readdir_cache
            .get(&ino)
            .and_then(|e| (e.1.elapsed() < READDIR_CACHE_TTL).then(|| e.0.clone()));

        let entries = if let Some(entries) = cached {
            entries
        } else {
            let mut entries: Vec<DirEntry> = vec![
                (ino, FileType::Directory, ".".into()),
                (ino, FileType::Directory, "..".into()),
            ];
            let ino_to_path = self.ino_to_path.get(&ino).map(|r| Arc::clone(&r));
            let mut get_ino = |path: &str| self.get_or_create_ino(path);
            let layout = self.current_layout();
            populate_entries(
                ino,
                ino_to_path.as_deref(),
                &self.db_pool,
                &self.runtime,
                &layout,
                &mut entries,
                &mut get_ino,
            );

            let mut seen = HashSet::new();
            let deduped: Vec<DirEntry> = entries
                .into_iter()
                .filter(|(_, _, n)| seen.insert(n.clone()))
                .collect();
            self.readdir_cache
                .insert(ino, (deduped.clone(), Instant::now()));
            deduped
        };

        for (i, (entry_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*entry_ino, (i + 1) as i64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        let Some(path) = self.ino_to_path.get(&ino).map(|r| Arc::clone(&r)) else {
            reply.error(libc::ENOENT);
            return;
        };
        if self.debug_logging {
            tracing::debug!(path = %path, "open");
        }
        let Some(entry) = self.get_entry_cached(&path) else {
            reply.error(libc::ENOENT);
            return;
        };
        let Some(stream_url) = self.get_stream_url_for_open(&path, &entry) else {
            reply.error(if entry.download_url.is_some() {
                libc::EIO
            } else {
                libc::ENOENT
            });
            return;
        };

        let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);
        let file_size = entry.file_size as u64;
        self.file_handles.insert(
            fd,
            FileHandle {
                path,
                stream_url: Arc::from(stream_url),
                media_stream: MediaStream::new(ino, file_size),
            },
        );
        reply.opened(fd, 0);
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let Some(mut handle) = self.file_handles.get_mut(&fh) else {
            reply.error(libc::EBADF);
            return;
        };

        let start = offset as u64;
        if start >= handle.media_stream.file_size() {
            reply.data(&[]);
            return;
        }
        let end = (start + size as u64 - 1).min(handle.media_stream.file_size() - 1);

        if self.debug_logging {
            tracing::debug!(path = %handle.path, offset = start, size, "read");
        }

        let stream_url = Arc::clone(&handle.stream_url);
        match handle.media_stream.read(
            start,
            end,
            &stream_url,
            &self.range_cache,
            &self.stream_client,
            &self.runtime,
            self.debug_logging,
        ) {
            ReadOutcome::Data(buf) => reply.data(&buf),
            ReadOutcome::Error(code) => reply.error(code),
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        if self.debug_logging {
            tracing::debug!(fh, "release");
        }
        self.file_handles.remove(&fh);
        reply.ok();
    }
}

fn matches_profile(entry: &FileSystemEntry, profile_key: Option<&str>) -> bool {
    let Some(profile_key) = profile_key else {
        return true;
    };
    LibraryProfileMembership::from_json(entry.library_profiles.as_ref()).contains(profile_key)
}
