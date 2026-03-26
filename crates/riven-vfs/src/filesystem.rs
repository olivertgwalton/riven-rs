use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    Request,
};
use lru::LruCache;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use riven_core::config::vfs::*;
use riven_db::entities::FileSystemEntry;
use riven_db::repo;

use crate::cache::RangeCache;
use crate::chunks::FileLayout;
use crate::detect::detect_read_type;
use crate::fetcher::{serve_read, ReadOutcome, resolve_stream_url};
use crate::path_info::{parse_path, PathType};
use crate::prefetch::Prefetch;
use crate::readdir::{populate_entries, DirEntry};
use crate::LinkRequest;

const TTL: Duration = Duration::from_secs(300);
const READDIR_CACHE_TTL: Duration = Duration::from_secs(30);

const ROOT_INO: u64 = 1;
const MOVIES_INO: u64 = 2;
const SHOWS_INO: u64 = 3;
const FIRST_DYNAMIC_INO: u64 = 100;

/// Reject hidden files (`.trickplay`, `.nfo`, etc.) and known ignored names
/// that media servers probe for but the VFS never serves.
fn is_ignored_name(name: &str) -> bool {
    name.starts_with('.') || name.eq_ignore_ascii_case("folder.jpg")
}

fn make_attr(ino: u64, kind: FileType, size: u64, mtime: SystemTime) -> FileAttr {
    let is_dir = kind == FileType::Directory;
    FileAttr {
        ino,
        size,
        blocks: if is_dir { 0 } else { (size + BLOCK_SIZE - 1) / BLOCK_SIZE },
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
    let ts = entry.updated_at.unwrap_or(entry.created_at);
    UNIX_EPOCH + Duration::from_secs(ts.timestamp().max(0) as u64)
}

struct FileHandle {
    ino: u64,
    path: Arc<str>,
    stream_url: String,
    file_size: u64,
    layout: FileLayout,
    previous_read_pos: Option<u64>,
    prefetch: Option<Prefetch>,
}

pub struct RivenFs {
    db_pool: sqlx::PgPool,
    http_client: reqwest::Client,
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
        db_pool: sqlx::PgPool,
        http_client: reqwest::Client,
        link_request_tx: mpsc::Sender<LinkRequest>,
        debug_logging: bool,
        cache_max_size_mb: u64,
    ) -> Self {
        let entries = if cache_max_size_mb == 0 { 256 } else { cache_max_size_mb as usize };
        Self {
            db_pool,
            http_client,
            link_request_tx,
            debug_logging,
            runtime: tokio::runtime::Handle::current(),
            next_fd: AtomicU64::new(1),
            file_handles: DashMap::new(),
            path_to_ino: DashMap::new(),
            ino_to_path: DashMap::new(),
            next_ino: AtomicU64::new(FIRST_DYNAMIC_INO),
            range_cache: Mutex::new(LruCache::new(std::num::NonZeroUsize::new(entries).unwrap())),
            readdir_cache: DashMap::new(),
            entry_cache: DashMap::new(),
            stream_url_cache: DashMap::new(),
        }
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
        if let Some(cached) = self.entry_cache.get(path) {
            if cached.1.elapsed() < TTL {
                return cached.0.clone();
            }
        }
        let result = self
            .runtime
            .block_on(repo::get_media_entry_by_path(&self.db_pool, path))
            .ok()
            .flatten();
        self.entry_cache.insert(path.to_string(), (result.clone(), Instant::now()));
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

    fn resolve_path(&self, parent_ino: u64, name: &str) -> Arc<str> {
        let parent: Arc<str> = match parent_ino {
            ROOT_INO => Arc::from("/"),
            MOVIES_INO => Arc::from("/movies"),
            SHOWS_INO => Arc::from("/shows"),
            _ => self.ino_to_path.get(&parent_ino).map(|r| Arc::clone(&r)).unwrap_or_else(|| Arc::from("/")),
        };
        if &*parent == "/" {
            Arc::from(format!("/{name}").as_str())
        } else {
            Arc::from(format!("{parent}/{name}").as_str())
        }
    }
}

impl Filesystem for RivenFs {
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
        match parse_path(&path) {
            PathType::Root => reply.entry(&TTL, &dir_attr(ROOT_INO), 0),
            PathType::AllMovies => reply.entry(&TTL, &dir_attr(MOVIES_INO), 0),
            PathType::AllShows => reply.entry(&TTL, &dir_attr(SHOWS_INO), 0),
            PathType::MovieDir { .. } | PathType::ShowDir { .. } | PathType::SeasonDir { .. } => {
                reply.entry(&TTL, &dir_attr(self.get_or_create_ino(&path)), 0);
            }
            PathType::MovieFile { .. } | PathType::EpisodeFile { .. } => {
                match self.get_entry_cached(&path) {
                    Some(entry) => {
                        let ino = self.get_or_create_ino(&path);
                        reply.entry(&TTL, &file_attr(ino, entry.file_size as u64, entry_mtime(&entry)), 0);
                    }
                    None => reply.error(libc::ENOENT),
                }
            }
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
                match parse_path(&path) {
                    PathType::MovieFile { .. } | PathType::EpisodeFile { .. } => {
                        match self.get_entry_cached(&*path) {
                            Some(entry) => reply.attr(&TTL, &file_attr(ino, entry.file_size as u64, entry_mtime(&entry))),
                            None => reply.error(libc::ENOENT),
                        }
                    }
                    _ => reply.attr(&TTL, &dir_attr(ino)),
                }
            }
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let cached = self.readdir_cache.get(&ino).and_then(|e| {
            (e.1.elapsed() < READDIR_CACHE_TTL).then(|| e.0.clone())
        });

        let entries = if let Some(entries) = cached {
            entries
        } else {
            let mut entries: Vec<DirEntry> = vec![
                (ino, FileType::Directory, ".".into()),
                (ino, FileType::Directory, "..".into()),
            ];
            let ino_to_path = self.ino_to_path.get(&ino).map(|r| Arc::clone(&r));
            let mut get_ino = |path: &str| self.get_or_create_ino(path);
            populate_entries(ino, ino_to_path.as_deref(), &self.db_pool, &self.runtime, &mut entries, &mut get_ino);

            let mut seen = HashSet::new();
            let deduped: Vec<DirEntry> = entries.into_iter().filter(|(_, _, n)| seen.insert(n.clone())).collect();
            self.readdir_cache.insert(ino, (deduped.clone(), Instant::now()));
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
        let Some(entry) = self.get_entry_cached(&*path) else {
            reply.error(libc::ENOENT);
            return;
        };
        let Some(stream_url) = self.get_stream_url_cached(&*path, &entry) else {
            reply.error(if entry.download_url.is_some() { libc::EIO } else { libc::ENOENT });
            return;
        };

        let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);
        let file_size = entry.file_size as u64;
        self.file_handles.insert(fd, FileHandle {
            ino,
            path,
            stream_url,
            file_size,
            layout: FileLayout::new(file_size),
            previous_read_pos: None,
            prefetch: None,
        });
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
        if start >= handle.file_size {
            reply.data(&[]);
            return;
        }
        let end = (start + size as u64 - 1).min(handle.file_size - 1);
        let read_type = detect_read_type(start, handle.previous_read_pos, handle.layout.header_end);

        if self.debug_logging {
            tracing::debug!(path = %handle.path, offset = start, size, read_type = ?read_type, "read");
        }

        handle.previous_read_pos = Some(start);
        let stream_url = handle.stream_url.clone();

        match serve_read(read_type, handle.ino, start, end, &stream_url, &self.range_cache, &self.http_client, &self.runtime, &mut handle.prefetch, self.debug_logging) {
            ReadOutcome::Data(buf) => reply.data(&buf),
            ReadOutcome::Error(code) => reply.error(code),
        }
    }

    fn release(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: fuser::ReplyEmpty) {
        if self.debug_logging {
            tracing::debug!(fh, "release");
        }
        self.file_handles.remove(&fh);
        reply.ok();
    }
}
