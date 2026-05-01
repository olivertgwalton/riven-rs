use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuser::{
    Errno, FileAttr, FileHandle as FuseFh, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    KernelConfig, LockOwner, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use parking_lot::Mutex;
use tokio::sync::{RwLock, Semaphore};
use tokio::sync::mpsc;

use riven_core::config::vfs::*;
use riven_core::settings::LibraryProfileMembership;
use riven_core::stream_link::request_stream_url_blocking;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::entities::FileSystemEntry;
use riven_db::repo;

use crate::cache::RangeCache;
use crate::chunks::FileLayout;
use crate::media_stream::{MediaStream, ReadOutcome};
use crate::path_info::{CanonicalPath, PathTarget, parse_path};
use crate::readdir::{DirEntry, populate_entries};
use crate::stream::fetch_range;

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
        ino: INodeNo(ino),
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
    UNIX_EPOCH + Duration::from_secs(ts.timestamp().max(0).cast_unsigned())
}

struct OpenedFile {
    entry_id: i64,
    path: Arc<str>,
    stream_url: Arc<str>,
    download_url: Option<Arc<str>>,
    provider: Option<Arc<str>>,
    stream_session: MediaStream,
}

#[derive(Clone)]
struct CachedEntry {
    id: i64,
    file_size: u64,
    mtime: SystemTime,
    download_url: Option<Arc<str>>,
    stream_url: Option<Arc<str>>,
    provider: Option<Arc<str>>,
    library_profiles: LibraryProfileMembership,
}

impl CachedEntry {
    fn from_db(entry: FileSystemEntry) -> Self {
        Self {
            id: entry.id,
            file_size: u64::try_from(entry.file_size).unwrap_or(0),
            mtime: entry_mtime(&entry),
            download_url: entry.download_url.map(Arc::from),
            stream_url: entry.stream_url.map(Arc::from),
            provider: entry.provider.map(Arc::from),
            library_profiles: LibraryProfileMembership::from_json(entry.library_profiles.as_ref()),
        }
    }

    fn matches_profile(&self, profile_key: Option<&str>) -> bool {
        let Some(profile_key) = profile_key else {
            return true;
        };
        self.library_profiles.contains(profile_key)
    }
}

pub struct RivenFs {
    vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    filesystem_settings_revision: Arc<AtomicU64>,
    cache_revision: AtomicU64,
    db_pool: sqlx::PgPool,
    stream_client: reqwest::Client,
    link_request_tx: mpsc::Sender<riven_core::stream_link::LinkRequest>,
    runtime: tokio::runtime::Handle,

    next_fd: AtomicU64,
    // Mutex wrap satisfies fuser 0.17's `Filesystem: Sync` bound — `MediaStream`
    // holds a `BoxStream<Send>` which isn't Sync. The Mutex is uncontended in
    // practice (DashMap already serializes per-key access), but it lets the type
    // system see `OpenedFile` as Sync.
    file_handles: DashMap<u64, Mutex<OpenedFile>>,

    path_to_ino: DashMap<Arc<str>, u64>,
    ino_to_path: DashMap<u64, Arc<str>>,
    next_ino: AtomicU64,

    range_cache: Arc<RangeCache>,
    readdir_cache: DashMap<u64, (Vec<DirEntry>, Instant)>,
    entry_cache: DashMap<String, (Option<Arc<CachedEntry>>, Instant)>,
    prewarm_semaphore: Arc<Semaphore>,
}

impl RivenFs {
    pub fn new(
        vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
        filesystem_settings_revision: Arc<AtomicU64>,
        db_pool: sqlx::PgPool,
        stream_client: reqwest::Client,
        link_request_tx: mpsc::Sender<riven_core::stream_link::LinkRequest>,
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
            runtime: tokio::runtime::Handle::current(),
            next_fd: AtomicU64::new(1),
            file_handles: DashMap::new(),
            path_to_ino,
            ino_to_path,
            next_ino: AtomicU64::new(FIRST_DYNAMIC_INO),
            range_cache: Arc::new(RangeCache::new(entries)),
            readdir_cache: DashMap::new(),
            entry_cache: DashMap::new(),
            prewarm_semaphore: Arc::new(Semaphore::new(8)),
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

    fn get_entry_cached(&self, path: &str) -> Option<Arc<CachedEntry>> {
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
                        .map(CachedEntry::from_db)
                        .filter(|entry| entry.matches_profile(profile_key.as_deref()))
                        .map(Arc::new)
                }
            }
            _ => None,
        };
        self.entry_cache
            .insert(path.to_string(), (result.clone(), Instant::now()));
        result
    }

    fn request_and_persist_stream_url(
        &self,
        path: &str,
        entry_id: i64,
        download_url: Option<&str>,
        provider: Option<&str>,
    ) -> Option<String> {
        let url = request_stream_url_blocking(
            download_url,
            provider,
            &self.link_request_tx,
            &self.runtime,
        )?;
        if let Err(err) = self.runtime.block_on(riven_db::repo::update_stream_url(
            &self.db_pool,
            entry_id,
            &url,
        )) {
            tracing::warn!(entry_id, %err, "failed to persist refreshed stream url");
        }
        self.entry_cache.remove(path);
        Some(url)
    }

    fn resolve_stream_url(&self, path: &str, entry: &CachedEntry) -> Option<String> {
        if let Some(url) = entry.stream_url.as_deref() {
            return Some(url.to_string());
        }

        self.request_and_persist_stream_url(
            path,
            entry.id,
            entry.download_url.as_deref(),
            entry.provider.as_deref(),
        )
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

    fn read_handle(
        &self,
        handle: &mut OpenedFile,
        start: u64,
        end: u64,
        stream_url: &str,
    ) -> ReadOutcome {
        handle.stream_session.read(
            start,
            end,
            stream_url,
            &self.range_cache,
            &self.stream_client,
            &self.runtime,
        )
    }
}

/// Fetches header and footer byte ranges into the shared cache so that a
/// media-server scan (Plex, Jellyfin, etc.) finds them already cached.
///
/// Header and footer are fetched concurrently: the footer fetch races the
/// synchronous header read that the FUSE layer issues right after open,
/// so by the time the first CDN round-trip completes the footer is usually
/// already in cache and the second read returns instantly.
async fn prewarm_header_footer(
    cache: Arc<RangeCache>,
    client: reqwest::Client,
    semaphore: Arc<Semaphore>,
    ino: u64,
    stream_url: String,
    file_size: u64,
) {
    let Ok(_permit) = semaphore.acquire_owned().await else {
        return;
    };

    let layout = FileLayout::new(file_size);
    let header = layout.header_chunk();
    let footer = layout.footer_chunk();

    let fetch_header = async {
        if cache.get((ino, header.start, header.end)).is_none() {
            match fetch_range(&client, &stream_url, header.start, header.end).await {
                Ok(data) => cache.put((ino, header.start, header.end), data),
                Err(e) => tracing::debug!(target: "streaming", ino, error = %e, "pre-warm header failed"),
            }
        }
    };

    let fetch_footer = async {
        if footer != header && cache.get((ino, footer.start, footer.end)).is_none() {
            match fetch_range(&client, &stream_url, footer.start, footer.end).await {
                Ok(data) => cache.put((ino, footer.start, footer.end), data),
                Err(e) => tracing::debug!(target: "streaming", ino, error = %e, "pre-warm footer failed"),
            }
        }
    };

    tokio::join!(fetch_header, fetch_footer);
}

impl Filesystem for RivenFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // Maximum readahead lets the kernel pipeline reads aggressively, reducing
        // latency stalls between successive FUSE reads during sequential playback.
        let _previous = config.set_max_readahead(128 * 1024 * 1024);
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = parent.0;
        let name = name.to_string_lossy();
        if is_ignored_name(&name) {
            reply.error(Errno::ENOENT);
            return;
        }
        let path = self.resolve_path(parent, &name);
        tracing::debug!(target: "streaming", path = %path, "lookup");
        self.refresh_caches_if_needed();
        let layout = self.current_layout();
        match parse_path(&layout, &path) {
            PathTarget::Root => reply.entry(&TTL, &dir_attr(ROOT_INO), Generation(0)),
            PathTarget::ProfilePrefixDir => {
                reply.entry(&TTL, &dir_attr(self.get_or_create_ino(&path)), Generation(0))
            }
            PathTarget::Canonical {
                profile_key,
                path: canonical,
            } => match canonical {
                CanonicalPath::Root => reply.entry(&TTL, &dir_attr(ROOT_INO), Generation(0)),
                CanonicalPath::AllMovies => {
                    let ino = if profile_key.is_some() {
                        self.get_or_create_ino(&path)
                    } else {
                        MOVIES_INO
                    };
                    reply.entry(&TTL, &dir_attr(ino), Generation(0));
                }
                CanonicalPath::AllShows => {
                    let ino = if profile_key.is_some() {
                        self.get_or_create_ino(&path)
                    } else {
                        SHOWS_INO
                    };
                    reply.entry(&TTL, &dir_attr(ino), Generation(0));
                }
                CanonicalPath::MovieDir { .. }
                | CanonicalPath::ShowDir { .. }
                | CanonicalPath::SeasonDir { .. } => {
                    reply.entry(
                        &TTL,
                        &dir_attr(self.get_or_create_ino(&path)),
                        Generation(0),
                    );
                }
                CanonicalPath::MovieFile { .. } | CanonicalPath::EpisodeFile { .. } => {
                    match self.get_entry_cached(&path) {
                        Some(entry) => {
                            let ino = self.get_or_create_ino(&path);
                            reply.entry(
                                &TTL,
                                &file_attr(ino, entry.file_size, entry.mtime),
                                Generation(0),
                            );
                        }
                        None => reply.error(Errno::ENOENT),
                    }
                }
                CanonicalPath::Invalid => reply.error(Errno::ENOENT),
            },
            PathTarget::Invalid => reply.error(Errno::ENOENT),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FuseFh>, reply: ReplyAttr) {
        let ino = ino.0;
        match ino {
            ROOT_INO => reply.attr(&TTL, &dir_attr(ROOT_INO)),
            MOVIES_INO => reply.attr(&TTL, &dir_attr(MOVIES_INO)),
            SHOWS_INO => reply.attr(&TTL, &dir_attr(SHOWS_INO)),
            _ => {
                let Some(path) = self.ino_to_path.get(&ino) else {
                    reply.error(Errno::ENOENT);
                    return;
                };
                self.refresh_caches_if_needed();
                let layout = self.current_layout();
                match parse_path(&layout, &path) {
                    PathTarget::Canonical {
                        path: CanonicalPath::MovieFile { .. } | CanonicalPath::EpisodeFile { .. },
                        ..
                    } => match self.get_entry_cached(&path) {
                        Some(entry) => {
                            reply.attr(&TTL, &file_attr(ino, entry.file_size, entry.mtime))
                        }
                        None => reply.error(Errno::ENOENT),
                    },
                    _ => reply.attr(&TTL, &dir_attr(ino)),
                }
            }
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FuseFh,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let ino = ino.0;
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
            if reply.add(INodeNo(*entry_ino), (i + 1) as u64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let ino = ino.0;
        let Some(path) = self.ino_to_path.get(&ino).map(|r| Arc::clone(&r)) else {
            reply.error(Errno::ENOENT);
            return;
        };
        tracing::debug!(target: "streaming", path = %path, "open");
        let Some(entry) = self.get_entry_cached(&path) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(stream_url) = self.resolve_stream_url(&path, &entry) else {
            reply.error(if entry.download_url.is_some() {
                Errno::EIO
            } else {
                Errno::ENOENT
            });
            return;
        };

        let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);
        let file_size = entry.file_size;

        // Pre-warm header and footer bytes so media-server scans hit the cache,
        // not the CDN.
        self.runtime.spawn(prewarm_header_footer(
            Arc::clone(&self.range_cache),
            self.stream_client.clone(),
            Arc::clone(&self.prewarm_semaphore),
            ino,
            stream_url.clone(),
            file_size,
        ));

        self.file_handles.insert(
            fd,
            Mutex::new(OpenedFile {
                entry_id: entry.id,
                path,
                stream_url: Arc::from(stream_url),
                download_url: entry.download_url.clone(),
                provider: entry.provider.clone(),
                stream_session: MediaStream::new(ino, file_size),
            }),
        );
        reply.opened(FuseFh(fd), FopenFlags::FOPEN_KEEP_CACHE);
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FuseFh,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let fh = fh.0;
        let Some(entry) = self.file_handles.get(&fh) else {
            reply.error(Errno::EBADF);
            return;
        };
        let mut handle = entry.lock();

        let start = offset;
        if start >= handle.stream_session.file_size() {
            reply.data(&[]);
            return;
        }
        let end = (start + size as u64 - 1).min(handle.stream_session.file_size() - 1);

        tracing::debug!(target: "streaming", path = %handle.path, offset = start, size, "read");

        let stream_url = Arc::clone(&handle.stream_url);
        let outcome = match self.read_handle(&mut handle, start, end, &stream_url) {
            ReadOutcome::Data(buf) => ReadOutcome::Data(buf),
            ReadOutcome::Error(code) => {
                let Some(download_url) = handle.download_url.as_ref().map(Arc::clone) else {
                    return reply.error(Errno::from_i32(code));
                };
                let path = Arc::clone(&handle.path);
                let entry_id = handle.entry_id;
                let provider = handle.provider.as_ref().map(Arc::clone);

                tracing::warn!(
                    path = %path,
                    offset = start,
                    size,
                    code,
                    "read failed, refreshing stream url and retrying once"
                );

                let Some(url) = self.request_and_persist_stream_url(
                    &path,
                    entry_id,
                    Some(download_url.as_ref()),
                    provider.as_deref(),
                ) else {
                    return reply.error(Errno::from_i32(code));
                };
                handle.stream_url = Arc::from(url);
                let refreshed = Arc::clone(&handle.stream_url);
                self.read_handle(&mut handle, start, end, &refreshed)
            }
        };

        match outcome {
            ReadOutcome::Data(buf) => reply.data(&buf),
            ReadOutcome::Error(code) => reply.error(Errno::from_i32(code)),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FuseFh,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let fh = fh.0;
        tracing::debug!(target: "streaming", fh, "release");
        self.file_handles.remove(&fh);
        reply.ok();
    }
}
