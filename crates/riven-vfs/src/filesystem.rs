use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuser::{
    Errno, FileAttr, FileHandle as FuseFh, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    KernelConfig, LockOwner, OpenFlags, PollEvents, PollFlags, PollNotifier, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyPoll, Request,
};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::{RwLock, Semaphore};

use riven_core::config::vfs::*;
use riven_core::stream_link::request_stream_url_blocking;
use riven_core::types::FileSystemEntryType;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::repo;

use crate::path_info::{CanonicalPath, PathTarget, parse_path};
use crate::prefetch::Prefetcher;
use crate::readdir::{DirEntry, populate_entries};
use crate::source::{ByteSource, HttpSource, LinkRefresher, UsenetSource};
use crate::state::{CachedEntry, MOVIES_INO, OpenedFile, ROOT_INO, SHOWS_INO, VfsState};
use riven_core::local_source::parse_usenet_url;

const TTL: Duration = Duration::from_secs(300);

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

/// Inner state shared via `Arc` so FUSE handlers can hand the heavy I/O work
/// off to tokio without borrowing from `&self`. The fuser session has one
/// dispatcher thread that loops reading kernel requests; if a handler does a
/// synchronous `runtime.block_on(...)` on that thread, the entire mount
/// head-of-line blocks until the future completes. Cloning this `Arc` into a
/// `spawn_blocking` closure lets the dispatcher return immediately while the
/// real work runs on tokio's blocking-task pool, so a slow read on one file
/// no longer wedges every other FUSE op.

/// Decrements the in-flight counter when the read task ends, on every path
/// (reply, error, or early return).
struct InflightGuard(Arc<FuseStats>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// FUSE-level counters. The prefetcher sees its own hit rate, but only this
/// layer sees what the *player* experiences: total time from the kernel's
/// read request to the reply, including semaphore admission. A stall the
/// player notices always shows up here, whatever caused it.
#[derive(Default)]
struct FuseStats {
    reads: AtomicU64,
    /// Reads currently being served — if this pins at the semaphore limit,
    /// admission is the bottleneck rather than the origin.
    inflight: AtomicU64,
    slow_reads: AtomicU64,
    total_us: AtomicU64,
    worst_us: AtomicU64,
    /// Time spent waiting for a read permit, isolated from fetch time.
    permit_wait_us: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    last_log_secs: AtomicU64,
}

impl FuseStats {
    /// Record one completed FUSE read and, every 10s, emit a summary.
    ///
    /// `permit_us` separates admission from service: if it dominates, the
    /// 32-permit gate is the bottleneck, not the origin. `worst_us` is what
    /// the player felt at its worst — a single multi-second read is a visible
    /// stutter even when the average looks fine.
    fn record_read(&self, permit_us: u64, total_us: u64, bytes: usize, ok: bool) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(total_us, Ordering::Relaxed);
        self.permit_wait_us.fetch_add(permit_us, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        if !ok {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.worst_us.fetch_max(total_us, Ordering::Relaxed);
        if total_us >= 500_000 {
            self.slow_reads.fetch_add(1, Ordering::Relaxed);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let last = self.last_log_secs.load(Ordering::Relaxed);
        if now < last + 10
            || self
                .last_log_secs
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let reads = self.reads.load(Ordering::Relaxed).max(1);
        tracing::info!(
            target: "streaming",
            reads,
            inflight = self.inflight.load(Ordering::Relaxed),
            avg_ms = self.total_us.load(Ordering::Relaxed) / reads / 1000,
            worst_ms = self.worst_us.swap(0, Ordering::Relaxed) / 1000,
            slow_reads = self.slow_reads.load(Ordering::Relaxed),
            avg_permit_ms = self.permit_wait_us.load(Ordering::Relaxed) / reads / 1000,
            errors = self.errors.load(Ordering::Relaxed),
            served_mb = self.bytes.load(Ordering::Relaxed) >> 20,
            "fuse read stats"
        );
    }
}

struct RivenFsInner {
    vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    filesystem_settings_revision: Arc<AtomicU64>,
    stream_client: reqwest::Client,
    link_request_tx: mpsc::Sender<riven_core::stream_link::LinkRequest>,
    runtime: tokio::runtime::Handle,

    state: VfsState,

    /// Ceiling on one file's adaptive read-ahead window.
    max_prefetch_window: u64,
    read_semaphore: Arc<Semaphore>,
    fuse_stats: Arc<FuseStats>,
    link_refresh_locks: DashMap<i64, Arc<Mutex<()>>>,
    local_source: Option<Arc<dyn riven_core::local_source::LocalByteSource>>,
}

pub struct RivenFs {
    inner: Arc<RivenFsInner>,
}

impl RivenFs {
    pub fn new(
        vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
        filesystem_settings_revision: Arc<AtomicU64>,
        stream_client: reqwest::Client,
        link_request_tx: mpsc::Sender<riven_core::stream_link::LinkRequest>,
        cache_max_size_mb: u64,
        local_source: Option<Arc<dyn riven_core::local_source::LocalByteSource>>,
    ) -> Self {
        Self {
            inner: Arc::new(RivenFsInner::new(
                vfs_layout,
                filesystem_settings_revision,
                stream_client,
                link_request_tx,
                cache_max_size_mb,
                local_source,
            )),
        }
    }
}

impl RivenFsInner {
    fn new(
        vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
        filesystem_settings_revision: Arc<AtomicU64>,
        stream_client: reqwest::Client,
        link_request_tx: mpsc::Sender<riven_core::stream_link::LinkRequest>,
        cache_max_size_mb: u64,
        local_source: Option<Arc<dyn riven_core::local_source::LocalByteSource>>,
    ) -> Self {
        let cache_capacity_bytes = if cache_max_size_mb == 0 {
            50 * 1024 * 1024
        } else {
            (cache_max_size_mb * 1024 * 1024) as usize
        };
        Self {
            vfs_layout,
            filesystem_settings_revision,
            stream_client,
            link_request_tx,
            runtime: tokio::runtime::Handle::current(),
            state: VfsState::new(),
            max_prefetch_window: cache_capacity_bytes as u64,
            // Backstop only. Reads are async and normally served from the
            // prefetch buffer, which bounds real fetches itself — measured
            // permit wait is 0 and in-flight peaks in single digits during
            // playback. This exists purely so a pathological fan-out (a media
            // server analysing a whole library at once) cannot spawn unbounded
            // read tasks.
            read_semaphore: Arc::new(Semaphore::new(32)),
            fuse_stats: Arc::new(FuseStats::default()),
            link_refresh_locks: DashMap::new(),
            local_source,
        }
    }

    fn current_layout(&self) -> VfsLibraryLayout {
        self.vfs_layout.blocking_read().clone()
    }

    fn refresh_caches_if_needed(&self) {
        let revision = self.filesystem_settings_revision.load(Ordering::SeqCst);
        self.state.refresh(revision);
    }

    /// Resolve a VFS path to its current `filesystem_entries` row. Always
    /// re-queries the DB — there is no in-process cache here, mirroring the
    /// approach the TypeScript implementation took. Caching the entry keyed by
    /// path was a footgun: when a row's `path`/`download_url`/`media_item_id`
    /// got rewritten (re-scrape, library rebuild), the cache kept serving the
    /// pre-rewrite mapping until process restart, leaving Plex hammering dead
    /// debrid links forever. The hot per-FUSE-op caches (`vfs_layout`,
    /// `path_to_ino`, `readdir_cache`) are unaffected, so this only adds a
    /// single indexed lookup per `open()` / metadata-stat — measured impact
    /// is sub-millisecond compared with a media read.
    fn get_entry(&self, path: &str) -> Option<Arc<CachedEntry>> {
        let layout = self.current_layout();
        let (profile_key, actual_path) = match parse_path(&layout, path) {
            PathTarget::Canonical {
                profile_key,
                path: canonical,
            } => {
                let actual_path = match canonical {
                    CanonicalPath::MovieFile { actual_path }
                    | CanonicalPath::EpisodeFile { actual_path } => actual_path,
                    _ => return None,
                };
                if actual_path.is_empty() {
                    return None;
                }
                (profile_key, actual_path)
            }
            _ => return None,
        };
        self.runtime
            .block_on(repo::get_filesystem_entry_by_path(&actual_path))
            .ok()
            .flatten()
            .map(CachedEntry::from_db)
            .filter(|entry| entry.matches_profile(profile_key.as_deref()))
            .map(Arc::new)
    }

    /// Mint a fresh stream URL for an entry and persist it, coalescing
    /// concurrent callers. `current_url` is the URL the caller already knows is
    /// unusable (or `None` if the entry had no URL yet) — once the per-entry
    /// lock is held, the DB is re-checked and any *different* URL a peer
    /// persisted while we waited is returned instead of firing another request.
    fn request_and_persist_stream_url(
        &self,
        entry_id: i64,
        download_url: Option<&str>,
        provider: Option<&str>,
        current_url: Option<&str>,
    ) -> Option<String> {
        let lock = self
            .link_refresh_locks
            .entry(entry_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = lock.lock();

        if let Ok(Some(entry)) = self
            .runtime
            .block_on(riven_db::repo::get_media_entry_by_id(entry_id))
            && let Some(fresh) = entry.stream_url
            && Some(fresh.as_str()) != current_url
        {
            drop(guard);
            self.link_refresh_locks
                .remove_if(&entry_id, |_, arc| Arc::strong_count(arc) <= 2);
            return Some(fresh);
        }

        let url = request_stream_url_blocking(
            download_url,
            provider,
            Some(entry_id),
            current_url,
            &self.link_request_tx,
            &self.runtime,
        );

        if let Some(url) = url.as_deref()
            && let Err(err) = self
                .runtime
                .block_on(riven_db::repo::update_stream_url(entry_id, url))
        {
            tracing::warn!(entry_id, %err, "failed to persist refreshed stream url");
        }

        drop(guard);
        self.link_refresh_locks
            .remove_if(&entry_id, |_, arc| Arc::strong_count(arc) <= 2);
        url
    }

    fn resolve_stream_url(&self, entry: &CachedEntry) -> Option<String> {
        if let Some(url) = entry.stream_url.as_deref() {
            return Some(url.to_string());
        }

        self.request_and_persist_stream_url(
            entry.id,
            entry.download_url.as_deref(),
            entry.provider.as_deref(),
            None,
        )
    }
}

impl Filesystem for RivenFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // Network reads legitimately remain outstanding while the origin
        // responds. Fuser defaults to 16 background requests, which makes the
        // kernel declare congestion at 12 and suppress its own asynchronous
        // readahead. Mountpoint S3 uses 64 for the same network-filesystem
        // workload; keep the threshold explicit so this stays intentional.
        config.set_max_background(64).map_err(|minimum| {
            std::io::Error::other(format!("max_background must be >= {minimum}"))
        })?;
        config.set_congestion_threshold(48).map_err(|minimum| {
            std::io::Error::other(format!("congestion_threshold must be >= {minimum}"))
        })?;
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let s = &self.inner;
        let parent = parent.0;
        let name = name.to_string_lossy();
        if is_ignored_name(&name) {
            reply.error(Errno::ENOENT);
            return;
        }
        let path = s.state.resolve_path(parent, &name);
        tracing::debug!(target: "streaming", path = %path, "lookup");
        s.refresh_caches_if_needed();
        let layout = s.current_layout();
        match parse_path(&layout, &path) {
            PathTarget::Root => reply.entry(&TTL, &dir_attr(ROOT_INO), Generation(0)),
            PathTarget::ProfilePrefixDir => reply.entry(
                &TTL,
                &dir_attr(s.state.get_or_create_ino(&path)),
                Generation(0),
            ),
            PathTarget::Canonical {
                profile_key,
                path: canonical,
            } => match canonical {
                CanonicalPath::Root => reply.entry(&TTL, &dir_attr(ROOT_INO), Generation(0)),
                CanonicalPath::AllMovies => {
                    let ino = if profile_key.is_some() {
                        s.state.get_or_create_ino(&path)
                    } else {
                        MOVIES_INO
                    };
                    reply.entry(&TTL, &dir_attr(ino), Generation(0));
                }
                CanonicalPath::AllShows => {
                    let ino = if profile_key.is_some() {
                        s.state.get_or_create_ino(&path)
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
                        &dir_attr(s.state.get_or_create_ino(&path)),
                        Generation(0),
                    );
                }
                CanonicalPath::MovieFile { .. } | CanonicalPath::EpisodeFile { .. } => {
                    match s.get_entry(&path) {
                        Some(entry) => {
                            let ino = s.state.get_or_create_ino(&path);
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
        let s = &self.inner;
        let ino = ino.0;
        match ino {
            ROOT_INO => reply.attr(&TTL, &dir_attr(ROOT_INO)),
            MOVIES_INO => reply.attr(&TTL, &dir_attr(MOVIES_INO)),
            SHOWS_INO => reply.attr(&TTL, &dir_attr(SHOWS_INO)),
            _ => {
                let Some(path) = s.state.path(ino) else {
                    reply.error(Errno::ENOENT);
                    return;
                };
                s.refresh_caches_if_needed();
                let layout = s.current_layout();
                match parse_path(&layout, &path) {
                    PathTarget::Canonical {
                        path: CanonicalPath::MovieFile { .. } | CanonicalPath::EpisodeFile { .. },
                        ..
                    } => match s.get_entry(&path) {
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
        let s = &self.inner;
        let ino = ino.0;
        s.refresh_caches_if_needed();
        let cached = s.state.directory_entries(ino);

        let entries = if let Some(entries) = cached {
            entries
        } else {
            let mut entries: Vec<DirEntry> = vec![
                (ino, FileType::Directory, ".".into()),
                (ino, FileType::Directory, "..".into()),
            ];
            let ino_to_path = s.state.path(ino);
            let mut get_ino = |path: &str| s.state.get_or_create_ino(path);
            let layout = s.current_layout();
            populate_entries(
                ino,
                ino_to_path.as_deref(),
                &s.runtime,
                &layout,
                &mut entries,
                &mut get_ino,
            );

            let mut seen = HashSet::new();
            let deduped: Vec<DirEntry> = entries
                .into_iter()
                .filter(|(_, _, n)| seen.insert(n.clone()))
                .collect();
            s.state.cache_directory_entries(ino, deduped.clone());
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
        let s = &self.inner;
        let ino = ino.0;
        let Some(path) = s.state.path(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        tracing::debug!(target: "streaming", path = %path, "open");
        let Some(entry) = s.get_entry(&path) else {
            reply.error(Errno::ENOENT);
            return;
        };

        if entry.entry_type == FileSystemEntryType::Subtitle {
            let Some(content) = entry.subtitle_content.clone() else {
                reply.error(Errno::ENOENT);
                return;
            };
            let fd = s.state.open(OpenedFile::Subtitle { content });
            reply.opened(FuseFh(fd), FopenFlags::FOPEN_KEEP_CACHE);
            return;
        }

        let file_size = entry.file_size;

        let usenet_id = match (&entry.usenet_info_hash, entry.usenet_file_index) {
            (Some(ih), Some(idx)) => Some((ih.to_string(), idx)),
            _ => entry
                .stream_url
                .as_deref()
                .or(entry.download_url.as_deref())
                .and_then(parse_usenet_url),
        };
        if let (Some(source), Some((info_hash, file_index))) = (s.local_source.clone(), usenet_id) {
            let filename: Arc<str> = Arc::from(path.rsplit('/').next().unwrap_or(&path));
            let byte_source: Arc<dyn ByteSource> = Arc::new(UsenetSource::new(
                source,
                Arc::from(info_hash.as_str()),
                file_index,
                file_size,
                &filename,
            ));
            let fd = s.state.open(OpenedFile::Streamed {
                path,
                prefetcher: Arc::new(Prefetcher::new(
                    byte_source,
                    s.max_prefetch_window,
                    &s.runtime,
                )),
            });
            reply.opened(FuseFh(fd), FopenFlags::FOPEN_KEEP_CACHE);
            return;
        }

        let Some(stream_url) = s.resolve_stream_url(&entry) else {
            reply.error(if entry.download_url.is_some() {
                Errno::EIO
            } else {
                Errno::ENOENT
            });
            return;
        };

        // Lets a dying link be re-minted mid-playback instead of failing the
        // read; reuses the same coalesced refresh path as `open`.
        let refresher: LinkRefresher = {
            let inner = Arc::clone(&self.inner);
            let entry_id = entry.id;
            let download_url = entry.download_url.clone();
            let provider = entry.provider.clone();
            let stale: Arc<str> = Arc::from(stream_url.as_str());
            Arc::new(move || {
                inner
                    .request_and_persist_stream_url(
                        entry_id,
                        download_url.as_deref(),
                        provider.as_deref(),
                        Some(stale.as_ref()),
                    )
                    .map(|url| Arc::from(url.as_str()))
            })
        };
        let byte_source: Arc<dyn ByteSource> = Arc::new(HttpSource::new(
            s.stream_client.clone(),
            Arc::from(stream_url),
            file_size,
            Some(refresher),
        ));
        let fd = s.state.open(OpenedFile::Streamed {
            path,
            prefetcher: Arc::new(Prefetcher::new(
                byte_source,
                s.max_prefetch_window,
                &s.runtime,
            )),
        });
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
        // Fully async: the FUSE dispatcher returns immediately and the read is
        // driven on the tokio runtime. Nothing blocks a runtime worker, and
        // nothing holds a lock across the network fetch, so concurrent reads
        // on one open file overlap instead of serialising behind each other.
        // The semaphore bounds how many reads may be in flight, so a player's
        // analyser fan-out cannot queue unbounded work.
        let inner = Arc::clone(&self.inner);
        let fh = fh.0;
        self.inner.runtime.spawn(async move {
            let began = std::time::Instant::now();
            let Ok(_permit) = inner.read_semaphore.clone().acquire_owned().await else {
                reply.error(Errno::EIO);
                return;
            };
            let permit_us = began.elapsed().as_micros() as u64;
            let stats = Arc::clone(&inner.fuse_stats);
            stats.inflight.fetch_add(1, Ordering::Relaxed);
            let _guard = InflightGuard(Arc::clone(&stats));

            // Clone what is needed out of the handle map, then drop the guard.
            let opened = {
                let Some(entry) = inner.state.file_handles.get(&fh) else {
                    reply.error(Errno::EBADF);
                    return;
                };
                let guard = entry.lock();
                match &*guard {
                    OpenedFile::Subtitle { content } => Ok(Arc::clone(content)),
                    OpenedFile::Streamed { prefetcher, path } => {
                        Err((Arc::clone(prefetcher), Arc::clone(path)))
                    }
                }
            };

            match opened {
                Ok(content) => {
                    let len = content.len() as u64;
                    if offset >= len {
                        reply.data(&[]);
                        return;
                    }
                    let end = (offset + u64::from(size)).min(len);
                    reply.data(&content[offset as usize..end as usize]);
                }
                Err((prefetcher, path)) => match prefetcher.read(offset, size as usize).await {
                    Ok(data) => {
                        let total_us = began.elapsed().as_micros() as u64;
                        // Surfaced individually as well as in aggregate: one
                        // multi-second read is a visible stutter, and the
                        // offset tells us whether it was a seek or a stall
                        // mid-sequence.
                        if total_us >= 1_000_000 {
                            tracing::warn!(
                                target: "streaming",
                                path = %path, offset, size,
                                took_ms = total_us / 1000,
                                permit_ms = permit_us / 1000,
                                "slow playback read"
                            );
                        }
                        stats.record_read(permit_us, total_us, data.len(), true);
                        reply.data(&data);
                    }
                    Err(error) => {
                        stats.record_read(permit_us, began.elapsed().as_micros() as u64, 0, false);
                        tracing::warn!(
                            target: "streaming",
                            path = %path, offset, size, %error,
                            "read failed"
                        );
                        reply.error(Errno::EIO);
                    }
                },
            }
        });
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FuseFh,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
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
        let s = &self.inner;
        let fh = fh.0;
        tracing::debug!(target: "streaming", fh, "release");
        s.state.close(fh);
        reply.ok();
    }

    fn poll(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FuseFh,
        _ph: PollNotifier,
        _events: PollEvents,
        _flags: PollFlags,
        reply: ReplyPoll,
    ) {
        // Media files are immutable and a `read` will either return bytes or
        // a regular I/O error. Report them immediately readable so media
        // servers do not fall back after an ENOSYS response from fuser's
        // default handler.
        reply.poll(PollEvents::POLLIN);
    }
}
