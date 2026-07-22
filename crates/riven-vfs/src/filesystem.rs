use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuser::{
    Errno, FileAttr, FileHandle as FuseFh, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    LockOwner, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    Request,
};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::{RwLock, Semaphore};

use riven_core::config::vfs::*;
use riven_core::stream_link::request_stream_url_blocking;
use riven_core::types::FileSystemEntryType;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::repo;

use crate::cache::RangeCache;
use crate::chunks::FileLayout;
use crate::media_stream::{MediaStream, ReadOutcome, UsenetSession};
use crate::path_info::{CanonicalPath, PathTarget, parse_path};
use crate::readdir::{DirEntry, populate_entries};
use crate::state::{CachedEntry, MOVIES_INO, OpenedFile, ROOT_INO, SHOWS_INO, VfsState};
use crate::stream::fetch_range;
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
struct RivenFsInner {
    vfs_layout: Arc<RwLock<VfsLibraryLayout>>,
    filesystem_settings_revision: Arc<AtomicU64>,
    stream_client: reqwest::Client,
    link_request_tx: mpsc::Sender<riven_core::stream_link::LinkRequest>,
    runtime: tokio::runtime::Handle,

    state: VfsState,

    range_cache: Arc<RangeCache>,
    prewarm_semaphore: Arc<Semaphore>,
    read_semaphore: Arc<Semaphore>,
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
            range_cache: Arc::new(RangeCache::new(cache_capacity_bytes)),
            prewarm_semaphore: Arc::new(Semaphore::new(8)),
            // Bound upstream reads independently of tokio's much larger
            // blocking-task pool. This prevents scans or concurrent players
            // from creating hundreds of live HTTP buffers at once.
            read_semaphore: Arc::new(Semaphore::new(32)),
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
    /// is sub-millisecond and dwarfed by the per-file CDN/NNTP prewarm.
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

    /// A stream-URL refresh failed. If the handle's entry was deleted out from
    /// under it — typically because a dead-link re-download replaced it with a
    /// fresh entry at the same path — the open handle is stale. Rebind it to
    /// the new entry so the in-flight read can be retried instead of failing.
    /// Returns the new stream URL, or `None` if there is no fresh entry to
    /// rebind to (a genuine dead end).
    fn rebind_stale_handle(
        &self,
        handle: &mut OpenedFile,
        path: &str,
        stale_entry_id: i64,
    ) -> Option<Arc<str>> {
        let fresh = self.get_entry(path)?;
        if fresh.id == stale_entry_id || fresh.entry_type != FileSystemEntryType::Media {
            return None;
        }
        let url: Arc<str> = Arc::from(self.resolve_stream_url(&fresh)?);
        if let OpenedFile::Media {
            entry_id,
            stream_url,
            download_url,
            provider,
            ..
        } = handle
        {
            tracing::warn!(
                path,
                stale_entry_id,
                new_entry_id = fresh.id,
                "open handle outlived its entry — rebinding to the replacement"
            );
            *entry_id = fresh.id;
            *download_url = fresh.download_url.clone();
            *provider = fresh.provider.clone();
            *stream_url = Arc::clone(&url);
        }
        Some(url)
    }

    fn read_handle(
        &self,
        handle: &mut OpenedFile,
        start: u64,
        end: u64,
        stream_url: &str,
    ) -> ReadOutcome {
        let OpenedFile::Media { stream_session, .. } = handle else {
            return ReadOutcome::Error(libc::EIO);
        };
        stream_session.read(
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
                Err(e) => {
                    tracing::debug!(target: "streaming", ino, error = %e, "pre-warm header failed")
                }
            }
        }
    };

    let fetch_footer = async {
        if footer != header && cache.get((ino, footer.start, footer.end)).is_none() {
            match fetch_range(&client, &stream_url, footer.start, footer.end).await {
                Ok(data) => cache.put((ino, footer.start, footer.end), data),
                Err(e) => {
                    tracing::debug!(target: "streaming", ino, error = %e, "pre-warm footer failed")
                }
            }
        }
    };

    tokio::join!(fetch_header, fetch_footer);
}

impl Filesystem for RivenFs {
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
            let fd = s.state.open(OpenedFile::Usenet {
                path,
                session: UsenetSession::new(
                    source,
                    Arc::from(info_hash.as_str()),
                    file_index,
                    file_size,
                    filename,
                ),
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

        s.runtime.spawn(prewarm_header_footer(
            Arc::clone(&s.range_cache),
            s.stream_client.clone(),
            Arc::clone(&s.prewarm_semaphore),
            ino,
            stream_url.clone(),
            file_size,
        ));

        let fd = s.state.open(OpenedFile::Media {
            entry_id: entry.id,
            path,
            stream_url: Arc::from(stream_url),
            download_url: entry.download_url.clone(),
            provider: entry.provider.clone(),
            stream_session: MediaStream::new(ino, file_size),
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
        // Hand the read off to a tokio blocking task so the FUSE dispatcher
        // thread returns immediately and is free to service the next kernel
        // request. The body below does `runtime.block_on(fetch_range(...))`
        // synchronously and can take seconds (NNTP fetch, dead-link refresh,
        // retry); running it on the FUSE thread head-of-line blocks every
        // other op on the mount, which is what wedged playback whenever
        // Plex's analyzer fan-out hammered a file. Admission to tokio's
        // blocking-task pool is bounded so a burst of kernel reads cannot
        // create hundreds of blocked network workers. Per-handle
        // serialisation is still preserved by the `Mutex<OpenedFile>` inside
        // `file_handles`.
        let inner = Arc::clone(&self.inner);
        let fh = fh.0;
        let runtime = inner.runtime.clone();
        let read_semaphore = Arc::clone(&inner.read_semaphore);
        runtime.clone().spawn(async move {
            let Ok(permit) = read_semaphore.acquire_owned().await else {
                reply.error(Errno::EIO);
                return;
            };
            if let Err(error) = runtime.spawn_blocking(move || {
                let _permit = permit;
                let s = inner.as_ref();
                let Some(entry) = s.state.file_handles.get(&fh) else {
                    reply.error(Errno::EBADF);
                    return;
                };
                let mut handle = entry.lock();

            if let OpenedFile::Subtitle { content } = &*handle {
                let len = content.len() as u64;
                let start = offset;
                if start >= len {
                    reply.data(&[]);
                    return;
                }
                let end = (start + size as u64 - 1).min(len - 1);
                reply.data(&content[start as usize..=end as usize]);
                return;
            }

            if let OpenedFile::Usenet { path, session } = &mut *handle {
                let start = offset;
                if start >= session.file_size() {
                    reply.data(&[]);
                    return;
                }
                let end = (start + size as u64 - 1).min(session.file_size() - 1);
                tracing::debug!(target: "streaming", path = %path, offset = start, size, "usenet read");
                match session.read(start, end, &s.runtime) {
                    ReadOutcome::Data(buf) => reply.data(&buf),
                    ReadOutcome::Error(code) => reply.error(Errno::from_i32(code)),
                }
                return;
            }

            let OpenedFile::Media {
                stream_session,
                path,
                stream_url,
                download_url,
                provider,
                entry_id,
            } = &*handle
            else {
                tracing::error!(fh, "non-media handle reached the media read path");
                return reply.error(Errno::EIO);
            };
            let (file_size, path, stream_url, download_url, provider, entry_id) = (
                stream_session.file_size(),
                Arc::clone(path),
                Arc::clone(stream_url),
                download_url.as_ref().map(Arc::clone),
                provider.as_ref().map(Arc::clone),
                *entry_id,
            );

            let start = offset;
            if start >= file_size {
                reply.data(&[]);
                return;
            }
            let end = (start + size as u64 - 1).min(file_size - 1);

            tracing::debug!(target: "streaming", path = %path, offset = start, size, "read");

            let outcome = match s.read_handle(&mut handle, start, end, &stream_url) {
                ReadOutcome::Data(buf) => ReadOutcome::Data(buf),
                ReadOutcome::Error(code) => {
                    let Some(download_url) = download_url else {
                        return reply.error(Errno::from_i32(code));
                    };

                    tracing::warn!(
                        path = %path,
                        offset = start,
                        size,
                        code,
                        "read failed, refreshing stream url and retrying once"
                    );

                    match s.request_and_persist_stream_url(
                        entry_id,
                        Some(download_url.as_ref()),
                        provider.as_deref(),
                        Some(stream_url.as_ref()),
                    ) {
                        Some(url) => {
                            let OpenedFile::Media { stream_url, .. } = &mut *handle else {
                                tracing::error!(fh, "non-media handle reached stream-url refresh");
                                return reply.error(Errno::EIO);
                            };
                            *stream_url = Arc::from(url);
                            let refreshed = Arc::clone(stream_url);
                            s.read_handle(&mut handle, start, end, &refreshed)
                        }
                        None => match s.rebind_stale_handle(&mut handle, &path, entry_id) {
                            Some(rebound) => s.read_handle(&mut handle, start, end, &rebound),
                            None => ReadOutcome::Error(code),
                        },
                    }
                }
            };

                match outcome {
                    ReadOutcome::Data(buf) => reply.data(&buf),
                    ReadOutcome::Error(code) => reply.error(Errno::from_i32(code)),
                }
            }).await {
                tracing::error!(%error, fh, "FUSE read worker failed");
            }
        });
    }

    // The VFS is read-only, so there is nothing to flush — but the kernel
    // issues `flush` on every `close()`, and the fuser default handler
    // answers `ENOSYS` and logs a "[Not Implemented]" warning each time.
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
}
