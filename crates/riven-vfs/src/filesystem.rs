use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    Request,
};
use lru::LruCache;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use riven_core::config::vfs::*;
use riven_db::repo;

use crate::chunks::{calculate_file_chunks, FileChunks};
use crate::detect::{detect_read_type, ReadType};
use crate::path_info::{parse_path, PathType};
use crate::prefetch::Prefetch;
use crate::stream::create_stream_request;
use crate::LinkRequest;

const TTL: Duration = Duration::from_secs(300);
const ROOT_INO: u64 = 1;
const MOVIES_INO: u64 = 2;
const SHOWS_INO: u64 = 3;
const FIRST_DYNAMIC_INO: u64 = 100;

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
    // Directories use a fixed epoch — they never change from Jellyfin's perspective.
    make_attr(ino, FileType::Directory, 0, UNIX_EPOCH)
}

fn file_attr(ino: u64, size: u64, mtime: SystemTime) -> FileAttr {
    make_attr(ino, FileType::RegularFile, size, mtime)
}

fn entry_mtime(entry: &riven_db::entities::FileSystemEntry) -> SystemTime {
    // Use the entry's creation time as a stable mtime so Jellyfin doesn't
    // re-probe files on every scan.  Falls back to UNIX_EPOCH if conversion fails.
    let ts = entry.updated_at.unwrap_or(entry.created_at);
    UNIX_EPOCH + std::time::Duration::from_secs(ts.timestamp().max(0) as u64)
}

/// Metadata for an open file handle.
struct FileHandle {
    path: String,
    stream_url: String,
    file_size: u64,
    chunks: FileChunks,
    previous_read_pos: Option<u64>,
    has_scanned_footer: bool,
    /// Background prefetch task + buffer for sequential body playback.
    /// Replaced (task aborted automatically) whenever a seek is detected.
    prefetch: Option<Prefetch>,
}

pub struct RivenFs {
    db_pool: sqlx::PgPool,
    http_client: reqwest::Client,
    link_request_tx: mpsc::Sender<LinkRequest>,
    debug_logging: bool,
    runtime: tokio::runtime::Handle,

    // File handle tracking
    next_fd: AtomicU64,
    file_handles: DashMap<u64, FileHandle>,

    // Inode mapping: path -> inode
    path_to_ino: DashMap<String, u64>,
    ino_to_path: DashMap<u64, String>,
    next_ino: AtomicU64,

    // Chunk cache: "filename:start-end" -> bytes
    chunk_cache: Mutex<LruCache<String, Vec<u8>>>,
}

impl RivenFs {
    pub fn new(
        db_pool: sqlx::PgPool,
        http_client: reqwest::Client,
        link_request_tx: mpsc::Sender<LinkRequest>,
        debug_logging: bool,
        cache_max_size_mb: u64,
    ) -> Self {
        // Each LRU entry holds one CHUNK_SIZE (1 MB) block.
        // Default to 1 024 entries (1 GB) when the setting is 0.
        let mb = if cache_max_size_mb == 0 { 1024 } else { cache_max_size_mb };
        let max_cache_entries = std::num::NonZeroUsize::new(mb as usize).unwrap();

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
            chunk_cache: Mutex::new(LruCache::new(max_cache_entries)),
        }
    }

    fn get_or_create_ino(&self, path: &str) -> u64 {
        if let Some(ino) = self.path_to_ino.get(path) {
            return *ino;
        }
        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        self.path_to_ino.insert(path.to_string(), ino);
        self.ino_to_path.insert(ino, path.to_string());
        ino
    }

    fn resolve_path(&self, parent_ino: u64, name: &str) -> String {
        let parent_path = match parent_ino {
            ROOT_INO => "/".to_string(),
            MOVIES_INO => "/movies".to_string(),
            SHOWS_INO => "/shows".to_string(),
            _ => self
                .ino_to_path
                .get(&parent_ino)
                .map(|p| p.clone())
                .unwrap_or_else(|| "/".to_string()),
        };

        if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        }
    }
}

impl Filesystem for RivenFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy();
        let path = self.resolve_path(parent, &name);

        if self.debug_logging {
            tracing::debug!(path = %path, "lookup");
        }

        let info = parse_path(&path);
        match info.path_type {
            PathType::Root => reply.entry(&TTL, &dir_attr(ROOT_INO), 0),
            PathType::AllMovies => reply.entry(&TTL, &dir_attr(MOVIES_INO), 0),
            PathType::AllShows => reply.entry(&TTL, &dir_attr(SHOWS_INO), 0),
            PathType::MovieDir { .. }
            | PathType::ShowDir { .. }
            | PathType::SeasonDir { .. } => {
                let ino = self.get_or_create_ino(&path);
                reply.entry(&TTL, &dir_attr(ino), 0);
            }
            PathType::MovieFile { .. } | PathType::EpisodeFile { .. } => {
                // Look up file size from DB
                let pool = self.db_pool.clone();
                let path_clone = path.clone();
                match self.runtime.block_on(async {
                    repo::get_media_entry_by_path(&pool, &path_clone).await
                }) {
                    Ok(Some(entry)) => {
                        let ino = self.get_or_create_ino(&path);
                        let mtime = entry_mtime(&entry);
                        reply.entry(&TTL, &file_attr(ino, entry.file_size as u64, mtime), 0);
                    }
                    _ => {
                        reply.error(libc::ENOENT);
                    }
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
                if let Some(path) = self.ino_to_path.get(&ino) {
                    let info = parse_path(&path);
                    match info.path_type {
                        PathType::MovieDir { .. }
                        | PathType::ShowDir { .. }
                        | PathType::SeasonDir { .. } => {
                            reply.attr(&TTL, &dir_attr(ino));
                        }
                        PathType::MovieFile { .. } | PathType::EpisodeFile { .. } => {
                            let pool = self.db_pool.clone();
                            let p = path.clone();
                            match self.runtime.block_on(async {
                                repo::get_media_entry_by_path(&pool, &p).await
                            }) {
                                Ok(Some(entry)) => {
                                    let mtime = entry_mtime(&entry);
                                    reply.attr(&TTL, &file_attr(ino, entry.file_size as u64, mtime));
                                }
                                _ => reply.error(libc::ENOENT),
                            }
                        }
                        _ => reply.attr(&TTL, &dir_attr(ino)),
                    }
                } else {
                    reply.error(libc::ENOENT);
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
        tracing::debug!(ino = ino, offset = offset, "readdir");
        let mut entries: Vec<(u64, FileType, String)> = vec![
            (ino, FileType::Directory, ".".into()),
            (ino, FileType::Directory, "..".into()),
        ];

        let pool = self.db_pool.clone();

        match ino {
            ROOT_INO => {
                entries.push((MOVIES_INO, FileType::Directory, "movies".into()));
                entries.push((SHOWS_INO, FileType::Directory, "shows".into()));
            }
            MOVIES_INO => {
                if let Ok(movies) = self.runtime.block_on(repo::list_movies(&pool)) {
                    for movie in movies {
                        let name = movie.pretty_name();
                        let child_ino =
                            self.get_or_create_ino(&format!("/movies/{name}"));
                        entries.push((child_ino, FileType::Directory, name));
                    }
                }
            }
            SHOWS_INO => {
                if let Ok(shows) = self.runtime.block_on(repo::list_shows(&pool)) {
                    for show in shows {
                        let name = show.pretty_name();
                        let child_ino =
                            self.get_or_create_ino(&format!("/shows/{name}"));
                        entries.push((child_ino, FileType::Directory, name));
                    }
                }
            }
            _ => {
                if let Some(path) = self.ino_to_path.get(&ino).map(|p| p.clone()) {
                    let info = parse_path(&path);
                    match info.path_type {
                        PathType::MovieDir { ref tmdb_id, .. } => {
                            if let Some(tmdb_id) = tmdb_id {
                                if let Ok(Some(movie)) = self.runtime.block_on(
                                    repo::get_media_item_by_tmdb(&pool, tmdb_id),
                                ) {
                                    if let Ok(fse) = self.runtime.block_on(
                                        repo::get_media_entries(&pool, movie.id),
                                    ) {
                                        for entry in fse {
                                            let fname = entry.vfs_filename(&movie.pretty_name());
                                            let file_path =
                                                format!("{path}/{fname}");
                                            let child_ino =
                                                self.get_or_create_ino(&file_path);
                                            entries.push((
                                                child_ino,
                                                FileType::RegularFile,
                                                fname,
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        PathType::ShowDir { ref tvdb_id, .. } => {
                            if let Some(tvdb_id) = tvdb_id {
                                if let Ok(Some(show)) = self.runtime.block_on(
                                    repo::get_media_item_by_tvdb(&pool, tvdb_id),
                                ) {
                                    if let Ok(seasons) = self
                                        .runtime
                                        .block_on(repo::list_seasons(&pool, show.id))
                                    {
                                        for season in seasons {
                                            let num = season.season_number.unwrap_or(0);
                                            let name = format!("Season {num:02}");
                                            let season_path = format!("{path}/{name}");
                                            let child_ino =
                                                self.get_or_create_ino(&season_path);
                                            entries.push((
                                                child_ino,
                                                FileType::Directory,
                                                name,
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        PathType::SeasonDir {
                            ref tvdb_id,
                            season_number,
                            ..
                        } => {
                            if let Some(tvdb_id) = tvdb_id {
                                if let Ok(Some(show)) = self.runtime.block_on(
                                    repo::get_media_item_by_tvdb(&pool, tvdb_id),
                                ) {
                                    if let Ok(seasons) = self
                                        .runtime
                                        .block_on(repo::list_seasons(&pool, show.id))
                                    {
                                        if let Some(season) = seasons
                                            .iter()
                                            .find(|s| s.season_number == Some(season_number))
                                        {
                                            if let Ok(episodes) = self.runtime.block_on(
                                                repo::list_episodes(&pool, season.id),
                                            ) {
                                                for ep in episodes {
                                                    let ep_num = ep.episode_number.unwrap_or(0);
                                                    let fname = format!(
                                                        "{} - s{:02}e{:02}.mkv",
                                                        show.pretty_name(),
                                                        season_number,
                                                        ep_num
                                                    );
                                                    let ep_path =
                                                        format!("{path}/{fname}");
                                                    let child_ino =
                                                        self.get_or_create_ino(&ep_path);
                                                    entries.push((
                                                        child_ino,
                                                        FileType::RegularFile,
                                                        fname,
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        tracing::debug!(ino = ino, count = entries.len(), "readdir returning entries");
        // Deduplicate entries by name (keep first occurrence)
        let mut seen = HashSet::new();
        let deduped: Vec<_> = entries
            .into_iter()
            .filter(|(_, _, name)| seen.insert(name.clone()))
            .collect();

        for (i, (ino, kind, name)) in deduped.iter().enumerate().skip(offset as usize) {
            if reply.add(*ino, (i + 1) as i64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        let path = match self.ino_to_path.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if self.debug_logging {
            tracing::debug!(path = %path, "open");
        }

        let pool = self.db_pool.clone();
        let entry = match self
            .runtime
            .block_on(repo::get_media_entry_by_path(&pool, &path))
        {
            Ok(Some(e)) => e,
            _ => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Always generate a fresh stream URL via the plugin system when a download_url
        // is available (mirrors riven-ts behaviour — cached stream URLs expire).
        // Fall back to the cached stream_url only if no download_url is stored.
        let stream_url = if let Some(ref download_url) = entry.download_url {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let link_req = LinkRequest {
                download_url: download_url.clone(),
                response_tx: tx,
            };
            if self.link_request_tx.blocking_send(link_req).is_err() {
                reply.error(libc::EIO);
                return;
            }
            match self.runtime.block_on(rx) {
                Ok(Some(url)) => {
                    let _ = self
                        .runtime
                        .block_on(repo::update_stream_url(&pool, entry.id, &url));
                    url
                }
                _ => {
                    reply.error(libc::EIO);
                    return;
                }
            }
        } else if let Some(ref url) = entry.stream_url {
            url.clone()
        } else {
            reply.error(libc::ENOENT);
            return;
        };

        let file_size = entry.file_size as u64;
        let chunks = calculate_file_chunks(file_size);
        let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);

        self.file_handles.insert(
            fd,
            FileHandle {
                path,
                stream_url,
                file_size,
                chunks,
                previous_read_pos: None,
                has_scanned_footer: false,
                prefetch: None,
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
        let start = offset as u64;
        let end = start + size as u64 - 1;

        let mut handle = match self.file_handles.get_mut(&fh) {
            Some(h) => h,
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        if start >= handle.file_size {
            reply.data(&[]);
            return;
        }

        let end = end.min(handle.file_size - 1);
        let needed_chunks = handle.chunks.chunks_for_range(start, end);

        // Check cache for all chunks
        let all_cached = {
            let cache = self.chunk_cache.lock();
            needed_chunks
                .iter()
                .all(|c| cache.contains(&c.cache_key(&handle.path)))
        };

        let read_type = detect_read_type(
            start,
            size as u64,
            handle.previous_read_pos,
            handle.file_size,
            handle.chunks.footer_start,
            all_cached,
            handle.has_scanned_footer,
        );

        if self.debug_logging {
            tracing::debug!(
                path = %handle.path,
                offset = start,
                size = size,
                read_type = ?read_type,
                "read"
            );
        }

        handle.previous_read_pos = Some(start);

        match read_type {
            ReadType::CacheHit => {
                let mut buf = Vec::with_capacity(size as usize);
                let mut cache = self.chunk_cache.lock();
                for chunk in &needed_chunks {
                    if let Some(data) = cache.get(&chunk.cache_key(&handle.path)) {
                        let chunk_offset = start.saturating_sub(chunk.start) as usize;
                        let chunk_end =
                            ((end - chunk.start + 1) as usize).min(data.len());
                        if chunk_offset < data.len() {
                            buf.extend_from_slice(
                                &data[chunk_offset..chunk_end.min(data.len())],
                            );
                        }
                    }
                }
                reply.data(&buf);
            }

            ReadType::HeaderScan => {
                let url = handle.stream_url.clone();
                let client = self.http_client.clone();
                let path = handle.path.clone();

                let fetch_start = needed_chunks.first().map(|c| c.start).unwrap_or(start);
                let fetch_end = needed_chunks.last().map(|c| c.end).unwrap_or(end);

                match self.runtime.block_on(async {
                    let resp =
                        create_stream_request(&client, &url, fetch_start, Some(fetch_end))
                            .await?;
                    let bytes = resp.bytes().await?;
                    Ok::<_, anyhow::Error>(bytes.to_vec())
                }) {
                    Ok(data) => {
                        for chunk in &needed_chunks {
                            let chunk_start_in_data =
                                (chunk.start - fetch_start) as usize;
                            let chunk_end_in_data =
                                ((chunk.end - fetch_start + 1) as usize).min(data.len());
                            if chunk_start_in_data < data.len() {
                                let chunk_data =
                                    data[chunk_start_in_data..chunk_end_in_data].to_vec();
                                let mut cache = self.chunk_cache.lock();
                                cache.put(chunk.cache_key(&path), chunk_data);
                            }
                        }
                        let ret_start = (start - fetch_start) as usize;
                        let ret_end = ret_start + (end - start + 1) as usize;
                        reply.data(&data[ret_start..ret_end.min(data.len())]);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "stream read failed");
                        reply.error(libc::EIO);
                    }
                }
            }

            ReadType::FooterScan | ReadType::GeneralScan => {
                // Random / probe reads (ffprobe seeking, player seeking).
                // Abort any running prefetch — the next BodyRead will restart it
                // at the correct sequential position.
                handle.prefetch = None;

                let url = handle.stream_url.clone();
                let client = self.http_client.clone();
                let path = handle.path.clone();

                if read_type == ReadType::FooterScan {
                    handle.has_scanned_footer = true;
                }

                let exact_key = format!("{path}:{start}-{end}");
                let cached = {
                    let mut cache = self.chunk_cache.lock();
                    cache.get(&exact_key).cloned()
                };

                if let Some(data) = cached {
                    reply.data(&data);
                } else {
                    match self.runtime.block_on(async {
                        let resp = create_stream_request(&client, &url, start, Some(end)).await?;
                        let bytes = resp.bytes().await?;
                        Ok::<_, anyhow::Error>(bytes.to_vec())
                    }) {
                        Ok(data) => {
                            let mut cache = self.chunk_cache.lock();
                            cache.put(exact_key, data.clone());
                            reply.data(&data);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "stream read failed");
                            reply.error(libc::EIO);
                        }
                    }
                }
            }

            ReadType::BodyRead | ReadType::FooterRead => {
                let bytes_needed = (end - start + 1) as usize;
                let url = handle.stream_url.clone();
                let client = self.http_client.clone();

                // (Re)start the prefetch task if we don't have one or it can no
                // longer serve this position (e.g. after a user-initiated seek).
                let need_restart = handle
                    .prefetch
                    .as_ref()
                    .map(|p| !p.is_valid_for(start))
                    .unwrap_or(true);

                if need_restart {
                    if self.debug_logging {
                        tracing::debug!(position = start, "starting prefetch task");
                    }
                    handle.prefetch =
                        Some(Prefetch::start(client, url, start, &self.runtime));
                }

                // The prefetch task is racing ahead on the HTTP stream. Reading
                // from it is usually instant once the buffer is primed.
                let prefetch = handle.prefetch.as_mut().unwrap();
                match prefetch.read(start, bytes_needed, &self.runtime) {
                    Ok(data) => reply.data(&data),
                    Err(e) => {
                        tracing::error!(error = %e, "prefetch read failed");
                        handle.prefetch = None; // force reconnect on next read
                        reply.error(libc::EIO);
                    }
                }
            }
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
            tracing::debug!(fh = fh, "release");
        }
        self.file_handles.remove(&fh);
        reply.ok();
    }
}
