use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::Mutex;
use riven_core::settings::LibraryProfileMembership;
use riven_core::types::FileSystemEntryType;
use riven_db::entities::FileSystemEntry;

use crate::prefetch::{Prefetcher, UNIT_CACHE_BYTES, UnitCache};
use crate::readdir::DirEntry;

pub(crate) const ROOT_INO: u64 = 1;
pub(crate) const MOVIES_INO: u64 = 2;
pub(crate) const SHOWS_INO: u64 = 3;
const FIRST_DYNAMIC_INO: u64 = 100;
const READDIR_CACHE_TTL: Duration = Duration::from_secs(30);
/// How long a file's decoded read-ahead survives with no handle open on it.
///
/// Players re-open the same file every couple of seconds, and a cache that
/// died with its handle threw away a full read-ahead window every time — which
/// is what made playback stall. streamnzb keeps the equivalent state on its
/// `File` for the life of the session rather than on the per-request
/// `SegmentReader`; this is the same idea with an idle bound so a finished
/// stream is not held forever.
const STREAM_IDLE_TTL: Duration = Duration::from_secs(60);

pub(crate) enum OpenedFile {
    /// Any network-backed file. The origin (usenet or HTTP) is behind
    /// `ByteSource`, and read-ahead is the same for both — so there is one
    /// variant here rather than one per backend.
    Streamed {
        path: Arc<str>,
        prefetcher: Arc<Prefetcher>,
    },
    /// Subtitles are small and already resident; no streaming machinery.
    Subtitle { content: Arc<[u8]> },
}

pub(crate) struct CachedEntry {
    pub id: i64,
    pub entry_type: FileSystemEntryType,
    pub file_size: u64,
    pub mtime: SystemTime,
    pub download_url: Option<Arc<str>>,
    pub stream_url: Option<Arc<str>>,
    pub provider: Option<Arc<str>>,
    pub subtitle_content: Option<Arc<[u8]>>,
    library_profiles: LibraryProfileMembership,
    pub usenet_info_hash: Option<Arc<str>>,
    pub usenet_file_index: Option<usize>,
}

impl CachedEntry {
    pub(crate) fn from_db(entry: FileSystemEntry) -> Self {
        let subtitle_content = match entry.entry_type {
            FileSystemEntryType::Subtitle => entry
                .subtitle_content
                .as_deref()
                .map(|content| Arc::<[u8]>::from(content.as_bytes())),
            FileSystemEntryType::Media => None,
        };
        let file_size = match (&entry.entry_type, &subtitle_content) {
            (FileSystemEntryType::Subtitle, Some(content)) => content.len() as u64,
            _ => u64::try_from(entry.file_size).unwrap_or(0),
        };
        let mtime =
            UNIX_EPOCH + Duration::from_secs(entry.created_at.timestamp().max(0).cast_unsigned());
        Self {
            id: entry.id,
            entry_type: entry.entry_type,
            file_size,
            mtime,
            download_url: entry.download_url.map(Arc::from),
            stream_url: entry.stream_url.map(Arc::from),
            provider: entry.provider.map(Arc::from),
            subtitle_content,
            library_profiles: LibraryProfileMembership::from_json(entry.library_profiles.as_ref()),
            usenet_info_hash: entry.usenet_info_hash.map(Arc::from),
            usenet_file_index: entry
                .usenet_file_index
                .and_then(|index| usize::try_from(index).ok()),
        }
    }

    pub(crate) fn matches_profile(&self, profile_key: Option<&str>) -> bool {
        profile_key.is_none_or(|key| self.library_profiles.contains(key))
    }
}

/// One file's decoded read-ahead, shared by every handle open on it and kept
/// warm across the gaps between them. Each handle still keeps its own cursor
/// and window — only the bytes are shared.
struct Stream {
    cache: Arc<UnitCache>,
    last_used: Mutex<Instant>,
}

pub(crate) struct VfsState {
    revision: AtomicU64,
    pub file_handles: DashMap<u64, Mutex<OpenedFile>>,
    /// Keyed by inode, not by handle: see [`STREAM_IDLE_TTL`].
    streams: DashMap<u64, Stream>,
    path_to_ino: DashMap<Arc<str>, u64>,
    ino_to_path: DashMap<u64, Arc<str>>,
    next_ino: AtomicU64,
    next_fd: AtomicU64,
    readdir_cache: DashMap<u64, (Vec<DirEntry>, Instant)>,
}

impl VfsState {
    pub(crate) fn new() -> Self {
        let state = Self {
            revision: AtomicU64::new(0),
            file_handles: DashMap::new(),
            streams: DashMap::new(),
            path_to_ino: DashMap::new(),
            ino_to_path: DashMap::new(),
            next_ino: AtomicU64::new(FIRST_DYNAMIC_INO),
            next_fd: AtomicU64::new(1),
            readdir_cache: DashMap::new(),
        };
        state.register_static_path("/movies", MOVIES_INO);
        state.register_static_path("/shows", SHOWS_INO);
        state
    }

    fn register_static_path(&self, path: &str, ino: u64) {
        let path: Arc<str> = Arc::from(path);
        self.path_to_ino.insert(Arc::clone(&path), ino);
        self.ino_to_path.insert(ino, path);
    }

    pub(crate) fn refresh(&self, revision: u64) {
        if self.revision.load(Ordering::SeqCst) == revision {
            return;
        }
        self.readdir_cache.clear();
        // A settings change can repoint a path at different bytes, so a warm
        // read-ahead for it is no longer trustworthy.
        self.streams.clear();
        self.revision.store(revision, Ordering::SeqCst);
    }

    /// The decoded read-ahead cache for `ino`, creating it on first use.
    ///
    /// Handles come and go every couple of seconds; the bytes they fetched
    /// should not. A re-open builds a fresh window but finds the units around
    /// the play position already decoded, instead of starting cold.
    pub(crate) fn unit_cache(&self, ino: u64) -> Arc<UnitCache> {
        self.evict_idle_streams();
        let stream = self.streams.entry(ino).or_insert_with(|| Stream {
            cache: UnitCache::new(UNIT_CACHE_BYTES),
            last_used: Mutex::new(Instant::now()),
        });
        *stream.last_used.lock() = Instant::now();
        Arc::clone(&stream.cache)
    }

    /// Drop caches that no handle has touched for [`STREAM_IDLE_TTL`]. Called
    /// from `unit_cache`, so a process that stops streaming stops paying.
    fn evict_idle_streams(&self) {
        self.streams.retain(|_ino, stream| {
            Arc::strong_count(&stream.cache) > 1
                || stream.last_used.lock().elapsed() < STREAM_IDLE_TTL
        });
    }

    pub(crate) fn get_or_create_ino(&self, path: &str) -> u64 {
        if let Some(ino) = self.path_to_ino.get(path) {
            return *ino;
        }
        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        let path: Arc<str> = Arc::from(path);
        self.path_to_ino.insert(Arc::clone(&path), ino);
        self.ino_to_path.insert(ino, path);
        ino
    }

    pub(crate) fn path(&self, ino: u64) -> Option<Arc<str>> {
        self.ino_to_path.get(&ino).map(|path| Arc::clone(&path))
    }

    pub(crate) fn resolve_path(&self, parent_ino: u64, name: &str) -> Arc<str> {
        let parent = match parent_ino {
            ROOT_INO => Arc::<str>::from("/"),
            MOVIES_INO => Arc::<str>::from("/movies"),
            SHOWS_INO => Arc::<str>::from("/shows"),
            _ => self
                .path(parent_ino)
                .unwrap_or_else(|| Arc::<str>::from("/")),
        };
        Arc::from(if parent.as_ref() == "/" {
            format!("/{name}")
        } else {
            format!("{parent}/{name}")
        })
    }

    pub(crate) fn directory_entries(&self, ino: u64) -> Option<Vec<DirEntry>> {
        self.readdir_cache
            .get(&ino)
            .and_then(|entry| (entry.1.elapsed() < READDIR_CACHE_TTL).then(|| entry.0.clone()))
    }

    pub(crate) fn cache_directory_entries(&self, ino: u64, entries: Vec<DirEntry>) {
        self.readdir_cache.insert(ino, (entries, Instant::now()));
    }

    pub(crate) fn open(&self, file: OpenedFile) -> u64 {
        let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);
        self.file_handles.insert(fd, Mutex::new(file));
        fd
    }

    pub(crate) fn close(&self, fd: u64) {
        self.file_handles.remove(&fd);
    }
}

#[cfg(test)]
mod tests {
    use fuser::FileType;

    use super::*;

    struct EmptySource;

    #[async_trait::async_trait]
    impl crate::source::ByteSource for EmptySource {
        async fn read_range(&self, _start: u64, _end: u64) -> std::io::Result<bytes::Bytes> {
            Ok(bytes::Bytes::new())
        }
        fn size(&self) -> u64 {
            1 << 20
        }
    }

    fn handle(state: &VfsState, ino: u64) -> u64 {
        let prefetcher = Arc::new(Prefetcher::new(
            Arc::new(EmptySource),
            state.unit_cache(ino),
            &tokio::runtime::Handle::current(),
        ));
        state.open(OpenedFile::Streamed {
            path: state.path(ino).unwrap(),
            prefetcher,
        })
    }

    #[tokio::test]
    async fn a_reopened_file_keeps_the_bytes_its_last_handle_fetched() {
        let state = VfsState::new();
        let ino = state.get_or_create_ino("/movies/Film/film.mkv");

        let first = state.unit_cache(ino);
        let fd = handle(&state, ino);
        // The player closes every handle it has before opening the next one.
        state.close(fd);

        let reopened = state.unit_cache(ino);
        assert!(Arc::ptr_eq(&first, &reopened));
        assert_eq!(state.streams.len(), 1);
    }

    #[tokio::test]
    async fn each_handle_gets_its_own_window_over_the_shared_bytes() {
        let state = VfsState::new();
        let ino = state.get_or_create_ino("/movies/Film/film.mkv");

        // Two handles at once, as a player's overlapping range requests do.
        let one = handle(&state, ino);
        let two = handle(&state, ino);
        assert_ne!(one, two);
        assert_eq!(state.streams.len(), 1, "one shared cache");
        assert_eq!(state.file_handles.len(), 2, "two independent windows");
    }

    #[tokio::test]
    async fn a_settings_change_drops_warm_read_ahead() {
        let state = VfsState::new();
        let ino = state.get_or_create_ino("/movies/Film/film.mkv");
        let cache = state.unit_cache(ino);
        state.refresh(1);
        assert_eq!(state.streams.len(), 0);
        assert!(!Arc::ptr_eq(&cache, &state.unit_cache(ino)));
    }

    #[test]
    fn inode_assignment_is_stable_and_preserves_static_roots() {
        let state = VfsState::new();
        assert_eq!(state.get_or_create_ino("/movies"), MOVIES_INO);
        let first = state.get_or_create_ino("/movies/Film");
        assert_eq!(state.get_or_create_ino("/movies/Film"), first);
        assert_eq!(state.path(first).as_deref(), Some("/movies/Film"));
    }

    #[test]
    fn directory_cache_is_cleared_when_settings_change() {
        let state = VfsState::new();
        state.cache_directory_entries(
            ROOT_INO,
            vec![(MOVIES_INO, FileType::Directory, "movies".to_string())],
        );
        assert!(state.directory_entries(ROOT_INO).is_some());
        state.refresh(1);
        assert!(state.directory_entries(ROOT_INO).is_none());
    }

    #[test]
    fn child_paths_are_resolved_from_inode_state() {
        let state = VfsState::new();
        let parent = state.get_or_create_ino("/movies/Film");
        assert_eq!(
            state.resolve_path(parent, "Film.mkv").as_ref(),
            "/movies/Film/Film.mkv"
        );
    }
}
