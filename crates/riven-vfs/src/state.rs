use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::Mutex;
use riven_core::settings::LibraryProfileMembership;
use riven_core::types::FileSystemEntryType;
use riven_db::entities::FileSystemEntry;

use crate::media_stream::{MediaStream, UsenetSession};
use crate::readdir::DirEntry;

pub(crate) const ROOT_INO: u64 = 1;
pub(crate) const MOVIES_INO: u64 = 2;
pub(crate) const SHOWS_INO: u64 = 3;
const FIRST_DYNAMIC_INO: u64 = 100;
const READDIR_CACHE_TTL: Duration = Duration::from_secs(30);

pub(crate) enum OpenedFile {
    Media {
        entry_id: i64,
        path: Arc<str>,
        stream_url: Arc<str>,
        download_url: Option<Arc<str>>,
        provider: Option<Arc<str>>,
        stream_session: MediaStream,
    },
    Usenet {
        path: Arc<str>,
        session: UsenetSession,
    },
    Subtitle {
        content: Arc<[u8]>,
    },
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

pub(crate) struct VfsState {
    revision: AtomicU64,
    pub file_handles: DashMap<u64, Mutex<OpenedFile>>,
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
        self.revision.store(revision, Ordering::SeqCst);
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
