use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TorrentStatus {
    Cached,
    Queued,
    Downloading,
    Processing,
    Downloaded,
    Uploading,
    Failed,
    Invalid,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheCheckResult {
    pub hash: String,
    /// Which debrid store this result came from. Set by the plugin after reading
    /// from Redis or the live API; not persisted to Redis itself.
    #[serde(default)]
    pub store: String,
    pub status: TorrentStatus,
    pub files: Vec<CacheCheckFile>,
}

/// Pre-checked availability for one store, ready to pass directly to add_torrent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedStoreEntry {
    pub store: String,
    pub files: Vec<CacheCheckFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheCheckFile {
    pub index: u32,
    pub name: String,
    #[serde(default)]
    pub path: String,
    /// File size in bytes. `None` when the store did not report a size (e.g. returns -1).
    pub size: Option<u64>,
    /// Direct download link for this file, populated from a live cache-check
    /// response when `status == Cached`. Intentionally not serialized so that
    /// expiring links are never written into the Redis 24-hour cache.
    #[serde(skip_serializing, default)]
    pub link: Option<String>,
}
