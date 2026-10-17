use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Per-torrent data returned by a scrape plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapeEntry {
    pub title: String,
    /// Total torrent size in bytes, if known at scrape time.
    pub file_size_bytes: Option<u64>,
    /// The URL the release is actually fetched from, for sources whose
    /// `info_hash` is synthetic and therefore carries no handle back to the
    /// release (NZBs: the hash is a SHA-1 *of* this URL, so it cannot be
    /// reversed). Stored on the stream row so the handle lives exactly as
    /// long as the candidate does; a cache that outlives it is not enough,
    /// since a stream nobody can fetch is indistinguishable from a dead
    /// release and gets blacklisted for good. `None` for torrents, whose
    /// info_hash is the handle.
    pub source_url: Option<String>,
}

impl ScrapeEntry {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            file_size_bytes: None,
            source_url: None,
        }
    }

    pub fn with_size(title: impl Into<String>, size: u64) -> Self {
        Self {
            title: title.into(),
            file_size_bytes: Some(size),
            source_url: None,
        }
    }
}

/// Maps info_hash -> scrape entry (title + optional size)
pub type ScrapeResponse = HashMap<String, ScrapeEntry>;

pub fn build_magnet_uri(info_hash: &str) -> String {
    format!("magnet:?xt=urn:btih:{}", info_hash.to_lowercase())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadResult {
    pub info_hash: String,
    pub files: Vec<DownloadFile>,
    pub provider: Option<String>,
    pub plugin_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadFile {
    pub filename: String,
    pub file_size: u64,
    pub download_url: Option<String>,
    pub stream_url: Option<String>,
    /// Usenet article address, set by the usenet plugin so the VFS can read
    /// the file in-process. `None` for debrid and other providers.
    #[serde(default)]
    pub usenet_info_hash: Option<String>,
    #[serde(default)]
    pub usenet_file_index: Option<i32>,
}
