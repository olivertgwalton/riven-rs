use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Per-torrent data returned by a scrape plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapeEntry {
    pub title: String,
    /// Total torrent size in bytes, if known at scrape time.
    pub file_size_bytes: Option<u64>,
}

impl ScrapeEntry {
    pub fn new(title: impl Into<String>) -> Self {
        Self { title: title.into(), file_size_bytes: None }
    }

    pub fn with_size(title: impl Into<String>, size: u64) -> Self {
        Self { title: title.into(), file_size_bytes: Some(size) }
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
}
