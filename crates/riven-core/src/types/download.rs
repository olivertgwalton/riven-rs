use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Maps info_hash -> torrent title
pub type ScrapeResponse = HashMap<String, String>;

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
