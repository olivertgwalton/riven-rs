use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Maps provider stream id -> scrape metadata.
///
/// Torrent scrapers use the info hash as the key and only set `title`.
/// Non-torrent scrapers can provide a stable synthetic id plus a provider-owned
/// `magnet` payload that is persisted with the stream and handed back to the
/// same plugin during download/stream-link requests.
pub type ScrapeResponse = HashMap<String, ScrapeStream>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScrapeStream {
    Title(String),
    Details(ScrapeStreamDetails),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapeStreamDetails {
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub magnet: Option<String>,
}

impl ScrapeStream {
    pub fn new(title: impl Into<String>) -> Self {
        Self::Title(title.into())
    }

    pub fn with_magnet(title: impl Into<String>, magnet: impl Into<String>) -> Self {
        Self::Details(ScrapeStreamDetails {
            title: title.into(),
            magnet: Some(magnet.into()),
        })
    }

    pub fn title(&self) -> &str {
        match self {
            Self::Title(title) => title,
            Self::Details(details) => &details.title,
        }
    }

    pub fn magnet(&self) -> Option<&str> {
        match self {
            Self::Title(_) => None,
            Self::Details(details) => details.magnet.as_deref(),
        }
    }
}

impl From<String> for ScrapeStream {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for ScrapeStream {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

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
