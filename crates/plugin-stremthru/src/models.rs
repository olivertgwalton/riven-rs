use serde::Deserialize;

use riven_core::types::TorrentStatus;

pub fn parse_torrent_status(status: &str) -> TorrentStatus {
    match status {
        "cached" => TorrentStatus::Cached,
        "queued" => TorrentStatus::Queued,
        "downloading" => TorrentStatus::Downloading,
        "processing" => TorrentStatus::Processing,
        "downloaded" => TorrentStatus::Downloaded,
        "uploading" => TorrentStatus::Uploading,
        "failed" => TorrentStatus::Failed,
        "invalid" => TorrentStatus::Invalid,
        _ => TorrentStatus::Unknown,
    }
}

#[derive(Deserialize)]
pub struct StremthruResponse<T> {
    pub data: Option<T>,
}

#[derive(Deserialize)]
pub struct StremthruCacheCheck {
    pub items: Vec<StremthruCacheItem>,
}

#[derive(Deserialize)]
pub struct StremthruCacheItem {
    pub hash: String,
    pub status: String,
    #[serde(default)]
    pub files: Vec<StremthruCacheFile>,
}

#[derive(Deserialize)]
pub struct StremthruCacheFile {
    pub index: i32,
    pub name: String,
    pub size: u64,
    #[serde(default)]
    pub link: String,
}

#[derive(Deserialize)]
pub struct StremthruTorz {
    pub id: String,
    pub status: String,
    #[serde(default)]
    pub files: Vec<StremthruTorzFile>,
}

#[derive(Deserialize)]
pub struct StremthruTorzFile {
    #[serde(default)]
    pub index: i32,
    pub name: String,
    pub size: u64,
    #[serde(default)]
    pub link: String,
}

#[derive(Deserialize)]
pub struct StremthruLink {
    pub link: String,
}

#[derive(Deserialize)]
pub struct StremthruUser {
    pub email: Option<String>,
    pub subscription_status: Option<String>,
}
