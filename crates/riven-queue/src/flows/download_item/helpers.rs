use std::collections::BTreeMap;

use riven_core::events::RivenEvent;
use riven_core::types::DownloadFile;
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::JobQueue;

/// Log a bitrate failure, blacklist the stream, and send a PartialSuccess event.
pub async fn handle_bitrate_failure(
    id: i64,
    info_hash: &str,
    file_size: u64,
    runtime: Option<i32>,
    context: &str,
    queue: &JobQueue,
) {
    tracing::warn!(
        id,
        file_size,
        runtime = ?runtime,
        info_hash = %info_hash,
        "{context} failed bitrate check — blacklisting stream"
    );
    if !info_hash.is_empty() {
        let _ = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await;
        let _ = repo::update_stream_file_size(&queue.db_pool, info_hash, file_size).await;
    }
    queue
        .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
        .await;
    queue.fan_out_download(id).await;
}

/// Load a media item by id, or send a `MediaItemDownloadError` event and return `None`.
pub async fn load_item_or_err(
    id: i64,
    queue: &JobQueue,
    error_msg: &str,
) -> Option<MediaItem> {
    match repo::get_media_item(&queue.db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => {
            tracing::error!(id, "{error_msg}");
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: String::new(),
                    error: error_msg.into(),
                })
                .await;
            None
        }
        Err(e) => {
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: String::new(),
                    error: e.to_string(),
                })
                .await;
            None
        }
    }
}

/// Parse a file path by merging metadata from all path segments.
pub fn parse_file_path(path: &str) -> riven_rank::ParsedData {
    let mut merged = riven_rank::ParsedData::default();
    for segment in path.split('/').filter(|s| !s.is_empty()) {
        merged.merge(riven_rank::parse(segment));
    }
    merged
}

/// Returns true if a parsed file covers the given season/episode.
pub fn matches_episode(
    parsed: &riven_rank::ParsedData,
    season: i32,
    ep: i32,
    abs: Option<i32>,
) -> bool {
    parsed.seasons.contains(&season)
        && (parsed.episodes.contains(&ep)
            || abs.map_or(false, |a| parsed.episodes.contains(&a)))
}

/// Build the VFS path for an episode file.
/// Appends `.ptN` before the extension when `part` is `Some`.
pub fn episode_vfs_path(show: &str, season: i32, ep: i32, part: Option<i32>) -> String {
    let suffix = part.map(|n| format!(".pt{n}")).unwrap_or_default();
    format!("/shows/{show}/Season {season:02}/{show} - s{season:02}e{ep:02}{suffix}.mkv")
}

/// Choose which files to persist for an episode.
///
/// - If any matched file has a `part` number, return one entry per distinct part
///   (largest file wins per part).
/// - Otherwise return the single largest file.
pub fn select_episode_files<'a>(
    matched: &[(&'a DownloadFile, riven_rank::ParsedData)],
) -> Vec<(&'a DownloadFile, Option<i32>)> {
    let mut by_part: BTreeMap<i32, &'a DownloadFile> = BTreeMap::new();
    let mut largest: Option<&'a DownloadFile> = None;

    for (file, parsed) in matched {
        if let Some(n) = parsed.part {
            let entry = by_part.entry(n).or_insert(file);
            if file.file_size > entry.file_size {
                *entry = file;
            }
        } else if largest.map_or(true, |f| file.file_size > f.file_size) {
            largest = Some(file);
        }
    }

    if !by_part.is_empty() {
        by_part.into_iter().map(|(n, f)| (f, Some(n))).collect()
    } else {
        largest.map(|f| vec![(f, None)]).unwrap_or_default()
    }
}
