use std::collections::BTreeMap;

use riven_core::events::RivenEvent;
use riven_core::types::{CacheCheckFile, DownloadFile, MediaItemType};
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::JobQueue;
use crate::context::load_media_item_or_download_error;
use crate::orchestrator::LibraryOrchestrator;

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
    LibraryOrchestrator::new(queue)
        .fan_out_download_failure(id)
        .await;
}

/// Load a media item by id, or send a `MediaItemDownloadError` event and return `None`.
pub async fn load_item_or_err(id: i64, queue: &JobQueue, error_msg: &str) -> Option<MediaItem> {
    load_media_item_or_download_error(queue, id, error_msg).await
}

const VALID_VIDEO_EXTENSIONS: &[&str] = &["mp4", "mkv", "avi", "mov", "wmv", "flv", "webm"];

/// Returns true if the filename has a recognised video extension.
/// Matches riven-ts VALID_FILE_EXTENSIONS list.
pub fn is_video_file(filename: &str) -> bool {
    let ext = filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    VALID_VIDEO_EXTENSIONS.contains(&ext.as_str())
}

/// Returns true if the cached file list contains at least one file that matches
/// the media item. Used to skip add_torrent when the torrent clearly won't
/// satisfy the item (matches riven-ts getCachedTorrentFiles pre-validation).
pub fn has_matching_file(files: &[CacheCheckFile], item: &MediaItem) -> bool {
    match item.item_type {
        MediaItemType::Movie => files
            .iter()
            .any(|f| is_video_file(&f.name) && parse_file_path(&f.name).media_type() == "movie"),
        MediaItemType::Episode => {
            let season = item.season_number.unwrap_or(1);
            let ep = item.episode_number.unwrap_or(1);
            files.iter().any(|f| {
                is_video_file(&f.name)
                    && matches_episode(&parse_file_path(&f.name), season, ep, item.absolute_number)
            })
        }
        // For seasons, any video file is sufficient; per-episode validation
        // happens in persist_season which already accepts partial packs.
        _ => files.iter().any(|f| is_video_file(&f.name)),
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
///
/// Handles two cases (matching riven-ts mapItemsToFiles behaviour):
/// 1. Normal: file has season info → season must match, episode or abs must match.
/// 2. Abs-only: file has no season info → abs number alone is sufficient.
pub fn matches_episode(
    parsed: &riven_rank::ParsedData,
    season: i32,
    ep: i32,
    abs: Option<i32>,
) -> bool {
    let season_match = parsed.seasons.contains(&season)
        && (parsed.episodes.contains(&ep) || abs.is_some_and(|a| parsed.episodes.contains(&a)));

    let abs_only_match =
        parsed.seasons.is_empty() && abs.is_some_and(|a| parsed.episodes.contains(&a));

    season_match || abs_only_match
}

/// Build the VFS path for an episode file.
/// Appends `.ptN` before the extension when `part` is `Some`.
/// In multi-version mode, `path_tag` (e.g. `Some("ultra_hd")`) is prepended as
/// a top-level directory so each profile has its own directory tree.
pub fn episode_vfs_path(
    show: &str,
    season: i32,
    ep: i32,
    part: Option<i32>,
    path_tag: Option<&str>,
) -> String {
    let part_suffix = part.map(|n| format!(".pt{n}")).unwrap_or_default();
    let tag_suffix = path_tag.map(|t| format!(" [{t}]")).unwrap_or_default();
    format!(
        "/shows/{show}/Season {season:02}/{show} - s{season:02}e{ep:02}{part_suffix}{tag_suffix}.mkv"
    )
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
        } else if largest.is_none_or(|f| file.file_size > f.file_size) {
            largest = Some(file);
        }
    }

    if !by_part.is_empty() {
        by_part.into_iter().map(|(n, f)| (f, Some(n))).collect()
    } else {
        largest.map(|f| vec![(f, None)]).unwrap_or_default()
    }
}
