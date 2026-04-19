use std::collections::BTreeMap;

use riven_core::types::{DownloadFile, MediaItemType};
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::JobQueue;
use crate::context::load_media_item_or_download_error;

/// Load a media item by id, or send a `MediaItemDownloadError` event and return `None`.
pub async fn load_item_or_err(id: i64, queue: &JobQueue, error_msg: &str) -> Option<MediaItem> {
    load_media_item_or_download_error(queue, id, error_msg).await
}

/// Log a bitrate failure and store the file size so the next download attempt can
/// pre-filter this stream before touching the debrid service. Does not blacklist
/// and does not emit any events — the caller's loop continues to the next stream.
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
        "{context} failed bitrate check — skipping stream"
    );
    if !info_hash.is_empty() {
        if let Err(err) = repo::update_stream_file_size(&queue.db_pool, info_hash, file_size).await
        {
            tracing::warn!(info_hash, %err, "failed to update stream file size");
        }
    }
}

/// Returns `(max_size_bytes, min_size_bytes)` derived from the configured bitrate
/// limits and the item's runtime. Both are `None` when the limit is not configured
/// or when the item has no runtime.
pub async fn load_bitrate_limits(queue: &JobQueue, item: &MediaItem) -> (Option<u64>, Option<u64>) {
    let config = queue.downloader_config.read().await;
    let bitrate_threshold = |movies: Option<u32>, episodes: Option<u32>| {
        let mbps = match item.item_type {
            MediaItemType::Movie => movies,
            MediaItemType::Episode => episodes,
            _ => None,
        };
        mbps.zip(item.runtime)
            .map(|(m, rt)| riven_core::downloader::DownloaderConfig::threshold_bytes(m, rt))
    };
    let max_size_bytes = bitrate_threshold(
        config.maximum_average_bitrate_movies,
        config.maximum_average_bitrate_episodes,
    );
    let min_size_bytes = bitrate_threshold(
        config.minimum_average_bitrate_movies,
        config.minimum_average_bitrate_episodes,
    );
    (max_size_bytes, min_size_bytes)
}

const VALID_VIDEO_EXTENSIONS: &[&str] = &["mp4", "mkv", "avi", "mov", "wmv", "flv", "webm"];

/// Returns true if the filename has a recognised video extension.
pub fn is_video_file(filename: &str) -> bool {
    let ext = filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    VALID_VIDEO_EXTENSIONS.contains(&ext.as_str())
}

pub fn episode_lookup_keys(season: i32, ep: i32, abs: Option<i32>) -> Vec<String> {
    let mut keys = Vec::with_capacity(2);
    if let Some(abs) = abs {
        keys.push(format!("abs:{abs}"));
    }
    keys.push(format!("{season}:{ep}"));
    keys
}

pub fn file_lookup_keys(parsed: &riven_rank::ParsedData) -> Vec<String> {
    if parsed.episodes.is_empty() {
        return Vec::new();
    }

    if parsed.seasons.is_empty() {
        return parsed
            .episodes
            .iter()
            .map(|episode| format!("abs:{episode}"))
            .collect();
    }

    parsed
        .seasons
        .iter()
        .flat_map(|season| {
            parsed
                .episodes
                .iter()
                .map(move |episode| format!("{season}:{episode}"))
        })
        .collect()
}

pub fn matches_episode_lookup(
    parsed: &riven_rank::ParsedData,
    season: i32,
    ep: i32,
    abs: Option<i32>,
) -> bool {
    let lookups = episode_lookup_keys(season, ep, abs);
    file_lookup_keys(parsed)
        .iter()
        .any(|key| lookups.iter().any(|lookup| lookup == key))
}

/// Parse a file path by merging metadata from all path segments.
pub fn parse_file_path(path: &str) -> riven_rank::ParsedData {
    let mut merged = riven_rank::ParsedData::default();
    for segment in path.split('/').filter(|s| !s.is_empty()) {
        merged.merge(riven_rank::parse(segment));
    }
    merged
}

/// Build the VFS path for an episode file.
/// Appends `.ptN` before the extension when `part` is `Some`.
/// When active ranking profiles are enabled, `path_tag` (e.g. `Some("ultra_hd")`) is prepended as
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
