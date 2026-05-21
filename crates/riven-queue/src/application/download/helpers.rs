use std::collections::BTreeMap;

use riven_core::types::DownloadFile;
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
    if !info_hash.is_empty()
        && let Err(err) = repo::update_stream_file_size(&queue.db_pool, info_hash, file_size).await
    {
        tracing::warn!(info_hash, %err, "failed to update stream file size");
    }
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

/// Heuristic check for an obfuscated filename — random hash/blob stems with no
/// release-name structure. Used to decide whether to fall back to the NZB
/// release title:
///
/// - long alphanumeric stems with no spaces, dots, dashes, or underscores
///   between the first and last character (e.g. `VfYc6l3ibzTHwlPkvX1hocwymwUNt6yt`)
/// - pure 32-char hex (`^[a-f0-9]{32}$`)
/// - long dot-separated hex sequences (`^[a-f0-9.]{40,}$`)
/// - `abc.xyz...` placeholder pattern emitted by some indexers
///
/// We're conservative: only flag when the stem is plausibly random. Real
/// release names always have at least one separator (`.`, ` `, `-`, `_`), so
/// excluding those handles every well-formed scene/p2p release.
pub fn looks_obfuscated(filename: &str) -> bool {
    let stem = match filename.rfind('.') {
        Some(i) if i > 0 => &filename[..i],
        _ => filename,
    };
    if stem.is_empty() {
        return false;
    }
    if stem.starts_with("abc.xyz") {
        return true;
    }
    let lower = stem.to_ascii_lowercase();
    let is_hex = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit());
    if stem.len() == 32 && is_hex(&lower) {
        return true;
    }
    if lower.len() >= 40 && lower.chars().all(|c| c.is_ascii_hexdigit() || c == '.') {
        return true;
    }
    // Long, single-token stem with no separators — typical of obfuscated
    // releases (e.g. `VfYc6l3ibzTHwlPkvX1hocwymwUNt6yt`). 24 chars is the
    // shortest such name we want to catch without flagging real titles like
    // `Movie.2024.mkv`.
    if stem.len() >= 24
        && !stem.contains(['.', ' ', '-', '_'])
        && stem.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return true;
    }
    false
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
/// When active ranking profiles are enabled, `path_tag` (e.g. `Some("ultra_hd")`) is appended as
/// a bracketed suffix in the filename (e.g. `Show - s01e01 [ultra_hd].mkv`).
///
/// `show` is sanitized: path separators and leading dots are replaced with `_` to prevent
/// directory traversal or embedded path components.
pub fn episode_vfs_path(
    show: &str,
    season: i32,
    ep: i32,
    part: Option<i32>,
    path_tag: Option<&str>,
) -> String {
    let safe_show: String = show
        .chars()
        .map(|c| if c == '/' || c == '\\' || c == '\0' { '_' } else { c })
        .collect();
    let safe_show = safe_show.trim_start_matches('.');
    let part_suffix = part.map(|n| format!(".pt{n}")).unwrap_or_default();
    let tag_suffix = path_tag.map(|t| format!(" [{t}]")).unwrap_or_default();
    format!(
        "/shows/{safe_show}/Season {season:02}/{safe_show} - s{season:02}e{ep:02}{part_suffix}{tag_suffix}.mkv"
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
