use std::collections::BTreeMap;

use riven_core::types::DownloadFile;
use riven_db::entities::Stream;
use riven_db::repo;

/// The parsed resolution of a stream, falling back to `"unknown"` when the
/// `parsed_data` is missing or has no `resolution` field.
pub(crate) fn stream_resolution(stream: &Stream) -> &str {
    stream
        .parsed_data
        .as_ref()
        .and_then(|parsed| parsed.get("resolution"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
}

/// The indexer release title of a stream, falling back to `""` when the
/// `parsed_data` is missing or has no `raw_title` field.
pub(crate) fn stream_raw_title(stream: &Stream) -> &str {
    stream
        .parsed_data
        .as_ref()
        .and_then(|parsed| parsed.get("raw_title"))
        .and_then(|value| value.as_str())
        .unwrap_or("")
}

/// Log a bitrate failure and store the file size so the next download attempt can
/// pre-filter this stream before handing it to a download plugin. Does not blacklist
/// and does not emit any events — the caller's loop continues to the next stream.
pub async fn handle_bitrate_failure(
    id: i64,
    info_hash: &str,
    file: &str,
    file_size: u64,
    runtime: Option<i32>,
    context: &str,
) {
    tracing::warn!(
        id,
        file_size,
        runtime = ?runtime,
        info_hash = %info_hash,
        file,
        "{context} failed bitrate check — skipping stream"
    );
    if !info_hash.is_empty()
        && let Err(err) = repo::update_stream_file_size(info_hash, file_size).await
    {
        tracing::warn!(info_hash, file, %err, "failed to update stream file size");
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

/// Release samples ("movie.sample.mkv", "Sample/…") are short preview clips.
/// Detected by a delimited "sample" token anywhere in the path so that titles
/// merely containing the word (e.g. "Free.Samples.2012") don't match.
pub fn is_sample_file(path: &str) -> bool {
    path.to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|token| token == "sample")
}

/// Video files eligible for media persistence. Samples are excluded so a
/// preview clip is never persisted as the item's media — even when the full
/// file is missing from the payload (e.g. failed RAR assembly), in which case
/// the stream should fail and be blacklisted rather than "succeed" with a
/// 30-second clip.
pub fn is_persistable_video_file(filename: &str) -> bool {
    is_video_file(filename) && !is_sample_file(filename)
}

pub use riven_core::filename::looks_obfuscated;

/// Whether a parsed file names this episode: by season+episode when the file
/// carries seasons, otherwise by absolute number.
pub fn matches_episode_lookup(
    parsed: &riven_rank::ParsedData,
    season: i32,
    ep: i32,
    abs: Option<i32>,
) -> bool {
    if parsed.seasons.is_empty() {
        abs.is_some_and(|abs| parsed.episodes.contains(&abs))
    } else {
        parsed.seasons.contains(&season) && parsed.episodes.contains(&ep)
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
        .map(|c| {
            if c == '/' || c == '\\' || c == '\0' {
                '_'
            } else {
                c
            }
        })
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

#[cfg(test)]
mod tests {
    use super::{is_persistable_video_file, is_sample_file, matches_episode_lookup};

    #[test]
    fn episode_lookup_matches_season_episode_or_absolute() {
        let mut parsed = riven_rank::ParsedData {
            seasons: vec![1, 2],
            episodes: vec![3],
            ..Default::default()
        };
        assert!(matches_episode_lookup(&parsed, 2, 3, None));
        assert!(!matches_episode_lookup(&parsed, 3, 3, Some(3)));

        parsed.seasons.clear();
        assert!(matches_episode_lookup(&parsed, 1, 1, Some(3)));
        assert!(!matches_episode_lookup(&parsed, 1, 3, None));

        parsed.episodes.clear();
        assert!(!matches_episode_lookup(&parsed, 1, 3, Some(3)));
    }

    #[test]
    fn sample_files_are_detected_and_excluded() {
        assert!(is_sample_file(
            "top.gear.s23e03.1080p.bluray.x264-ouija.sample.mkv"
        ));
        assert!(is_sample_file("Sample/top.gear.s23e03.1080p.mkv"));
        assert!(!is_persistable_video_file(
            "top.gear.s23e03.1080p.bluray.x264-ouija.sample.mkv"
        ));

        assert!(!is_sample_file("Free.Samples.2012.1080p.BluRay.mkv"));
        assert!(is_persistable_video_file(
            "top.gear.s23e03.1080p.bluray.x264-ouija.mkv"
        ));
    }
}
