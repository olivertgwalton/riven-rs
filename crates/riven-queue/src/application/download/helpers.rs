use std::collections::BTreeMap;

use riven_core::types::DownloadFile;
use riven_db::entities::{MediaItem, Stream};
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
/// What Plex and Jellyfin play as music.
const VALID_AUDIO_EXTENSIONS: &[&str] = &[
    "flac", "mp3", "m4a", "aac", "ogg", "oga", "opus", "wav", "aif", "aiff", "dsf",
];

fn extension(filename: &str) -> String {
    filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
}

/// Audio files eligible for media persistence, samples excluded as for video.
pub fn is_persistable_audio_file(filename: &str) -> bool {
    VALID_AUDIO_EXTENSIONS.contains(&extension(filename).as_str()) && !is_sample_file(filename)
}

/// A track keeps its file's own extension: a music server reads the format
/// from it, where a video player probes the container. The disc goes in the
/// name rather than a folder of its own, so every track is the same depth
/// below its album.
pub fn track_vfs_path(
    artist: &str,
    album: &str,
    year: Option<i32>,
    disc: Option<i32>,
    number: i32,
    title: &str,
    original_filename: &str,
) -> String {
    let year = year.map(|y| format!(" ({y})")).unwrap_or_default();
    let disc = disc.map(|d| format!("{d}-")).unwrap_or_default();
    format!(
        "/music/{}/{}{year}/{disc}{number:02} - {}.{}",
        path_component(artist),
        path_component(album),
        path_component(title),
        extension(original_filename),
    )
}

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

/// Whether a parsed file names episode `ep` of TVDB season `season`: by
/// season+episode when the file carries seasons (any of the numberings
/// `release_numberings` allows, given the show's `show_episodes`), otherwise
/// by absolute number.
pub fn matches_episode_lookup(
    parsed: &riven_rank::ParsedData,
    ep: &MediaItem,
    season: i32,
    show_episodes: &[crate::discovery::ShowEpisode],
) -> bool {
    if parsed.seasons.is_empty() {
        ep.absolute_number
            .is_some_and(|abs| parsed.episodes.contains(&abs))
    } else {
        crate::discovery::release_numberings(
            (season, ep.episode_number.unwrap_or(1)),
            ep.scene_season_number.zip(ep.scene_episode_number),
            show_episodes,
        )
        .iter()
        .any(|(s, e)| parsed.seasons.contains(s) && parsed.episodes.contains(e))
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
/// A name made safe to be one component of a VFS path.
fn path_component(name: &str) -> String {
    let safe: String = name
        .chars()
        .map(|c| {
            if c == '/' || c == '\\' || c == '\0' {
                '_'
            } else {
                c
            }
        })
        .collect();
    safe.trim_start_matches('.').to_owned()
}

pub fn episode_vfs_path(
    show: &str,
    season: i32,
    ep: i32,
    part: Option<i32>,
    path_tag: Option<&str>,
) -> String {
    let safe_show = path_component(show);
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
    use super::{
        is_persistable_video_file, is_sample_file, matches_episode_lookup, parse_file_path,
    };
    use crate::application::download::candidates::tests::media_item;
    use crate::discovery::ShowEpisode;
    use riven_db::entities::MediaItem;

    #[test]
    fn episode_lookup_matches_season_episode_or_absolute() {
        let ep = |episode, abs| MediaItem {
            episode_number: Some(episode),
            absolute_number: abs,
            ..media_item()
        };
        let mut parsed = riven_rank::ParsedData {
            seasons: vec![1, 2],
            episodes: vec![3],
            ..Default::default()
        };
        assert!(matches_episode_lookup(&parsed, &ep(3, None), 2, &[]));
        assert!(!matches_episode_lookup(&parsed, &ep(3, Some(3)), 3, &[]));

        parsed.seasons.clear();
        assert!(matches_episode_lookup(&parsed, &ep(1, Some(3)), 1, &[]));
        assert!(!matches_episode_lookup(&parsed, &ep(3, None), 1, &[]));

        parsed.episodes.clear();
        assert!(!matches_episode_lookup(&parsed, &ep(3, Some(3)), 1, &[]));
    }

    #[test]
    fn episode_lookup_uses_scene_numbering() {
        // Kitchen Nightmares (US): TVDB S07E05 is scene S08E05, TVDB S08E05
        // is scene S09E05.
        let ep = |season, scene: (i32, i32)| {
            (
                MediaItem {
                    episode_number: Some(5),
                    scene_season_number: Some(scene.0),
                    scene_episode_number: Some(scene.1),
                    ..media_item()
                },
                ShowEpisode {
                    id: None,
                    season,
                    episode: 5,
                    title: String::new(),
                    scene: Some(scene),
                },
            )
        };
        let (s7, s7_show) = ep(7, (8, 5));
        let (s8, s8_show) = ep(8, (9, 5));
        let show = [s7_show, s8_show];
        let parsed = parse_file_path("Kitchen.Nightmares.S08E05.1080p.HULU-ViETNAM.mkv");
        assert!(matches_episode_lookup(&parsed, &s7, 7, &show));
        assert!(!matches_episode_lookup(&parsed, &s8, 8, &show));
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

    #[test]
    fn a_track_sits_in_its_album_with_its_own_extension() {
        assert_eq!(
            super::track_vfs_path(
                "AC/DC",
                "Back in Black",
                Some(1980),
                None,
                6,
                "Back in Black",
                "06.flac"
            ),
            "/music/AC_DC/Back in Black (1980)/06 - Back in Black.flac"
        );
        assert_eq!(
            super::track_vfs_path(
                "Radiohead",
                "Kid A Mnesia",
                None,
                Some(3),
                2,
                "Fog",
                "x.MP3"
            ),
            "/music/Radiohead/Kid A Mnesia/3-02 - Fog.mp3"
        );
    }
}
