//! Which file of an album release is which track.
//!
//! Album files are named for their place on the album and their title:
//! `01 - Airbag.flac`, `CD2/03. Lull.flac`, `1-03 Lull.flac`,
//! `Radiohead - OK Computer - 01 - Airbag.flac`. A title outranks a number,
//! since editions differ in order and length; the number places the rest.

use std::sync::LazyLock;

use regex::Regex;
use riven_rank::music::tokenize;

pub struct TrackRef<'a> {
    pub disc: i32,
    pub number: i32,
    pub title: &'a str,
}

/// For each track, the index of the file that is it.
pub fn match_files_to_tracks(files: &[&str], tracks: &[TrackRef<'_>]) -> Vec<Option<usize>> {
    let multi_disc = tracks.iter().any(|track| track.disc > 1);
    let titles: Vec<Vec<String>> = tracks.iter().map(|track| tokenize(track.title)).collect();

    let mut order: Vec<usize> = (0..files.len()).collect();
    order.sort_by(|a, b| files[*a].cmp(files[*b]));

    let mut owner_of_track: Vec<Option<usize>> = vec![None; tracks.len()];
    for &file in &order {
        let owner = named_track(files[file], &titles).or_else(|| {
            let (disc, number) = position(files[file], multi_disc)?;
            tracks
                .iter()
                .position(|track| track.number == number && disc.unwrap_or(1) == track.disc)
        });
        if let Some(track) = owner
            && owner_of_track[track].is_none()
        {
            owner_of_track[track] = Some(file);
        }
    }

    // Obfuscated names say nothing; a release of exactly the album's length
    // in name order is the album in order. The release's own name was judged
    // to be the album before it was downloaded.
    if owner_of_track.iter().all(Option::is_none) && files.len() == tracks.len() {
        let mut by_place: Vec<usize> = (0..tracks.len()).collect();
        by_place.sort_by_key(|&track| (tracks[track].disc, tracks[track].number));
        for (track, file) in by_place.into_iter().zip(order) {
            owner_of_track[track] = Some(file);
        }
    }
    owner_of_track
}

/// The track whose title the file's name spells out, the longest when one
/// title is part of another (`You` and `Thinking About You`); none when two
/// are spelled out at equal length.
fn named_track(path: &str, titles: &[Vec<String>]) -> Option<usize> {
    let name = tokenize(file_stem(path));
    let mut best: Option<(usize, usize)> = None;
    let mut tied = false;
    for (track, title) in titles.iter().enumerate() {
        if title.is_empty()
            || !name
                .windows(title.len())
                .any(|window| window == title.as_slice())
        {
            continue;
        }
        match best {
            Some((_, len)) if len > title.len() => {}
            Some((_, len)) if len == title.len() => tied = true,
            _ => {
                best = Some((track, title.len()));
                tied = false;
            }
        }
    }
    best.filter(|_| !tied).map(|(track, _)| track)
}

fn file_stem(path: &str) -> &str {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.rsplit_once('.').map_or(name, |(stem, _)| stem)
}

/// The disc (when the name says) and the track number a file is named for.
fn position(path: &str, multi_disc: bool) -> Option<(Option<i32>, i32)> {
    static DISC_DIR: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)^(?:cd|disc|disk)\s*(\d{1,2})\b").unwrap());
    static DISC_TRACK: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^\s*(\d{1,2})[-.](\d{1,3})\b").unwrap());
    static LEADING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\s*(\d{1,3})\b").unwrap());
    static INNER: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?:^|[\s_\-.])(\d{1,2})(?:[\s_\-.]|$)").unwrap());

    let mut components: Vec<&str> = path.split('/').collect();
    components.pop();
    let dir_disc = components
        .iter()
        .rev()
        .find_map(|dir| DISC_DIR.captures(dir.trim()))
        .and_then(|captures| captures[1].parse().ok());

    let stem = file_stem(path);
    if let Some(captures) = DISC_TRACK.captures(stem) {
        return Some((captures[1].parse().ok(), captures[2].parse().ok()?));
    }
    if let Some(captures) = LEADING.captures(stem) {
        let number: i32 = captures[1].parse().ok()?;
        // `103` on a multi-disc album is disc 1, track 3.
        if multi_disc && dir_disc.is_none() && number >= 100 {
            return Some((Some(number / 100), number % 100));
        }
        return Some((dir_disc, number));
    }
    let number = INNER.captures(stem)?[1].parse().ok()?;
    Some((dir_disc, number))
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK_COMPUTER: &[&str] = &["Airbag", "Paranoid Android", "Subterranean Homesick Alien"];

    fn tracks<'a>(titles: &[&'a str]) -> Vec<TrackRef<'a>> {
        titles
            .iter()
            .enumerate()
            .map(|(i, title)| TrackRef {
                disc: 1,
                number: i as i32 + 1,
                title,
            })
            .collect()
    }

    #[test]
    fn numbered_files_are_their_tracks() {
        let files = [
            "Radiohead - OK Computer/03 - Subterranean Homesick Alien.flac",
            "Radiohead - OK Computer/01 - Airbag.flac",
            "Radiohead - OK Computer/02 - Paranoid Android.flac",
        ];
        assert_eq!(
            match_files_to_tracks(&files, &tracks(OK_COMPUTER)),
            [Some(1), Some(2), Some(0)]
        );
    }

    /// A reissue that orders its tracks differently still has their names.
    #[test]
    fn a_title_outranks_a_number() {
        let files = ["01 - Paranoid Android.flac", "02 - Airbag.flac"];
        assert_eq!(
            match_files_to_tracks(&files, &tracks(OK_COMPUTER)),
            [Some(1), Some(0), None]
        );
    }

    #[test]
    fn the_longest_title_wins() {
        let files = ["12 - Thinking About You.flac", "01 - You.flac"];
        let titles = tracks(&["You", "Thinking About You"]);
        assert_eq!(match_files_to_tracks(&files, &titles), [Some(1), Some(0)]);
    }

    #[test]
    fn a_title_track_is_placed_by_number_beside_the_album_name() {
        let files = [
            "Radiohead - The Bends - 01 - Planet Telex.flac",
            "Radiohead - The Bends - 02 - The Bends.flac",
        ];
        let titles = tracks(&["Planet Telex", "The Bends"]);
        assert_eq!(match_files_to_tracks(&files, &titles), [Some(0), Some(1)]);
    }

    #[test]
    fn discs_come_from_folders_prefixes_or_hundreds() {
        let titles = vec![
            TrackRef {
                disc: 1,
                number: 1,
                title: "Airbag",
            },
            TrackRef {
                disc: 2,
                number: 1,
                title: "Lull",
            },
        ];
        for files in [
            ["CD1/01 - Track.flac", "CD2/01 - Track.flac"],
            ["1-01 Track.flac", "2-01 Track.flac"],
            ["101 Track.flac", "201 Track.flac"],
        ] {
            assert_eq!(
                match_files_to_tracks(&files, &titles),
                [Some(0), Some(1)],
                "{files:?}"
            );
        }
    }

    #[test]
    fn obfuscated_files_of_the_albums_length_go_in_order() {
        let files = ["c2a9f1.flac", "a81be0.flac", "b07d3c.flac"];
        assert_eq!(
            match_files_to_tracks(&files, &tracks(OK_COMPUTER)),
            [Some(1), Some(2), Some(0)]
        );
    }

    #[test]
    fn obfuscated_files_of_another_length_are_no_tracks() {
        let files = ["c2a9f1.flac", "a81be0.flac"];
        assert_eq!(
            match_files_to_tracks(&files, &tracks(OK_COMPUTER)),
            [None, None, None]
        );
    }
}
