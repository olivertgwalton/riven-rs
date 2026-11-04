//! Music releases: whether one is the album asked for, and how good a copy.
//!
//! A music release is named for an artist and an album, then the format it
//! was encoded in and where it was ripped from:
//! `Radiohead - OK Computer (1997) [FLAC 24bit-96kHz]`, or in scene style
//! `Radiohead-OK_Computer-(CDNODATA22)-WEB-FLAC-2016-GROUP`. None of the
//! film and television vocabulary (resolution, HDR, episodes) applies, so
//! these have a parser of their own, and profiles of their own in
//! [`MusicRankSettings`].

use serde::{Deserialize, Serialize};

use crate::parse::title::remove_accents;

mod profile;

pub use profile::{BitDepthPreference, MusicProfile, MusicRankSettings, PerFormat, PerSource};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioFormat {
    Flac,
    Alac,
    Wav,
    Dsd,
    Mp3,
    Aac,
    Opus,
    Vorbis,
}

impl AudioFormat {
    pub const fn is_lossless(self) -> bool {
        matches!(self, Self::Flac | Self::Alac | Self::Wav | Self::Dsd)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LossyQuality {
    /// Constant bitrate, in kbit/s.
    Cbr(u32),
    /// LAME's highest-quality variable bitrate preset, about 245 kbit/s.
    V0,
    /// About 190 kbit/s.
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioSource {
    Cd,
    Web,
    Vinyl,
    Sacd,
    Cassette,
}

/// What a release's name says about it, beyond the artist and album.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MusicRelease {
    pub raw_title: String,
    pub format: Option<AudioFormat>,
    pub bit_depth: Option<u8>,
    pub lossy_quality: Option<LossyQuality>,
    pub source: Option<AudioSource>,
    /// The album's title is followed by nothing else of a title: a year, a
    /// format, a tag or the end. `Kid A` is named exactly by `Kid A (2000)`
    /// and only partly by `Kid A Mnesia`.
    pub names_album_exactly: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MusicRejection {
    #[error("release does not name the artist")]
    WrongArtist,
    #[error("release does not name the album")]
    WrongAlbum,
    #[error("release is a discography or collection, not one album")]
    Discography,
}

/// Words that say a release holds many albums.
const COLLECTION_WORDS: &[&str] = &[
    "discography",
    "discografia",
    "discographie",
    "discog",
    "anthology",
    "collection",
    "boxset",
];

/// Decide whether `raw_title` is a release of `album` by `artist` (or one of
/// its `artist_aliases`), and read what it says about its quality.
pub fn judge_album(
    raw_title: &str,
    artist: &str,
    artist_aliases: &[String],
    album: &str,
) -> Result<MusicRelease, MusicRejection> {
    let tokens = tokenize(raw_title);
    let artist_run = std::iter::once(artist)
        .chain(artist_aliases.iter().map(String::as_str))
        .flat_map(artist_spellings)
        .find_map(|spelling| find_run(&tokens, &spelling, 0).map(|at| (at, spelling.len())))
        .ok_or(MusicRejection::WrongArtist)?;

    let album_tokens = tokenize(album);
    // Self-titled albums are named once, over the artist.
    let album_at = find_run(&tokens, &album_tokens, artist_run.0 + artist_run.1)
        .or_else(|| find_run(&tokens, &album_tokens, 0))
        .ok_or(MusicRejection::WrongAlbum)?;
    let after_album = album_at + album_tokens.len();

    let rest: Vec<&str> = tokens
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            !(*i >= artist_run.0 && *i < artist_run.0 + artist_run.1)
                && !(*i >= album_at && *i < after_album)
        })
        .map(|(_, token)| token.as_str())
        .collect();
    if is_collection(&rest) {
        return Err(MusicRejection::Discography);
    }

    Ok(MusicRelease {
        raw_title: raw_title.to_owned(),
        format: format_of(&rest),
        bit_depth: bit_depth_of(&rest),
        lossy_quality: lossy_quality_of(&rest),
        source: source_of(&rest),
        names_album_exactly: tokens.get(after_album).is_none_or(|next| is_tag(next)),
    })
}

/// Lower case, without accents or punctuation, `&` read as `and`. Numbers
/// are kept: `1989` and `2112` are album titles.
pub fn tokenize(text: &str) -> Vec<String> {
    let lower = remove_accents(&text.to_lowercase()).replace('&', " and ");
    lower
        .chars()
        .filter(|c| !matches!(c, '\'' | '\u{2019}'))
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// An artist is often written without its leading article.
fn artist_spellings(name: &str) -> Vec<Vec<String>> {
    let tokens = tokenize(name);
    let mut spellings = vec![tokens.clone()];
    if tokens.len() > 1 && tokens[0] == "the" {
        spellings.push(tokens[1..].to_vec());
    }
    spellings
}

fn find_run(tokens: &[String], run: &[String], from: usize) -> Option<usize> {
    if run.is_empty() || from > tokens.len() {
        return None;
    }
    tokens[from..]
        .windows(run.len())
        .position(|window| window == run)
        .map(|at| at + from)
}

fn is_year(token: &str) -> bool {
    token.len() == 4
        && token.chars().all(|c| c.is_ascii_digit())
        && matches!(&token[..2], "19" | "20")
}

/// A token that is about the release rather than the album's title.
fn is_tag(token: &str) -> bool {
    is_year(token)
        || format_of(&[token]).is_some()
        || source_of(&[token]).is_some()
        || matches!(
            token,
            "deluxe" | "remastered" | "remaster" | "edition" | "web" | "cd"
        )
}

fn is_collection(rest: &[&str]) -> bool {
    if rest.iter().any(|token| COLLECTION_WORDS.contains(token)) {
        return true;
    }
    // `1993 2016` once punctuation is gone: a span of years.
    rest.windows(2)
        .any(|pair| is_year(pair[0]) && is_year(pair[1]) && pair[0] < pair[1])
        || rest.windows(2).any(|pair| pair == ["complete", "albums"])
        || rest.windows(2).any(|pair| pair == ["box", "set"])
}

fn format_of(rest: &[&str]) -> Option<AudioFormat> {
    rest.iter().find_map(|token| match *token {
        "flac" | "webflac" => Some(AudioFormat::Flac),
        "alac" => Some(AudioFormat::Alac),
        "wav" => Some(AudioFormat::Wav),
        "dsd" | "dsf" | "dff" => Some(AudioFormat::Dsd),
        "mp3" => Some(AudioFormat::Mp3),
        "aac" | "m4a" => Some(AudioFormat::Aac),
        "opus" => Some(AudioFormat::Opus),
        "ogg" | "vorbis" => Some(AudioFormat::Vorbis),
        _ => None,
    })
}

fn bit_depth_of(rest: &[&str]) -> Option<u8> {
    for (i, token) in rest.iter().enumerate() {
        match *token {
            "24bit" | "24bits" | "hires" => return Some(24),
            "16bit" | "16bits" => return Some(16),
            // `24-96`, `24 bit`, `16-44`.
            "24" | "16" => {
                let next = rest.get(i + 1).copied().unwrap_or_default();
                if matches!(
                    next,
                    "bit" | "bits" | "44" | "48" | "88" | "96" | "176" | "192"
                ) || next.ends_with("khz")
                {
                    return Some(if *token == "24" { 24 } else { 16 });
                }
            }
            "hi" if rest.get(i + 1) == Some(&"res") => return Some(24),
            _ => {}
        }
    }
    None
}

fn lossy_quality_of(rest: &[&str]) -> Option<LossyQuality> {
    rest.iter().find_map(|token| match *token {
        "v0" => Some(LossyQuality::V0),
        "v2" => Some(LossyQuality::V2),
        _ => {
            let digits = token
                .strip_suffix("kbps")
                .or_else(|| token.strip_suffix("kbs"))
                .or_else(|| token.strip_suffix('k'))
                .unwrap_or(token);
            match digits.parse::<u32>() {
                Ok(kbps @ (128 | 160 | 192 | 224 | 256 | 320)) => Some(LossyQuality::Cbr(kbps)),
                _ => None,
            }
        }
    })
}

fn source_of(rest: &[&str]) -> Option<AudioSource> {
    rest.iter().find_map(|token| match *token {
        "cd" | "cdda" | "cdrip" => Some(AudioSource::Cd),
        "web" | "webflac" | "webrip" => Some(AudioSource::Web),
        "vinyl" | "vinylrip" | "lp" => Some(AudioSource::Vinyl),
        "sacd" => Some(AudioSource::Sacd),
        "cassette" | "tape" => Some(AudioSource::Cassette),
        _ => None,
    })
}

#[cfg(test)]
mod tests;
