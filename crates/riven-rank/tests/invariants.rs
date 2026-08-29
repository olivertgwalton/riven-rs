//! Properties that must hold for *every* release title, no expectations
//! required — the half of the suite that can say the parser is wrong rather
//! than merely different from last time.
//!
//! Each check carries a `KNOWN` list of titles that violate it today. Those
//! lists are exact: fixing a parser bug fails the test until its title is
//! removed, so they only ever shrink.

mod common;

use common::{corpus, parsed};
use riven_rank::{ParsedData, parse};

/// Compare the violations found against the ones we know about.
fn expect(known: &[&str], found: &[(&str, String)]) {
    let mut fresh: Vec<&(&str, String)> = found
        .iter()
        .filter(|(raw, _)| !known.contains(raw))
        .collect();
    fresh.sort_by_key(|(raw, _)| *raw);

    let fixed: Vec<&&str> = known
        .iter()
        .filter(|raw| !found.iter().any(|(found, _)| found == *raw))
        .collect();

    assert!(
        fresh.is_empty(),
        "{} new violation(s):\n{}",
        fresh.len(),
        fresh
            .iter()
            .map(|(raw, detail)| format!("  {raw}\n      {detail}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        fixed.is_empty(),
        "{} title(s) no longer violate this — drop them from KNOWN:\n{}",
        fixed.len(),
        fixed
            .iter()
            .map(|raw| format!("  {raw}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// A release with no title cannot be matched to a media item, so it can never
/// be selected however good the release is.
#[test]
fn every_release_has_a_title() {
    const KNOWN: &[&str] = &[
        "2.Sezon",
        "Ponyo[2008]DvDrip-H264 Quad Audio[Eng Jap Fre Spa]AC3 5.1[DXO]",
    ];

    let found: Vec<(&str, String)> = parsed()
        .filter(|d| d.parsed_title.trim().is_empty())
        .map(|d| (d.raw_title.as_str(), "no title extracted".to_owned()))
        .collect();

    expect(KNOWN, &found);
}

/// Anything the parser lifted into a field must be gone from the title, or it
/// leaks into title similarity and drags in the wrong release.
#[test]
fn title_keeps_no_extracted_token() {
    const KNOWN: &[&str] = &[
        // quality left in the title
        "Avatar La Voie de l'eau.FRENCH.CAMHD.H264.AAC",
        "Movie.Title.R5.x264",
        "Movie.Title.VHSRip.x264",
        "Movie.Title.WEBMUX.x264",
        "Структура момента (Расим Исмайлов) [1980, Драма, VHSRip]",
        // codec left in the title
        "Movie.Title.mpeg2.DVD",
        // group left in the title
        "[Anime Time] One Piece (0001-1071+Movies+Specials) [BD+CR] [Dual Audio] [1080p][HEVC 10bit x265][AAC][Multi Sub]",
        "[Anime Time] Re Zero kara Hajimeru Isekai Seikatsu (Season 2 Part 1) [1080p][HEVC10bit x265][Multi Sub]",
        "[Eng Sub] Rebirth Ep #36 [8CF3ADFA].mkv",
        "[KNK E MMS Fansubs] Nisekoi - 20 Final [PT-BR].mkv",
        "[POPAS] Neon Genesis Evangelion: The End of Evangelion [jp_PT-pt",
        "Altair - A Record of Battles Vol. 01-08 (Digital) (danke-Empire)",
        "Some.Random.Title.Without.Resolution-GROUP",
    ];

    let extracted = |d: &ParsedData| {
        [
            (d.resolution != "unknown").then(|| ("resolution", d.resolution.clone())),
            d.quality.clone().map(|v| ("quality", v)),
            d.codec.clone().map(|v| ("codec", v)),
            d.group.clone().map(|v| ("group", v)),
            d.edition.clone().map(|v| ("edition", v)),
            d.extension.clone().map(|v| ("extension", v)),
            d.site.clone().map(|v| ("site", v)),
        ]
        .into_iter()
        .flatten()
    };

    let found: Vec<(&str, String)> = parsed()
        .filter_map(|d| {
            let title = d.parsed_title.to_lowercase();
            let leaked: Vec<String> = extracted(d)
                .filter(|(_, token)| title.contains(&token.to_lowercase()))
                .map(|(field, token)| format!("{field}={token}"))
                .collect();
            (!leaked.is_empty()).then(|| {
                (
                    d.raw_title.as_str(),
                    format!("{:?} still contains {}", d.parsed_title, leaked.join(", ")),
                )
            })
        })
        .collect();

    expect(KNOWN, &found);
}

/// The normalised title is what title matching keys on.
#[test]
fn normalized_title_is_normalised() {
    let found: Vec<(&str, String)> = parsed()
        .filter(|d| {
            let n = &d.normalized_title;
            *n != n.to_lowercase() || n.trim() != n || n.contains("  ")
        })
        .map(|d| {
            (
                d.raw_title.as_str(),
                format!(
                    "{:?} is not lowercase/trimmed/single-spaced",
                    d.normalized_title
                ),
            )
        })
        .collect();

    expect(&[], &found);
}

/// Downstream code treats these as ordered sets when matching episodes to a
/// season pack.
#[test]
fn season_and_episode_numbers_are_ordered_sets() {
    let found: Vec<(&str, String)> = parsed()
        .filter_map(|d| {
            let bad: Vec<String> = [
                ("seasons", &d.seasons),
                ("episodes", &d.episodes),
                ("volumes", &d.volumes),
            ]
            .into_iter()
            .filter_map(|(field, values)| {
                let mut ordered = values.clone();
                ordered.sort_unstable();
                ordered.dedup();
                (ordered != *values || values.iter().any(|&n| n < 0))
                    .then(|| format!("{field}={values:?}"))
            })
            .collect();
            (!bad.is_empty()).then(|| (d.raw_title.as_str(), bad.join(", ")))
        })
        .collect();

    expect(&[], &found);
}

#[test]
fn year_is_plausible() {
    let found: Vec<(&str, String)> = parsed()
        .filter(|d| d.year.is_some_and(|y| !(1900..=2100).contains(&y)))
        .map(|d| (d.raw_title.as_str(), format!("year={:?}", d.year)))
        .collect();

    expect(&[], &found);
}

/// Resolution is compared as a string all over the ranking code, so a novel
/// value silently disables every resolution rule.
#[test]
fn resolution_is_from_the_known_set() {
    const KNOWN_RESOLUTIONS: &[&str] = &[
        "unknown", "240p", "360p", "480p", "576p", "720p", "720i", "1080p", "1080i", "1440p",
        "2160p", "4320p",
    ];

    let found: Vec<(&str, String)> = parsed()
        .filter(|d| !KNOWN_RESOLUTIONS.contains(&d.resolution.as_str()))
        .map(|d| (d.raw_title.as_str(), format!("resolution={}", d.resolution)))
        .collect();

    expect(&[], &found);
}

/// Language settings are configured with ISO 639-1 codes; `multi` is the one
/// sentinel the parser adds on top.
#[test]
fn languages_are_iso_codes() {
    let found: Vec<(&str, String)> = corpus()
        .iter()
        .filter(|(case, _)| !case.translate_languages)
        .map(|(_, d)| d)
        .filter_map(|d| {
            let mut deduped = d.languages.clone();
            deduped.sort();
            deduped.dedup();
            let odd: Vec<&String> = d
                .languages
                .iter()
                .filter(|l| (l.len() != 2 || **l != l.to_lowercase()) && *l != "multi")
                .collect();
            (!odd.is_empty() || deduped.len() != d.languages.len())
                .then(|| (d.raw_title.as_str(), format!("languages={:?}", d.languages)))
        })
        .collect();

    expect(&[], &found);
}

/// Scene names use dots where usenet and manual entries use spaces. The two
/// spellings of the same release must parse the same.
#[test]
fn separator_style_does_not_change_the_parse() {
    const KNOWN: &[&str] = &[
        "[SubsPlease] Anime Title - 01.mkv",
        "[www.arabp2p.net]_-_تركي مترجم ومدبلج Last.Call.for.Istanbul.2023.1080p.NF.WEB-DL.DDP5.1.H.264.MKV.torrent",
        "Last.Call.for.Istanbul.2023.1080p.NF.WEB-DL.DDP5.1.H.264.MKV.torrent",
        "the-x-files-502.mkv",
        "wwf.raw.is.war.18.09.00.avi",
    ];

    let found = compare_variants(|raw| raw.contains('.').then(|| raw.replace('.', " ")));

    expect(KNOWN, &found);
}

/// An indexer prefixing its own tag must not disturb anything but `site`.
#[test]
fn site_prefix_does_not_change_the_parse() {
    const KNOWN: &[&str] = &[
        "www,1TamilMV.phd - The Great Indian Suicide (2023) Tamil TRUE WEB-DL - 4K SDR - HEVC - (DD+5.1 - 384Kbps & AAC) - 3.2GB - ESub.mkv",
    ];

    let found = compare_variants(|raw| Some(format!("[www.Torrenting.com] {raw}")));

    expect(KNOWN, &found);
}

/// Re-parse a rewritten spelling of each title and report where the fields that
/// identify the release drift.
fn compare_variants(rewrite: impl Fn(&str) -> Option<String>) -> Vec<(&'static str, String)> {
    let identity = |d: &ParsedData| {
        (
            d.resolution.clone(),
            d.seasons.clone(),
            d.episodes.clone(),
            d.year,
            d.codec.clone(),
            d.quality.clone(),
        )
    };

    parsed()
        .filter_map(|d| {
            let variant = parse(&rewrite(&d.raw_title)?);
            (identity(&variant) != identity(d)).then(|| {
                (
                    d.raw_title.as_str(),
                    format!(
                        "{:?}\n      variant {:?}\n      original {:?}",
                        variant.raw_title,
                        identity(&variant),
                        identity(d)
                    ),
                )
            })
        })
        .collect()
}
