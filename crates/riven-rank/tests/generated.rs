//! Round-trip coverage: build release names out of known tokens, then check the
//! parser hands back exactly the tokens that went in.
//!
//! The corpus in `tests/fixtures/release_titles.json` is real-world weirdness
//! pinned to whatever the parser does today; this is the opposite — a few
//! thousand well-formed names whose truth is known by construction, so a
//! failure here is a parser bug rather than a diff to review.

use riven_rank::{ParsedData, parse};

/// Titles with no digits and no token-shaped words, so the only things in a
/// generated name are the ones we put there.
const TITLES: &[&str] = &["Movie Title", "Blade Runner"];
const YEARS: &[i32] = &[1999, 2024];
const RESOLUTIONS: &[&str] = &["720p", "1080p", "2160p"];
/// `(token, expected)` — the parser normalises most of these.
const QUALITIES: &[(&str, &str)] = &[
    ("BluRay", "BluRay"),
    ("WEB-DL", "WEB-DL"),
    ("HDTV", "HDTV"),
    ("WEBRip", "WEBRip"),
];
const CODECS: &[(&str, &str)] = &[
    ("x264", "avc"),
    ("x265", "hevc"),
    ("XviD", "xvid"),
    ("AV1", "av1"),
];
const AUDIO: &[(&str, &str)] = &[("AAC", "AAC"), ("AC3", "Dolby Digital")];
const GROUPS: &[&str] = &["RARBG", "FraMeSToR"];
/// `(token, seasons, episodes)` — a movie, an episode and a season pack.
const NUMBERING: &[(&str, &[i32], &[i32])] =
    &[("", &[], &[]), ("S01E02", &[1], &[2]), ("S03", &[3], &[])];
/// Scene names use dots, usenet and manual entries use spaces.
const SEPARATORS: &[char] = &['.', ' '];

struct Case {
    name: String,
    title: &'static str,
    year: i32,
    seasons: &'static [i32],
    episodes: &'static [i32],
    resolution: &'static str,
    quality: &'static str,
    codec: &'static str,
    audio: &'static str,
    group: &'static str,
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for &title in TITLES {
        for &year in YEARS {
            for &(numbering, seasons, episodes) in NUMBERING {
                for &resolution in RESOLUTIONS {
                    for &(quality_token, quality) in QUALITIES {
                        for &(codec_token, codec) in CODECS {
                            for &(audio_token, audio) in AUDIO {
                                for &group in GROUPS {
                                    for &separator in SEPARATORS {
                                        let name = assemble(
                                            separator,
                                            &[
                                                title,
                                                &year.to_string(),
                                                numbering,
                                                resolution,
                                                quality_token,
                                                codec_token,
                                                audio_token,
                                            ],
                                            group,
                                        );
                                        cases.push(Case {
                                            name,
                                            title,
                                            year,
                                            seasons,
                                            episodes,
                                            resolution,
                                            quality,
                                            codec,
                                            audio,
                                            group,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    cases
}

fn assemble(separator: char, parts: &[&str], group: &str) -> String {
    let separator = separator.to_string();
    let body = parts
        .iter()
        .filter(|part| !part.is_empty())
        .map(|part| part.replace(' ', &separator))
        .collect::<Vec<_>>()
        .join(&separator);
    format!("{body}-{group}")
}

fn mismatches(case: &Case, data: &ParsedData) -> Vec<String> {
    let mut wrong = Vec::new();
    let mut check = |field: &str, got: String, want: String| {
        if got != want {
            wrong.push(format!("{field}: {got} != {want}"));
        }
    };

    check(
        "title",
        format!("{:?}", data.parsed_title),
        format!("{:?}", case.title),
    );
    check(
        "year",
        format!("{:?}", data.year),
        format!("{:?}", Some(case.year)),
    );
    check(
        "resolution",
        data.resolution.clone(),
        case.resolution.to_owned(),
    );
    check(
        "quality",
        format!("{:?}", data.quality),
        format!("{:?}", Some(case.quality)),
    );
    check(
        "codec",
        format!("{:?}", data.codec),
        format!("{:?}", Some(case.codec)),
    );
    check(
        "group",
        format!("{:?}", data.group),
        format!("{:?}", Some(case.group)),
    );
    check(
        "seasons",
        format!("{:?}", data.seasons),
        format!("{:?}", case.seasons),
    );
    check(
        "episodes",
        format!("{:?}", data.episodes),
        format!("{:?}", case.episodes),
    );

    if !data.audio.iter().any(|track| track == case.audio) {
        wrong.push(format!("audio: {:?} lacks {:?}", data.audio, case.audio));
    }
    wrong
}

#[test]
fn generated_names_round_trip() {
    let cases = cases();
    assert!(
        cases.len() > 1000,
        "generator produced {} names",
        cases.len()
    );

    let failures: Vec<String> = cases
        .iter()
        .filter_map(|case| {
            let wrong = mismatches(case, &parse(&case.name));
            (!wrong.is_empty())
                .then(|| format!("  {}\n      {}", case.name, wrong.join("\n      ")))
        })
        .collect();

    assert!(
        failures.is_empty(),
        "{} of {} generated names parsed wrongly:\n{}",
        failures.len(),
        cases.len(),
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
