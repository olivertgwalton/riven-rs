use regex::Regex;
use std::sync::LazyLock;

use super::patterns::*;

/// Earliest match start of any of `regexes` in `text`.
fn min_match_start(text: &str, regexes: &[&Regex]) -> Option<usize> {
    regexes
        .iter()
        .filter_map(|re| re.find(text))
        .map(|m| m.start())
        .min()
}

static RE_ANY_BRACKET_TITLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[([^\]]*)\]").unwrap());
static RE_BRACKET_ALIAS_TITLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[[^\]]*[A-Za-z][^\]]*\]").unwrap());
static RE_SLASH_ALIAS_TITLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"/\s*[A-Za-z]").unwrap());

fn normalize_bracket_candidate(inner: &str) -> String {
    let mut ascii_match: Option<&str> = None;
    for segment in inner.split('/').map(str::trim) {
        if !segment.chars().any(|c| c.is_ascii_alphabetic()) {
            continue;
        }

        if ascii_match.is_some() {
            return inner.to_string();
        }

        ascii_match = Some(segment);
    }

    ascii_match.unwrap_or(inner).to_string()
}

fn has_richer_later_bracket(s: &str) -> bool {
    RE_ANY_BRACKET_TITLE.captures_iter(s).any(|caps| {
        let inner = caps.get(1).unwrap().as_str().trim();
        inner.chars().any(|c| c.is_ascii_alphabetic())
            && (inner.contains(' ') || inner.contains('/') || inner.contains('×'))
            && !matches!(
                inner.to_ascii_lowercase().as_str(),
                "multiple subtitle" | "movie" | "avc" | "hevc" | "gb"
            )
    })
}

fn should_strip_non_english_prefix(s: &str) -> bool {
    RE_BRACKET_ALIAS_TITLE.is_match(s) || RE_SLASH_ALIAS_TITLE.is_match(s)
}

/// A bracketed token that looks like a release-group tag (`[GROUP]`,
/// `[Some-Group]`) rather than a real title.
fn looks_like_group(inner: &str) -> bool {
    !inner.contains(' ')
        && !inner.contains('/')
        && !inner.contains('×')
        && (inner.contains('-')
            || inner
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || "._".contains(c)))
}

/// A bracketed token that looks like technical metadata (`[1080p]`, `[AVC]`,
/// `[Multiple Subtitle]`) rather than a real title.
fn looks_like_metadata(inner: &str) -> bool {
    inner
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || " -_.+[]()".contains(c))
        || matches!(
            inner.to_ascii_lowercase().as_str(),
            "multiple subtitle" | "movie" | "avc" | "hevc" | "gb"
        )
}

/// Strip leading `[...]` bracket groups (e.g. anime fan-sub prefixes), promoting
/// a bracketed alias to the title when the bracket holds the only real name.
fn strip_leading_brackets(cleaned: String) -> String {
    static RE_LEADING_BRACKET: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^\s*\[([^\]]*)\]").unwrap());
    let mut rest = cleaned.clone();
    let mut bracket_title: Option<String> = None;

    while let Some(caps) = RE_LEADING_BRACKET.captures(&rest) {
        let full = caps.get(0).unwrap();
        let inner = caps.get(1).unwrap().as_str().trim();

        if inner.chars().any(|c| c.is_ascii_alphabetic())
            && !looks_like_group(inner)
            && !looks_like_metadata(inner)
        {
            if !inner.contains(' ')
                && !inner.contains('/')
                && !inner.contains('×')
                && rest[full.end()..]
                    .trim_start()
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic())
            {
                rest = rest[full.end()..].to_string();
                continue;
            }
            if !inner.contains(' ')
                && !inner.contains('/')
                && !inner.contains('×')
                && has_richer_later_bracket(&rest[full.end()..])
            {
                rest = rest[full.end()..].to_string();
                continue;
            }
            bracket_title = Some(normalize_bracket_candidate(inner));
            rest = rest[full.end()..].to_string();
            break;
        }

        rest = rest[full.end()..].to_string();
    }

    bracket_title.map_or(cleaned, |title| format!("{title} {rest}"))
}

/// Extract the human-readable title from a raw torrent name.
pub(crate) fn extract_title(raw: &str) -> String {
    let original_raw = raw;
    // Step 1: Clean the raw title - replace dots/underscores with spaces
    let raw = {
        static RE_HONORIFIC_DOT: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)\b(Mr|Mrs|Ms|Dr)\.\s").unwrap());
        RE_HONORIFIC_DOT
            .replace_all(raw, |caps: &regex::Captures| format!("{}§ ", &caps[1]))
            .to_string()
    };
    let cleaned = raw.replace(['_', '.'], " ");
    let cleaned = {
        static RE_PPV_TOKEN: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)(?:^|[\s\[(])PPV(?:$|[\s\])])").unwrap());
        RE_PPV_TOKEN.replace_all(&cleaned, " ").to_string()
    };
    let cleaned = {
        static RE_BRACKET_SITE_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(
                r"(?i)^\s*\[[^\]]*(?:(?:www?[.,]?\s*[\w-]+(?:[ .][\w-]+)*)|(?:[\w-]+\.[\w-]+(?:\.[\w-]+)?)|(?:[\w-]+\s+[\w-]+\s+[\w-]+))?[^\]]*\]\s*[-_]+\s*",
            )
            .unwrap()
        });
        RE_BRACKET_SITE_PREFIX.replace(&cleaned, "").to_string()
    };
    let cleaned = {
        static RE_ANY_BRACKET: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\[([^\]]*)\]").unwrap());
        let mut promoted: Option<String> = None;

        for caps in RE_ANY_BRACKET.captures_iter(&cleaned) {
            let full = caps.get(0).unwrap();
            let inner = caps.get(1).unwrap().as_str().trim();
            let prefix = &cleaned[..full.start()];

            if prefix.chars().all(|c| !c.is_ascii_alphabetic())
                && inner.chars().any(|c| c.is_ascii_alphabetic())
                && !looks_like_group(inner)
                && !looks_like_metadata(inner)
            {
                if !inner.contains(' ')
                    && !inner.contains('/')
                    && !inner.contains('×')
                    && cleaned[full.end()..]
                        .trim_start()
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_alphabetic())
                {
                    continue;
                }
                if !inner.contains(' ')
                    && !inner.contains('/')
                    && !inner.contains('×')
                    && has_richer_later_bracket(&cleaned[full.end()..])
                {
                    continue;
                }
                let inner = normalize_bracket_candidate(inner);
                promoted = Some(format!("{inner} {}", &cleaned[full.end()..]));
                break;
            }
        }

        promoted.unwrap_or(cleaned)
    };

    // Step 2: Remove bracket groups at the start (e.g., [SubGroup]) before
    // stripping non-Latin prefixes so anime group prefixes stay intact.
    let cleaned = strip_leading_brackets(cleaned);
    let cleaned = {
        static RE_BRACKET_START: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\s*\[[^\]]*\]\s*").unwrap());
        RE_BRACKET_START.replace(&cleaned, "").to_string()
    };

    // Step 3: Remove non-English character blocks from the beginning
    let cleaned = if should_strip_non_english_prefix(&cleaned) {
        RE_NON_ENGLISH_PREFIX.replace(&cleaned, "").to_string()
    } else {
        cleaned
    };
    let cleaned = strip_leading_brackets(cleaned);
    let cleaned = {
        static RE_BROKEN_GROUP_PREFIX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\s*[^\s\]]+\]\s*").unwrap());
        RE_BROKEN_GROUP_PREFIX.replace(&cleaned, "").to_string()
    };
    let cleaned = if let Some(first_ascii_alpha) = cleaned.find(|c: char| c.is_ascii_alphabetic()) {
        let first_metadata = min_match_start(
            &cleaned,
            &[
                &RE_YEAR,
                &RE_YEAR_RANGE,
                &RE_RES_3840,
                &RE_RES_1920,
                &RE_RES_1280,
                &RE_RES_QHD,
                &RE_RES_FHD,
                &RE_RES_PREFIXED_2160,
                &RE_RES_PREFIXED_1080,
                &RE_RES_PREFIXED_720,
                &RE_RES_PREFIXED_480,
                &RE_RES_GENERIC,
                &RE_TITLE_QUALITY,
                &RE_CODEC_AVC,
                &RE_CODEC_HEVC,
                &RE_CODEC_XVID,
                &RE_CODEC_AV1,
                &RE_AUDIO_DTS_LOSSLESS,
                &RE_AUDIO_DD_PLUS,
                &RE_AUDIO_DD,
                &RE_AUDIO_AAC,
            ],
        )
        .unwrap_or(cleaned.len());

        if first_ascii_alpha > 0
            && first_ascii_alpha < first_metadata
            && cleaned[..first_ascii_alpha]
                .chars()
                .any(|c| !c.is_ascii() && !c.is_whitespace())
        {
            cleaned[first_ascii_alpha..].trim_start().to_string()
        } else {
            cleaned
        }
    } else {
        cleaned
    };

    // Step 4: Remove site prefix if present
    let cleaned = if let Some(cap) = RE_SITE.captures(&cleaned) {
        cleaned[cap.get(0).unwrap().end()..].to_string()
    } else {
        cleaned
    };

    // Step 5: Find the earliest "marker" position
    let mut end = cleaned.len();

    // Check year — skip if it's at position 0 (year is part of the title, e.g. "2019 After...")
    for m in RE_YEAR.find_iter(&cleaned) {
        if m.start() > 0 {
            let tail = cleaned[m.end()..].trim_start();
            if tail.starts_with('(') {
                continue;
            }
            end = end.min(m.start());
            break;
        }
    }

    static RE_TITLE_INFO_PAREN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\([^)]*/[^)]*\)\s*(?:\[|$)").unwrap());
    static RE_EPISODE_RANGE_BRACKET: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\[\d{1,3}\s*-\s*\d{1,3}\]").unwrap());
    static RE_EPISODE_SINGLE_BRACKET: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\[\d{1,3}\]").unwrap());

    // Year range, info parens, resolution, season/episode indicators, codec,
    // audio, edition flags, collection markers, and embedded extensions all
    // unconditionally mark the end of the title — take the earliest.
    if let Some(start) = min_match_start(
        &cleaned,
        &[
            &RE_YEAR_RANGE,
            &RE_TITLE_INFO_PAREN,
            &RE_RES_3840,
            &RE_RES_1920,
            &RE_RES_1280,
            &RE_RES_QHD,
            &RE_RES_FHD,
            &RE_RES_PREFIXED_2160,
            &RE_RES_PREFIXED_1080,
            &RE_RES_PREFIXED_720,
            &RE_RES_PREFIXED_480,
            &RE_RES_GENERIC,
            &RE_RES_DIGITS,
            &RE_SEASON_SE,
            &RE_SEASON_FULL,
            &RE_SEASON_RANGE,
            &RE_SEASON_ORDINAL,
            &RE_SEASON_TR,
            &RE_EPISODE_STANDALONE,
            &RE_EPISODE_FULL,
            &RE_EPISODE_TR,
            &RE_EPISODE_RANGE_BARE,
            &RE_EPISODE_RANGE_PAREN,
            &RE_EPISODE_RANGE_BRACKET,
            &RE_EPISODE_SINGLE_BRACKET,
            &RE_EPISODE_CROSSREF,
        ],
    ) {
        end = end.min(start);
    }

    // Check quality markers
    if let Some(m) = RE_TITLE_QUALITY.find(&cleaned) {
        let matched = m.as_str().trim().to_ascii_lowercase();
        let tail = cleaned[m.end()..].trim_start();
        let web_before_year = matched == "web"
            && RE_YEAR
                .captures(tail)
                .is_some_and(|cap| cap.get(0).is_some_and(|m| m.start() == 0));

        if !web_before_year {
            end = end.min(m.start());
        }
    }
    {
        static RE_TITLE_METADATA_BRACKET: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"(?i)\[(?:movie|multiple subtitle|avc|hevc|aac|dual(?:[- ]audio)?|sub(?:title)?s?)\]").unwrap()
        });
        // Anime and scene names often use "Title - 05" for bare episode numbers.
        static RE_TITLE_DASH_EPISODE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\s-\s\d{1,3}\b").unwrap());
        if let Some(start) = min_match_start(
            &cleaned,
            &[
                &RE_TITLE_METADATA_BRACKET,
                &RE_COMPLETE,
                &RE_TITLE_DASH_EPISODE,
            ],
        ) {
            end = end.min(start);
        }
    }

    // Check network/service markers that commonly trail titles in scene names.
    {
        static RE_TITLE_NETWORK: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(
                r"(?i)\b(?:ATVP|ATV\+|Apple\s*TV\+?|AMZN|Amazon|NF|Netflix|NICK(?:elodeon)?|DSNP|DNSP|Disney\s*\+?|D\+|HMAX|HBO(?:\s*Max)?|HULU|PCOK|Peacock|PMTP|Paramount\+?|CBS|NBC|AMC|PBS|CC|Comedy\s*Central|CRAV|Crave|DCU|DC\s*Universe|DSNY|DisneyNOW|ESPN|FOX|FUNI|Funimation|RED|YouTube\s*(?:Red|Premium)|STAN|STZ|STARZ|SHO|Showtime|VRV|Crunchyroll|iT|iTunes|VUDU|ROKU|TVNZ|VICE|Sony|Hallmark|Adult\s*\.?\s*Swim|Animal\s*\.?\s*Planet|ANPL|Cartoon\s*\.?\s*Network)\b",
            )
            .unwrap()
        });
        if let Some(m) = RE_TITLE_NETWORK.find(&cleaned)
            && m.start() > 0
        {
            end = end.min(m.start());
        }
    }

    // Edition/flag, codec, audio, and embedded-extension markers all mark
    // the end of the title unconditionally.
    if let Some(start) = min_match_start(
        &cleaned,
        &[
            &RE_EDITION,
            &RE_UNRATED,
            &RE_UNCENSORED,
            &RE_EXTENDED,
            &RE_REMASTERED,
            &RE_PROPER,
            &RE_REPACK,
            &RE_DUBBED,
            &RE_HARDCODED,
            &RE_CODEC_AVC,
            &RE_CODEC_HEVC,
            &RE_CODEC_XVID,
            &RE_CODEC_AV1,
            &RE_AUDIO_DTS_LOSSLESS,
            &RE_AUDIO_DD_PLUS,
            &RE_AUDIO_DD,
            &RE_AUDIO_AAC,
            &RE_EXTENSION,
        ],
    ) {
        end = end.min(start);
    }
    // "Documentary" ends the title unless it's part of a phrase like
    // "Documentary in ...".
    if let Some(m) = RE_DOCUMENTARY.find(&cleaned) {
        let tail = cleaned[m.end()..].trim_start().to_ascii_lowercase();
        if !tail.starts_with("in ") {
            end = end.min(m.start());
        }
    }

    // Step 6: Extract and clean up
    let title = &cleaned[..end];
    let title = title.trim_start_matches(|c: char| c == '/' || c == '-' || c.is_whitespace());
    let title = {
        static RE_LEADING_YEAR_PAREN: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\(\d{4}\)\s*").unwrap());
        RE_LEADING_YEAR_PAREN.replace(title, "").to_string()
    };

    // Remove trailing dashes, parens, brackets, whitespace, and stray source tags
    let mut title = title
        .trim_end_matches(|c: char| matches!(c, '-' | '(' | '[' | ']') || c.is_whitespace())
        .to_string();
    while title.ends_with(')') && title.matches('(').count() < title.matches(')').count() {
        title.pop();
        title = title.trim_end().to_string();
    }
    let title = {
        static RE_TRAILING_SOURCE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)[\s(\[]*(?:BD|WEB|UHD|HD)\s*$").unwrap());
        RE_TRAILING_SOURCE.replace(&title, "").to_string()
    };
    let title = {
        static RE_TRAILING_EXTENSION_WORD: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"(?i)\s+(?:3g2|3gp|avi|flv|mkv|mk3d|mov|mp2|mp4|m4v|mpe|mpeg|mpg|mpv|webm|wmv|ogm|divx|ts|m2ts|iso|vob|sub|idx|ttxt|txt|smi|srt|ssa|ass|vtt|nfo|html|torrent)\s*$").unwrap()
        });
        RE_TRAILING_EXTENSION_WORD.replace(&title, "").to_string()
    };

    // Collapse whitespace and trim
    let title = title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .replace('§', ".");

    {
        static RE_LEADING_NUMERIC_SPACE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^(\d+)\s+(.*)$").unwrap());
        static RE_TITLE_WEB_YEAR: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)^\s*(.+?\bWeb)\s+(?:19|20)\d{2}\b").unwrap());

        let trimmed_original = original_raw.trim_start();
        let has_leading_numbered_dot = trimmed_original.find('.').is_some_and(|dot_idx| {
            dot_idx > 0
                && trimmed_original[..dot_idx]
                    .chars()
                    .all(|c| c.is_ascii_digit())
                && trimmed_original[dot_idx + 1..]
                    .chars()
                    .next()
                    .is_some_and(char::is_whitespace)
        });

        if has_leading_numbered_dot && let Some(caps) = RE_LEADING_NUMERIC_SPACE.captures(&title) {
            return format!("{}. {}", &caps[1], &caps[2]);
        }

        if let Some(caps) = RE_TITLE_WEB_YEAR.captures(trimmed_original) {
            let candidate = caps[1].trim();
            if candidate.len() > title.len()
                && candidate
                    .strip_suffix(" Web")
                    .is_some_and(|prefix| prefix == title)
            {
                return candidate.to_string();
            }
        }
    }

    title
}

/// Normalize a title for comparison (lowercase, no accents, no punctuation).
pub(crate) fn normalize_title(title: &str) -> String {
    let lower = title.to_lowercase();
    let no_accents = remove_accents(&lower);
    let replaced = no_accents.replace('&', " and ");

    // Remove 4-digit years (e.g. 1900-2099) to handle matching when one source excludes it
    static RE_YEAR_STRIP: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\b(19|20)\d{2}\b").unwrap());
    let no_year = RE_YEAR_STRIP.replace_all(&replaced, "").to_string();

    no_year
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Normalize edition strings to canonical forms.
pub(crate) fn normalize_edition(edition: &str) -> String {
    let lower = edition.to_lowercase();
    const TABLE: &[(&[&str], &str)] = &[
        (&["anniversary"], "Anniversary Edition"),
        (&["ultimate"], "Ultimate Edition"),
        (&["diamond"], "Diamond Edition"),
        (&["collector"], "Collectors Edition"),
        (&["special", "edition"], "Special Edition"),
        (&["director"], "Directors Cut"),
        (&["extended", "cut"], "Extended Cut"),
        (&["extended", "edition"], "Extended Edition"),
        (&["extended"], "Extended Edition"),
        (&["theatrical"], "Theatrical"),
        (&["uncut"], "Uncut"),
        (&["imax"], "IMAX"),
        (&["remaster"], "Remastered"),
        (&["criterion"], "Criterion Collection"),
        (&["final", "cut"], "Final Cut"),
        (&["limited"], "Limited Edition"),
        (&["deluxe"], "Deluxe Edition"),
    ];
    TABLE
        .iter()
        .find(|(keys, _)| keys.iter().all(|k| lower.contains(k)))
        .map_or_else(|| edition.to_string(), |(_, name)| name.to_string())
}

/// Remove accents / diacritics via manual mapping of common characters.
pub(crate) fn remove_accents(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ą' | 'Á' | 'À' | 'Â' | 'Ä' | 'Ã' | 'Å' | 'Ą' => {
                out.push('a')
            }
            'æ' | 'Æ' => out.push_str("ae"),
            'é' | 'è' | 'ê' | 'ë' | 'ę' | 'É' | 'È' | 'Ê' | 'Ë' | 'Ę' => out.push('e'),
            'í' | 'ì' | 'î' | 'ï' | 'Í' | 'Ì' | 'Î' | 'Ï' => out.push('i'),
            'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' | 'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' | 'Ø' => {
                out.push('o');
            }
            'ú' | 'ù' | 'û' | 'ü' | 'Ú' | 'Ù' | 'Û' | 'Ü' => out.push('u'),
            'ý' | 'ÿ' | 'Ý' | 'Ÿ' => out.push('y'),
            'ñ' | 'Ñ' | 'ň' | 'Ň' => out.push('n'),
            'ç' | 'Ç' | 'ć' | 'Ć' | 'č' | 'Č' => out.push('c'),
            'ð' | 'Ð' | 'đ' | 'Đ' => out.push('d'),
            'ß' => out.push_str("ss"),
            'ś' | 'Ś' | 'š' | 'Š' => out.push('s'),
            'ł' | 'Ł' => out.push('l'),
            'ž' | 'Ž' | 'ź' | 'Ź' | 'ż' | 'Ż' => out.push('z'),
            'ř' | 'Ř' => out.push('r'),
            'ť' | 'Ť' => out.push('t'),
            'þ' | 'Þ' => out.push_str("th"),
            _ => out.push(c),
        }
    }
    out
}
