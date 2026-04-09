use regex::Regex;
use std::sync::LazyLock;

use super::patterns::*;

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
            let looks_like_group = !inner.contains(' ')
                && !inner.contains('/')
                && !inner.contains('×')
                && (inner.contains('-')
                    || inner
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || "._".contains(c)));
            let looks_like_metadata = inner
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || " -_.+[]()".contains(c))
                || matches!(
                    inner.to_ascii_lowercase().as_str(),
                    "multiple subtitle" | "movie" | "avc" | "hevc" | "gb"
                );

            if prefix.chars().all(|c| !c.is_ascii_alphabetic())
                && inner.chars().any(|c| c.is_ascii_alphabetic())
                && !looks_like_group
                && !looks_like_metadata
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
    let cleaned = {
        static RE_LEADING_BRACKET: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\s*\[([^\]]*)\]").unwrap());
        let mut rest = cleaned.clone();
        let mut bracket_title: Option<String> = None;

        while let Some(caps) = RE_LEADING_BRACKET.captures(&rest) {
            let full = caps.get(0).unwrap();
            let inner = caps.get(1).unwrap().as_str().trim();

            let looks_like_group = !inner.contains(' ')
                && !inner.contains('/')
                && !inner.contains('×')
                && (inner.contains('-')
                    || inner
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || "._".contains(c)));
            let looks_like_metadata = inner
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || " -_.+[]()".contains(c))
                || matches!(
                    inner.to_ascii_lowercase().as_str(),
                    "multiple subtitle" | "movie" | "avc" | "hevc" | "gb"
                );

            if inner.chars().any(|c| c.is_ascii_alphabetic())
                && !looks_like_group
                && !looks_like_metadata
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

        if let Some(title) = bracket_title {
            format!("{title} {rest}")
        } else {
            cleaned
        }
    };
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
    let cleaned = {
        static RE_LEADING_BRACKET: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\s*\[([^\]]*)\]").unwrap());
        let mut rest = cleaned.clone();
        let mut bracket_title: Option<String> = None;

        while let Some(caps) = RE_LEADING_BRACKET.captures(&rest) {
            let full = caps.get(0).unwrap();
            let inner = caps.get(1).unwrap().as_str().trim();

            let looks_like_group = !inner.contains(' ')
                && !inner.contains('/')
                && !inner.contains('×')
                && (inner.contains('-')
                    || inner
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || "._".contains(c)));
            let looks_like_metadata = inner
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || " -_.+[]()".contains(c))
                || matches!(
                    inner.to_ascii_lowercase().as_str(),
                    "multiple subtitle" | "movie" | "avc" | "hevc" | "gb"
                );

            if inner.chars().any(|c| c.is_ascii_alphabetic())
                && !looks_like_group
                && !looks_like_metadata
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

        if let Some(title) = bracket_title {
            format!("{title} {rest}")
        } else {
            cleaned
        }
    };
    let cleaned = {
        static RE_BROKEN_GROUP_PREFIX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\s*[^\s\]]+\]\s*").unwrap());
        RE_BROKEN_GROUP_PREFIX.replace(&cleaned, "").to_string()
    };
    let cleaned = if let Some(first_ascii_alpha) = cleaned.find(|c: char| c.is_ascii_alphabetic()) {
        let mut first_metadata = cleaned.len();
        for re in [
            &*RE_YEAR,
            &*RE_YEAR_RANGE,
            &*RE_RES_3840,
            &*RE_RES_1920,
            &*RE_RES_1280,
            &*RE_RES_QHD,
            &*RE_RES_FHD,
            &*RE_RES_PREFIXED_2160,
            &*RE_RES_PREFIXED_1080,
            &*RE_RES_PREFIXED_720,
            &*RE_RES_PREFIXED_480,
            &*RE_RES_GENERIC,
            &*RE_TITLE_QUALITY,
            &*RE_CODEC_AVC,
            &*RE_CODEC_HEVC,
            &*RE_CODEC_XVID,
            &*RE_CODEC_AV1,
            &*RE_AUDIO_DTS_LOSSLESS,
            &*RE_AUDIO_DD_PLUS,
            &*RE_AUDIO_DD,
            &*RE_AUDIO_AAC,
        ] {
            if let Some(m) = re.find(&cleaned) {
                first_metadata = first_metadata.min(m.start());
            }
        }

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

    // Check year range
    if let Some(m) = RE_YEAR_RANGE.find(&cleaned) {
        end = end.min(m.start());
    }
    {
        static RE_TITLE_INFO_PAREN: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\([^)]*/[^)]*\)\s*(?:\[|$)").unwrap());
        if let Some(m) = RE_TITLE_INFO_PAREN.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Check resolution patterns
    for re in [
        &*RE_RES_3840,
        &*RE_RES_1920,
        &*RE_RES_1280,
        &*RE_RES_QHD,
        &*RE_RES_FHD,
        &*RE_RES_PREFIXED_2160,
        &*RE_RES_PREFIXED_1080,
        &*RE_RES_PREFIXED_720,
        &*RE_RES_PREFIXED_480,
        &*RE_RES_GENERIC,
        &*RE_RES_DIGITS,
    ] {
        if let Some(m) = re.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Check season indicators
    for re in [
        &*RE_SEASON_SE,
        &*RE_SEASON_FULL,
        &*RE_SEASON_RANGE,
        &*RE_SEASON_ORDINAL,
        &*RE_SEASON_TR,
    ] {
        if let Some(m) = re.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Check episode indicators (standalone E##)
    if let Some(m) = RE_EPISODE_STANDALONE.find(&cleaned) {
        end = end.min(m.start());
    }
    if let Some(m) = RE_EPISODE_FULL.find(&cleaned) {
        end = end.min(m.start());
    }
    if let Some(m) = RE_EPISODE_TR.find(&cleaned) {
        end = end.min(m.start());
    }
    if let Some(m) = RE_EPISODE_RANGE_BARE.find(&cleaned) {
        end = end.min(m.start());
    }
    if let Some(m) = RE_EPISODE_RANGE_PAREN.find(&cleaned) {
        end = end.min(m.start());
    }
    {
        static RE_EPISODE_RANGE_BRACKET: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\[\d{1,3}\s*-\s*\d{1,3}\]").unwrap());
        if let Some(m) = RE_EPISODE_RANGE_BRACKET.find(&cleaned) {
            end = end.min(m.start());
        }
    }
    {
        static RE_EPISODE_SINGLE_BRACKET: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\[\d{1,3}\]").unwrap());
        if let Some(m) = RE_EPISODE_SINGLE_BRACKET.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Check #x## format
    if let Some(m) = RE_EPISODE_CROSSREF.find(&cleaned) {
        end = end.min(m.start());
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
        if let Some(m) = RE_TITLE_METADATA_BRACKET.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Check complete-collection markers that should not remain in the title.
    for re in [&*RE_COMPLETE] {
        if let Some(m) = re.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Anime and scene names often use "Title - 05" for bare episode numbers.
    {
        static RE_TITLE_DASH_EPISODE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\s-\s\d{1,3}\b").unwrap());
        if let Some(m) = RE_TITLE_DASH_EPISODE.find(&cleaned) {
            end = end.min(m.start());
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
        if let Some(m) = RE_TITLE_NETWORK.find(&cleaned) {
            if m.start() > 0 {
                end = end.min(m.start());
            }
        }
    }

    // Check edition/flag markers — these mark the end of the title
    for re in [
        &*RE_EDITION,
        &*RE_UNRATED,
        &*RE_UNCENSORED,
        &*RE_EXTENDED,
        &*RE_REMASTERED,
        &*RE_PROPER,
        &*RE_REPACK,
        &*RE_DUBBED,
        &*RE_HARDCODED,
    ] {
        if let Some(m) = re.find(&cleaned) {
            end = end.min(m.start());
        }
    }
    if let Some(m) = RE_DOCUMENTARY.find(&cleaned) {
        let tail = cleaned[m.end()..].trim_start().to_ascii_lowercase();
        if !tail.starts_with("in ") {
            end = end.min(m.start());
        }
    }

    // Check codec markers
    for re in [
        &*RE_CODEC_AVC,
        &*RE_CODEC_HEVC,
        &*RE_CODEC_XVID,
        &*RE_CODEC_AV1,
    ] {
        if let Some(m) = re.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Check audio markers
    for re in [
        &*RE_AUDIO_DTS_LOSSLESS,
        &*RE_AUDIO_DD_PLUS,
        &*RE_AUDIO_DD,
        &*RE_AUDIO_AAC,
    ] {
        if let Some(m) = re.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Check extension markers if they are still embedded in the candidate title.
    if let Some(m) = RE_EXTENSION.find(&cleaned) {
        end = end.min(m.start());
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
        .replace('§', ".")
        .to_string();

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

        if has_leading_numbered_dot {
            if let Some(caps) = RE_LEADING_NUMERIC_SPACE.captures(&title) {
                return format!("{}. {}", &caps[1], &caps[2]);
            }
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
    s.chars()
        .flat_map(|c| match c {
            'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ą' => vec!['a'],
            'Á' | 'À' | 'Â' | 'Ä' | 'Ã' | 'Å' | 'Ą' => vec!['a'],
            'é' | 'è' | 'ê' | 'ë' | 'ę' => vec!['e'],
            'É' | 'È' | 'Ê' | 'Ë' | 'Ę' => vec!['e'],
            'í' | 'ì' | 'î' | 'ï' => vec!['i'],
            'Í' | 'Ì' | 'Î' | 'Ï' => vec!['i'],
            'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' => vec!['o'],
            'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' | 'Ø' => vec!['o'],
            'ú' | 'ù' | 'û' | 'ü' => vec!['u'],
            'Ú' | 'Ù' | 'Û' | 'Ü' => vec!['u'],
            'ý' | 'ÿ' => vec!['y'],
            'Ý' | 'Ÿ' => vec!['y'],
            'ñ' | 'Ñ' => vec!['n'],
            'ç' | 'Ç' | 'ć' | 'Ć' | 'č' | 'Č' => vec!['c'],
            'ð' | 'Ð' => vec!['d'],
            'ß' => vec!['s', 's'],
            'ś' | 'Ś' | 'š' | 'Š' => vec!['s'],
            'ł' | 'Ł' => vec!['l'],
            'ž' | 'Ž' | 'ź' | 'Ź' | 'ż' | 'Ż' => vec!['z'],
            'ř' | 'Ř' => vec!['r'],
            'ť' | 'Ť' => vec!['t'],
            'ň' | 'Ň' => vec!['n'],
            'đ' | 'Đ' => vec!['d'],
            'æ' | 'Æ' => vec!['a', 'e'],
            'þ' | 'Þ' => vec!['t', 'h'],
            _ => vec![c],
        })
        .collect()
}
