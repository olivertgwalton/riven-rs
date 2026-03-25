use regex::Regex;
use std::sync::LazyLock;

use super::patterns::*;

/// Extract the human-readable title from a raw torrent name.
pub(crate) fn extract_title(raw: &str) -> String {
    // Step 1: Clean the raw title - replace dots/underscores with spaces
    let cleaned = raw.replace('_', " ").replace('.', " ");

    // Step 2: Remove non-English character blocks from the beginning
    let cleaned = RE_NON_ENGLISH_PREFIX.replace(&cleaned, "").to_string();

    // Step 3: Remove site prefix if present
    let cleaned = if let Some(cap) = RE_SITE.captures(&cleaned) {
        cleaned[cap.get(0).unwrap().end()..].to_string()
    } else {
        cleaned
    };

    // Step 4: Remove bracket groups at the start (e.g., [SubGroup])
    let cleaned = {
        static RE_BRACKET_START: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\s*\[[^\]]*\]\s*").unwrap());
        RE_BRACKET_START.replace(&cleaned, "").to_string()
    };

    // Step 5: Find the earliest "marker" position
    let mut end = cleaned.len();

    // Check year
    if let Some(m) = RE_YEAR.find(&cleaned) {
        end = end.min(m.start());
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
    for re in [&*RE_SEASON_SE, &*RE_SEASON_FULL, &*RE_SEASON_RANGE] {
        if let Some(m) = re.find(&cleaned) {
            end = end.min(m.start());
        }
    }

    // Check episode indicators (standalone E##)
    if let Some(m) = RE_EPISODE_STANDALONE.find(&cleaned) {
        end = end.min(m.start());
    }

    // Check #x## format
    if let Some(m) = RE_EPISODE_CROSSREF.find(&cleaned) {
        end = end.min(m.start());
    }

    // Check quality markers
    if let Some(m) = RE_TITLE_QUALITY.find(&cleaned) {
        end = end.min(m.start());
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

    // Step 6: Extract and clean up
    let title = &cleaned[..end];

    // Remove trailing dashes, parens, brackets
    let title =
        title.trim_end_matches(|c: char| c == '-' || c == '(' || c == '[' || c.is_whitespace());

    // Collapse whitespace and trim
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

/// Normalize a title for comparison (lowercase, no accents, no punctuation).
pub(crate) fn normalize_title(title: &str) -> String {
    let lower = title.to_lowercase();
    let no_accents = remove_accents(&lower);
    let replaced = no_accents.replace('&', " and ");
    
    // Remove 4-digit years (e.g. 1900-2099) to handle matching when one source excludes it
    static RE_YEAR_STRIP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b(19|20)\d{2}\b").unwrap());
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
    if lower.contains("anniversary") {
        "Anniversary Edition".to_string()
    } else if lower.contains("ultimate") {
        "Ultimate Edition".to_string()
    } else if lower.contains("diamond") {
        "Diamond Edition".to_string()
    } else if lower.contains("collector") {
        "Collectors Edition".to_string()
    } else if lower.contains("special") && lower.contains("edition") {
        "Special Edition".to_string()
    } else if lower.contains("director") {
        "Directors Cut".to_string()
    } else if lower.contains("extended") && lower.contains("cut") {
        "Extended Cut".to_string()
    } else if lower.contains("extended") && lower.contains("edition") {
        "Extended Edition".to_string()
    } else if lower.contains("theatrical") {
        "Theatrical".to_string()
    } else if lower.contains("uncut") {
        "Uncut".to_string()
    } else if lower.contains("imax") {
        "IMAX".to_string()
    } else if lower.contains("remaster") {
        "Remastered".to_string()
    } else if lower.contains("criterion") {
        "Criterion Collection".to_string()
    } else if lower.contains("final") && lower.contains("cut") {
        "Final Cut".to_string()
    } else if lower.contains("limited") {
        "Limited Edition".to_string()
    } else if lower.contains("deluxe") {
        "Deluxe Edition".to_string()
    } else {
        edition.to_string()
    }
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
