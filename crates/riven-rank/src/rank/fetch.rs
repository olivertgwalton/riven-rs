use std::collections::HashSet;

use regex::Regex;

use crate::parse::ParsedData;
use crate::settings::RankSettings;

const TRASH_QUALITIES: &[&str] = &["CAM", "PDTV", "R5", "SCR", "TeleCine", "TeleSync"];

fn trash_handler(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    if !settings.options.remove_all_trash {
        return true;
    }
    if data.quality.as_ref().map_or(false, |q| {
        TRASH_QUALITIES.iter().any(|tq| q.eq_ignore_ascii_case(tq))
    }) {
        failed.push("trash_quality".into());
        return false;
    }
    if data.audio.iter().any(|a| a == "HQ Clean Audio") {
        failed.push("trash_audio".into());
        return false;
    }
    if data.trash {
        failed.push("trash_flag".into());
        return false;
    }
    true
}

fn adult_handler(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    if data.adult && settings.options.remove_adult_content {
        failed.push("trash_adult".into());
        return false;
    }
    true
}

fn check_required(data: &ParsedData, settings: &RankSettings) -> Option<bool> {
    if settings.require.is_empty() {
        return None;
    }
    if settings
        .require
        .iter()
        .filter_map(|p| Regex::new(p).ok())
        .any(|re| re.is_match(&data.raw_title))
    {
        Some(true)
    } else {
        None
    }
}

fn check_exclude(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    if settings
        .exclude
        .iter()
        .filter_map(|p| Regex::new(p).ok())
        .any(|re| re.is_match(&data.raw_title))
    {
        failed.push("excluded_pattern".into());
        return false;
    }
    true
}

const LANG_ANIME: &[&str]     = &["ja", "zh", "ko"];
const LANG_NON_ANIME: &[&str] = &[
    "de", "es", "hi", "ta", "ru", "ua", "th", "it", "ar", "pt", "fr", "pl", "nl", "sv",
    "no", "da", "fi", "tr", "cs", "hu", "ro", "bg", "hr", "sr", "sk", "sl", "el", "he",
    "id", "ms", "vi", "bn", "fa", "uk", "ca", "eu",
];
const LANG_COMMON: &[&str]    = &["de", "es", "hi", "ta", "ru", "ua", "th", "it", "zh", "ar", "fr"];

fn add_langs(set: &mut HashSet<String>, codes: &[&str]) {
    set.extend(codes.iter().copied().map(String::from));
}

/// Expand language group names into individual language codes.
fn populate_langs(langs: &[String]) -> HashSet<String> {
    let mut result = HashSet::new();
    for lang in langs {
        match lang.to_lowercase().as_str() {
            "anime"     => add_langs(&mut result, LANG_ANIME),
            "non_anime" => add_langs(&mut result, LANG_NON_ANIME),
            "common"    => add_langs(&mut result, LANG_COMMON),
            "all"       => { add_langs(&mut result, LANG_ANIME); add_langs(&mut result, LANG_NON_ANIME); }
            other       => { result.insert(other.to_string()); }
        }
    }
    result
}

fn language_handler(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let required = populate_langs(&settings.languages.required);
    let allowed = populate_langs(&settings.languages.allowed);
    let excluded = populate_langs(&settings.languages.exclude);
    let torrent_langs: HashSet<String> = data.languages.iter().cloned().collect();

    if torrent_langs.is_empty() && settings.options.remove_unknown_languages {
        failed.push("unknown_language".into());
        return false;
    }
    if torrent_langs.is_empty() && !required.is_empty() {
        failed.push("missing_required_language".into());
        return false;
    }
    if !required.is_empty() && !torrent_langs.iter().any(|l| required.contains(l)) {
        failed.push("missing_required_language".into());
        return false;
    }
    if torrent_langs.contains("en") && settings.options.allow_english_in_languages {
        return true;
    }
    if !allowed.is_empty() && torrent_langs.iter().any(|l| allowed.contains(l)) {
        return true;
    }
    if !excluded.is_empty() && torrent_langs.iter().any(|l| excluded.contains(l)) {
        failed.push("excluded_language".into());
        return false;
    }
    true
}

fn fetch_resolution(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let enabled = match data.resolution.as_str() {
        "2160p" => settings.resolutions.r2160p,
        "1080p" => settings.resolutions.r1080p,
        "720p" => settings.resolutions.r720p,
        "480p" => settings.resolutions.r480p,
        "360p" => settings.resolutions.r360p,
        _ => settings.resolutions.unknown,
    };
    if !enabled {
        failed.push("resolution".into());
        return false;
    }
    true
}

fn fetch_quality(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let q = match data.quality.as_deref() {
        Some(q) => q,
        None => return true,
    };
    if let Some(cr) = settings.custom_ranks.quality_rank(q) {
        if !cr.fetch {
            failed.push("quality".into());
            return false;
        }
    }
    true
}

fn fetch_audio(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    for a in &data.audio {
        if let Some(cr) = settings.custom_ranks.audio_rank(a) {
            if !cr.fetch {
                failed.push(format!("audio_{a}"));
                return false;
            }
        }
    }
    true
}

fn fetch_hdr(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let cr = &settings.custom_ranks;
    for h in &data.hdr {
        if let Some(rank) = cr.hdr_rank(h) {
            if !rank.fetch {
                failed.push(format!("hdr_{h}"));
                return false;
            }
        }
    }
    if data.bit_depth.is_some() && !cr.hdr.bit10.fetch {
        failed.push("hdr_bit10".into());
        return false;
    }
    true
}

fn fetch_codec(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let codec = match data.codec.as_deref() {
        Some(c) => c,
        None => return true,
    };
    if let Some(cr) = settings.custom_ranks.codec_rank(codec) {
        if !cr.fetch {
            failed.push(format!("codec_{codec}"));
            return false;
        }
    }
    true
}

fn fetch_other(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let cr = &settings.custom_ranks;
    let mut ok = true;

    let checks: &[(bool, &crate::settings::CustomRank, &str)] = &[
        (data.three_d, &cr.extras.three_d, "three_d"),
        (data.converted, &cr.extras.converted, "converted"),
        (data.documentary, &cr.extras.documentary, "documentary"),
        (data.dubbed, &cr.extras.dubbed, "dubbed"),
        (data.edition.is_some(), &cr.extras.edition, "edition"),
        (data.hardcoded, &cr.extras.hardcoded, "hardcoded"),
        (data.network.is_some(), &cr.extras.network, "network"),
        (data.proper, &cr.extras.proper, "proper"),
        (data.repack, &cr.extras.repack, "repack"),
        (data.retail, &cr.extras.retail, "retail"),
        (data.subbed, &cr.extras.subbed, "subbed"),
        (data.upscaled, &cr.extras.upscaled, "upscaled"),
        (data.site.is_some(), &cr.extras.site, "site"),
        (data.scene, &cr.extras.scene, "scene"),
        (data.uncensored, &cr.extras.uncensored, "uncensored"),
        (data.size.is_some(), &cr.trash.size, "size"),
    ];

    for &(cond, rank, name) in checks {
        if cond && !rank.fetch {
            failed.push(name.into());
            ok = false;
        }
    }
    ok
}

/// Check whether the torrent should be fetched based on settings.
pub fn check_fetch(data: &ParsedData, settings: &RankSettings) -> (bool, Vec<String>) {
    let mut failed = Vec::new();

    // Fail-fast pipeline
    let handlers: &[fn(&ParsedData, &RankSettings, &mut Vec<String>) -> bool] = &[
        trash_handler,
        adult_handler,
    ];
    for handler in handlers {
        if !handler(data, settings, &mut failed) {
            return (false, failed);
        }
    }

    // Required pattern bypass
    if let Some(true) = check_required(data, settings) {
        return (true, failed);
    }

    // Remaining checks — all fail-fast
    let checks: &[fn(&ParsedData, &RankSettings, &mut Vec<String>) -> bool] = &[
        check_exclude,
        language_handler,
        fetch_resolution,
        fetch_quality,
        fetch_audio,
        fetch_hdr,
        fetch_codec,
        fetch_other,
    ];
    for check in checks {
        if !check(data, settings, &mut failed) {
            return (false, failed);
        }
    }

    (failed.is_empty(), failed)
}
