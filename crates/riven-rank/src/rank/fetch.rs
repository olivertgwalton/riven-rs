use std::collections::HashSet;

use crate::parse::ParsedData;
use crate::settings::RankSettings;

const TRASH_QUALITIES: &[&str] = &["CAM", "PDTV", "R5", "SCR", "TeleCine", "TeleSync"];

fn trash_handler(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    if !settings.options.trash.remove_all_trash {
        return true;
    }
    if data
        .quality
        .as_ref()
        .is_some_and(|q| TRASH_QUALITIES.iter().any(|tq| q.eq_ignore_ascii_case(tq)))
    {
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
    if data.adult && settings.options.content.remove_adult_content {
        failed.push("trash_adult".into());
        return false;
    }
    true
}

/// Returns `true` if a required pattern matches the raw title.
///   `fetch: failed.size === 0 || checkRequired(data, settings)`
fn required_matches(data: &ParsedData, settings: &RankSettings) -> bool {
    if settings.require.is_empty() {
        return false;
    }
    if settings.require_compiled.is_empty() {
        debug_assert!(
            false,
            "RankSettings::prepare() was not called — regex compiled per-torrent"
        );
        settings
            .require
            .iter()
            .filter_map(|p| regex::Regex::new(p).ok())
            .any(|re| re.is_match(&data.raw_title))
    } else {
        settings
            .require_compiled
            .iter()
            .any(|re| re.is_match(&data.raw_title))
    }
}

fn check_exclude(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let excluded = if settings.exclude_compiled.is_empty() {
        settings
            .exclude
            .iter()
            .filter_map(|p| regex::Regex::new(p).ok())
            .any(|re| re.is_match(&data.raw_title))
    } else {
        settings
            .exclude_compiled
            .iter()
            .any(|re| re.is_match(&data.raw_title))
    };
    if excluded {
        failed.push("excluded_pattern".into());
        return false;
    }
    true
}

const LANG_ANIME: &[&str] = &["ja", "zh", "ko"];
const LANG_NON_ANIME: &[&str] = &[
    "de", "es", "hi", "ta", "ru", "ua", "th", "it", "ar", "pt", "fr", "pa", "mr", "gu", "te", "kn",
    "ml", "vi", "id", "ms", "bn", "tr", "he", "fa", "el", "lt", "lv", "et", "pl", "cs", "sk", "hu",
    "ro", "bg", "sr", "hr", "sl", "nl", "da", "fi", "sv", "no", "uk", "ca", "eu",
];
const LANG_COMMON: &[&str] = &[
    "de", "es", "hi", "ta", "ru", "ua", "th", "it", "zh", "ar", "fr",
];

fn add_langs(set: &mut HashSet<String>, codes: &[&str]) {
    set.extend(codes.iter().copied().map(String::from));
}

/// Expand language group names into individual language codes.
fn populate_langs(langs: &[String]) -> HashSet<String> {
    let mut result = HashSet::new();
    for lang in langs {
        match lang.to_lowercase().as_str() {
            "anime" => add_langs(&mut result, LANG_ANIME),
            "non_anime" => add_langs(&mut result, LANG_NON_ANIME),
            "common" => add_langs(&mut result, LANG_COMMON),
            "all" => {
                add_langs(&mut result, LANG_ANIME);
                add_langs(&mut result, LANG_NON_ANIME);
            }
            other => {
                result.insert(other.to_string());
            }
        }
    }
    result
}

fn language_handler(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let required = populate_langs(&settings.languages.required);
    let allowed = populate_langs(&settings.languages.allowed);
    let excluded = populate_langs(&settings.languages.exclude);
    let torrent_langs: HashSet<String> = data.languages.iter().cloned().collect();

    if torrent_langs.is_empty() && settings.options.language.remove_unknown_languages {
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
    if torrent_langs.contains("en") && settings.options.language.allow_english_in_languages {
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
    let enabled = settings.resolutions.allows(&data.resolution);
    if !enabled {
        failed.push("resolution".into());
        return false;
    }
    true
}

fn fetch_quality(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let Some(q) = data.quality.as_deref() else {
        if settings.options.quality.remove_unknown_quality {
            failed.push("unknown_quality".into());
            return false;
        }
        return true;
    };
    if let Some(cr) = settings.custom_ranks.quality_rank(q)
        && !cr.fetch
    {
        failed.push("quality".into());
        return false;
    }
    true
}

fn fetch_audio(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    for a in &data.audio {
        if let Some(cr) = settings.custom_ranks.audio_rank(a)
            && !cr.fetch
        {
            failed.push(format!("audio_{a}"));
            return false;
        }
    }
    true
}

fn fetch_hdr(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let cr = &settings.custom_ranks;
    for h in &data.hdr {
        if let Some(rank) = cr.hdr_rank(h)
            && !rank.fetch
        {
            failed.push(format!("hdr_{h}"));
            return false;
        }
    }
    if data.bit_depth.is_some() && !cr.hdr.bit10.fetch {
        failed.push("hdr_bit10".into());
        return false;
    }
    true
}

fn fetch_codec(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let Some(codec) = data.codec.as_deref() else {
        return true;
    };
    if let Some(cr) = settings.custom_ranks.codec_rank(codec)
        && !cr.fetch
    {
        failed.push(format!("codec_{codec}"));
        return false;
    }
    true
}

fn fetch_other(data: &ParsedData, settings: &RankSettings, failed: &mut Vec<String>) -> bool {
    let cr = &settings.custom_ranks;
    let speed = settings.options.fetch.enable_fetch_speed_mode;

    let checks: &[(bool, &crate::settings::CustomRank, &str)] = &[
        (data.three_d, &cr.extras.three_d, "three_d"),
        (data.converted, &cr.extras.converted, "converted"),
        (data.commentary, &cr.extras.commentary, "commentary"),
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

    let initial_len = failed.len();
    for &(cond, rank, name) in checks {
        if cond && !rank.fetch {
            failed.push(name.into());
            if speed {
                return false;
            }
        }
    }
    failed.len() == initial_len
}

/// Check whether the torrent should be fetched based on settings.
///
///   `fetch: failed.size === 0 || checkRequired(data, settings)`
///
/// Required patterns are checked **last** as an unconditional override — a
/// matching `require` pattern accepts the torrent even if other checks failed.
///
/// When `options.fetch.enable_fetch_speed_mode` is `true` (default) the pipeline
/// short-circuits on the first failure, but still applies the required-pattern
/// override before returning. When `false`, all checks run and every failure
/// reason is collected — useful for diagnostics.
#[must_use]
pub fn check_fetch(data: &ParsedData, settings: &RankSettings) -> (bool, Vec<String>) {
    let mut failed = Vec::new();
    let speed = settings.options.fetch.enable_fetch_speed_mode;

    if speed {
        if !trash_handler(data, settings, &mut failed) {
            return (false, failed);
        }
        if !adult_handler(data, settings, &mut failed) {
            return (false, failed);
        }
        if required_matches(data, settings) {
            return (true, failed);
        }
    }

    macro_rules! run {
        ($fn:expr) => {{
            let ok = $fn(data, settings, &mut failed);
            if speed && !ok {
                return (required_matches(data, settings), failed);
            }
        }};
    }

    run!(trash_handler);
    run!(adult_handler);
    run!(check_exclude);
    run!(language_handler);
    run!(fetch_resolution);
    run!(fetch_quality);
    run!(fetch_audio);
    run!(fetch_hdr);
    run!(fetch_codec);
    run!(fetch_other);

    (
        failed.is_empty() || required_matches(data, settings),
        failed,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse;

    /// Reproduces a real incident: "Spider-Man: Brand New Day" (still
    /// theatrical-only) had a fake/camrip release accepted because its
    /// filename had no recognizable quality tag (`SpiderMan-BrandNewDay-1080p-
    /// English.mp4`), and an unparseable quality tag was treated as
    /// automatically acceptable rather than suspicious.
    #[test]
    fn unknown_quality_passes_by_default_but_can_be_rejected() {
        let data = parse("SpiderMan-BrandNewDay-1080p-English.mp4");
        assert!(data.quality.is_none());

        let mut permissive = RankSettings::default();
        permissive.options.quality.remove_unknown_quality = false;
        let (ok, failed) = check_fetch(&data, &permissive);
        assert!(ok, "unknown quality should pass when the toggle is off");
        assert!(!failed.contains(&"unknown_quality".to_string()));

        let mut strict = RankSettings::default();
        strict.options.quality.remove_unknown_quality = true;
        let (ok, failed) = check_fetch(&data, &strict);
        assert!(
            !ok,
            "unknown quality should be rejected when the toggle is on"
        );
        assert!(failed.contains(&"unknown_quality".to_string()));
    }

    /// A release with a recognized quality tag is unaffected by the toggle.
    #[test]
    fn known_quality_is_unaffected_by_the_unknown_quality_toggle() {
        let data = parse("The.Odyssey.2026.1080p.WEB-DL.x264-GROUP.mkv");
        assert_eq!(data.quality.as_deref(), Some("WEB-DL"));

        let mut strict = RankSettings::default();
        strict.options.quality.remove_unknown_quality = true;
        let (ok, failed) = check_fetch(&data, &strict);
        assert!(ok, "a recognized quality tag must not be rejected");
        assert!(!failed.contains(&"unknown_quality".to_string()));
    }
}
