use crate::defaults::DEFAULT_SCORES;
use crate::parse::ParsedData;
use crate::settings::{CustomRank, CustomRanksConfig, RankSettings};

/// `rank`'s custom score, falling back to the built-in `default`.
fn resolve(rank: &CustomRank, default: &CustomRank) -> i64 {
    rank.resolve(default.rank.unwrap_or(0))
}

/// Resolve a looked-up rank against the same lookup on the built-in defaults;
/// 0 when the lookup does not recognise the value.
fn resolve_lookup<'a>(
    settings: &'a RankSettings,
    lookup: impl Fn(&'a CustomRanksConfig) -> Option<&'a CustomRank>,
) -> i64 {
    lookup(&settings.custom_ranks)
        .zip(lookup(&DEFAULT_SCORES))
        .map_or(0, |(rank, default)| resolve(rank, default))
}

/// The boolean extras flags with the rank that applies to each, as
/// `(present, rank, settings key)`. Shared by scoring and fetch checks.
pub(super) fn extras<'a>(
    data: &ParsedData,
    cr: &'a CustomRanksConfig,
) -> [(bool, &'a CustomRank, &'static str); 17] {
    [
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
    ]
}

fn calculate_quality_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let Some(q) = data.quality.as_deref() else {
        return 0;
    };
    resolve_lookup(settings, |cr| cr.quality_rank(q))
}

fn calculate_codec_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let Some(codec) = data.codec.as_deref() else {
        return 0;
    };
    resolve_lookup(settings, |cr| cr.codec_rank(codec))
}

fn calculate_hdr_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let mut score: i64 = data
        .hdr
        .iter()
        .map(|h| resolve_lookup(settings, |cr| cr.hdr_rank(h)))
        .sum();
    if data.bit_depth.is_some() {
        score += resolve(&settings.custom_ranks.hdr.bit10, &DEFAULT_SCORES.hdr.bit10);
    }
    score
}

fn calculate_audio_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    data.audio
        .iter()
        .map(|a| resolve_lookup(settings, |cr| cr.audio_rank(a)))
        .sum()
}

fn calculate_channels_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let cr = &settings.custom_ranks.audio;
    let d = &DEFAULT_SCORES.audio;
    data.channels
        .iter()
        .map(|c| match c.as_str() {
            "5.1" | "7.1" => resolve(&cr.surround, &d.surround),
            "stereo" | "2.0" => resolve(&cr.stereo, &d.stereo),
            "mono" => resolve(&cr.mono, &d.mono),
            _ => 0,
        })
        .sum()
}

fn calculate_extra_ranks(data: &ParsedData, settings: &RankSettings) -> i64 {
    extras(data, &settings.custom_ranks)
        .into_iter()
        .zip(extras(data, &DEFAULT_SCORES))
        .filter(|((present, _, _), _)| *present)
        .map(|((_, rank, _), (_, default, _))| resolve(rank, default))
        .sum()
}

fn calculate_preferred(data: &ParsedData, settings: &RankSettings) -> i64 {
    if settings
        .preferred_compiled
        .iter()
        .any(|re| re.is_match(&data.raw_title))
    {
        10000
    } else {
        0
    }
}

fn calculate_preferred_langs(data: &ParsedData, settings: &RankSettings) -> i64 {
    if data
        .languages
        .iter()
        .any(|l| settings.languages.preferred.contains(l))
    {
        10000
    } else {
        0
    }
}

/// Total rank score for a parsed release. `settings` must have been
/// [`RankSettings::prepare`]d so its pattern lists are compiled.
#[must_use]
pub fn get_rank_total(data: &ParsedData, settings: &RankSettings) -> i64 {
    calculate_quality_rank(data, settings)
        + calculate_hdr_rank(data, settings)
        + calculate_channels_rank(data, settings)
        + calculate_audio_rank(data, settings)
        + calculate_codec_rank(data, settings)
        + calculate_extra_ranks(data, settings)
        + calculate_preferred(data, settings)
        + calculate_preferred_langs(data, settings)
}
