use std::collections::HashMap;

use crate::defaults::RankingModel;
use crate::parse::ParsedData;
use crate::settings::RankSettings;

fn calculate_quality_rank(data: &ParsedData, settings: &RankSettings, model: &RankingModel) -> i64 {
    let Some(q) = data.quality.as_deref() else {
        return 0;
    };
    settings
        .custom_ranks
        .quality_rank(q)
        .map_or(0, |cr| cr.resolve(model.quality_score(q)))
}

fn calculate_codec_rank(data: &ParsedData, settings: &RankSettings, model: &RankingModel) -> i64 {
    let Some(codec) = data.codec.as_deref() else {
        return 0;
    };
    settings
        .custom_ranks
        .codec_rank(codec)
        .map_or(0, |cr| cr.resolve(model.codec_score(codec)))
}

fn calculate_hdr_rank(data: &ParsedData, settings: &RankSettings, model: &RankingModel) -> i64 {
    let cr = &settings.custom_ranks;
    let mut score: i64 = data
        .hdr
        .iter()
        .map(|h| {
            cr.hdr_rank(h)
                .map_or(0, |cr| cr.resolve(model.hdr_score(h)))
        })
        .sum();
    if data.bit_depth.is_some() {
        score += cr.hdr.bit10.resolve(model.bit10);
    }
    score
}

fn calculate_audio_rank(data: &ParsedData, settings: &RankSettings, model: &RankingModel) -> i64 {
    let cr = &settings.custom_ranks;
    data.audio
        .iter()
        .map(|a| {
            cr.audio_rank(a)
                .map_or(0, |cr| cr.resolve(model.audio_score(a)))
        })
        .sum()
}

fn calculate_channels_rank(
    data: &ParsedData,
    settings: &RankSettings,
    model: &RankingModel,
) -> i64 {
    let cr = &settings.custom_ranks;
    data.channels
        .iter()
        .map(|c| match c.as_str() {
            "5.1" | "7.1" => cr.audio.surround.resolve(model.surround),
            "stereo" | "2.0" => cr.audio.stereo.resolve(model.stereo),
            "mono" => cr.audio.mono.resolve(model.mono),
            _ => 0,
        })
        .sum()
}

fn calculate_extra_ranks(data: &ParsedData, settings: &RankSettings, model: &RankingModel) -> i64 {
    let cr = &settings.custom_ranks;
    let checks: &[(bool, &crate::settings::CustomRank, i64)] = &[
        (data.three_d, &cr.extras.three_d, model.three_d),
        (data.converted, &cr.extras.converted, model.converted),
        (data.commentary, &cr.extras.commentary, model.commentary),
        (data.documentary, &cr.extras.documentary, model.documentary),
        (data.dubbed, &cr.extras.dubbed, model.dubbed),
        (data.edition.is_some(), &cr.extras.edition, model.edition),
        (data.hardcoded, &cr.extras.hardcoded, model.hardcoded),
        (data.network.is_some(), &cr.extras.network, model.network),
        (data.proper, &cr.extras.proper, model.proper),
        (data.repack, &cr.extras.repack, model.repack),
        (data.retail, &cr.extras.retail, model.retail),
        (data.subbed, &cr.extras.subbed, model.subbed),
        (data.upscaled, &cr.extras.upscaled, model.upscaled),
        (data.site.is_some(), &cr.extras.site, model.site),
        (data.size.is_some(), &cr.trash.size, model.size),
        (data.scene, &cr.extras.scene, model.scene),
        (data.uncensored, &cr.extras.uncensored, model.uncensored),
    ];
    checks
        .iter()
        .filter(|(cond, _, _)| *cond)
        .map(|(_, rank, default)| rank.resolve(*default))
        .sum()
}

fn calculate_preferred(data: &ParsedData, settings: &RankSettings) -> i64 {
    if settings.preferred.is_empty() {
        return 0;
    }
    let matches = if settings.preferred_compiled.is_empty() {
        debug_assert!(
            false,
            "RankSettings::prepare() was not called — preferred regex compiled per-torrent"
        );
        settings
            .preferred
            .iter()
            .filter_map(|p| regex::Regex::new(p).ok())
            .any(|re| re.is_match(&data.raw_title))
    } else {
        settings
            .preferred_compiled
            .iter()
            .any(|re| re.is_match(&data.raw_title))
    };
    if matches { 10000 } else { 0 }
}

fn calculate_preferred_langs(data: &ParsedData, settings: &RankSettings) -> i64 {
    if settings.languages.preferred.is_empty() {
        return 0;
    }
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

#[must_use]
pub fn get_rank(
    data: &ParsedData,
    settings: &RankSettings,
    model: &RankingModel,
) -> (i64, HashMap<String, i64>) {
    let mut parts = HashMap::with_capacity(8);
    let mut rank: i64 = 0;

    let categories: &[(&str, i64)] = &[
        ("quality", calculate_quality_rank(data, settings, model)),
        ("hdr", calculate_hdr_rank(data, settings, model)),
        ("channels", calculate_channels_rank(data, settings, model)),
        ("audio", calculate_audio_rank(data, settings, model)),
        ("codec", calculate_codec_rank(data, settings, model)),
        ("extras", calculate_extra_ranks(data, settings, model)),
    ];

    for &(name, score) in categories {
        parts.insert(name.into(), score);
        rank += score;
    }

    for (name, score) in [
        ("preferred_patterns", calculate_preferred(data, settings)),
        (
            "preferred_languages",
            calculate_preferred_langs(data, settings),
        ),
    ] {
        if score != 0 {
            parts.insert(name.into(), score);
        }
        rank += score;
    }

    (rank, parts)
}
