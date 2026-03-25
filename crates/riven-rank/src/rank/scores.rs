use std::collections::HashMap;

use crate::parse::ParsedData;
use crate::settings::RankSettings;

pub(crate) mod defaults {
    // Quality
    pub const AV1: i64 = 500;
    pub const AVC: i64 = 500;
    pub const BLURAY: i64 = 100;
    pub const DVD: i64 = -5000;
    pub const HDTV: i64 = -5000;
    pub const HEVC: i64 = 500;
    pub const MPEG: i64 = -1000;
    pub const REMUX: i64 = 10000;
    pub const VHS: i64 = -10000;
    pub const WEB: i64 = 100;
    pub const WEBDL: i64 = 200;
    pub const WEBMUX: i64 = -10000;
    pub const XVID: i64 = -10000;
    pub const PDTV: i64 = -10000;

    // Rips
    pub const BDRIP: i64 = -5000;
    pub const BRRIP: i64 = -10000;
    pub const DVDRIP: i64 = -5000;
    pub const HDRIP: i64 = -10000;
    pub const PPVRIP: i64 = -10000;
    pub const TVRIP: i64 = -10000;
    pub const UHDRIP: i64 = -5000;
    pub const VHSRIP: i64 = -10000;
    pub const WEBDLRIP: i64 = -10000;
    pub const WEBRIP: i64 = -1000;

    // HDR
    pub const BIT_10: i64 = 100;
    pub const DOLBY_VISION: i64 = 3000;
    pub const HDR: i64 = 2000;
    pub const HDR10PLUS: i64 = 2100;
    pub const SDR: i64 = 0;

    // Audio
    pub const AAC: i64 = 100;
    pub const ATMOS: i64 = 1000;
    pub const DOLBY_DIGITAL: i64 = 50;
    pub const DOLBY_DIGITAL_PLUS: i64 = 150;
    pub const DTS_LOSSY: i64 = 100;
    pub const DTS_LOSSLESS: i64 = 2000;
    pub const FLAC: i64 = 0;
    pub const MP3: i64 = -1000;
    pub const TRUEHD: i64 = 2000;

    // Channels
    pub const MONO: i64 = 0;
    pub const STEREO: i64 = 0;
    pub const SURROUND: i64 = 0;

    // Extras
    pub const THREE_D: i64 = -10000;
    pub const CONVERTED: i64 = -1000;
    pub const DOCUMENTARY: i64 = -250;
    pub const DUBBED: i64 = -1000;
    pub const EDITION: i64 = 100;
    pub const HARDCODED: i64 = 0;
    pub const NETWORK: i64 = 0;
    pub const PROPER: i64 = 20;
    pub const REPACK: i64 = 20;
    pub const RETAIL: i64 = 0;
    pub const SITE: i64 = -10000;
    pub const UPSCALED: i64 = -10000;
    pub const SCENE: i64 = 0;
    pub const UNCENSORED: i64 = 0;
    pub const SUBBED: i64 = 0;

    // Trash
    pub const CAM: i64 = -10000;
    pub const CLEAN_AUDIO: i64 = -10000;
    pub const R5: i64 = -10000;
    pub const SATRIP: i64 = -10000;
    pub const SCREENER: i64 = -10000;
    pub const SIZE: i64 = -10000;
    pub const TELECINE: i64 = -10000;
    pub const TELESYNC: i64 = -10000;

    /// Map quality strings to their default scores.
    pub fn quality_default(q: &str) -> i64 {
        match q {
            "WEB" => WEB,
            "WEB-DL" => WEBDL,
            "BluRay" => BLURAY,
            "HDTV" => HDTV,
            "VHS" => VHS,
            "WEBMux" => WEBMUX,
            "BluRay REMUX" | "REMUX" => REMUX,
            "DVD" => DVD,
            "WEBRip" => WEBRIP,
            "WEB-DLRip" => WEBDLRIP,
            "UHDRip" => UHDRIP,
            "HDRip" => HDRIP,
            "DVDRip" => DVDRIP,
            "BDRip" => BDRIP,
            "BRRip" => BRRIP,
            "VHSRip" => VHSRIP,
            "PPVRip" => PPVRIP,
            "SATRip" => SATRIP,
            "TVRip" => TVRIP,
            "TeleCine" => TELECINE,
            "TeleSync" => TELESYNC,
            "SCR" => SCREENER,
            "R5" => R5,
            "CAM" => CAM,
            "PDTV" => PDTV,
            _ => 0,
        }
    }

    pub fn codec_default(codec: &str) -> i64 {
        match codec {
            "avc" => AVC,
            "hevc" => HEVC,
            "xvid" => XVID,
            "av1" => AV1,
            "mpeg" => MPEG,
            _ => 0,
        }
    }

    pub fn audio_default(audio: &str) -> i64 {
        match audio {
            "AAC" => AAC,
            "Atmos" => ATMOS,
            "Dolby Digital" => DOLBY_DIGITAL,
            "Dolby Digital Plus" => DOLBY_DIGITAL_PLUS,
            "DTS Lossy" => DTS_LOSSY,
            "DTS Lossless" => DTS_LOSSLESS,
            "FLAC" => FLAC,
            "MP3" => MP3,
            "TrueHD" => TRUEHD,
            "HQ Clean Audio" => CLEAN_AUDIO,
            _ => 0,
        }
    }

    pub fn hdr_default(hdr: &str) -> i64 {
        match hdr {
            "DV" => DOLBY_VISION,
            "HDR" => HDR,
            "HDR10+" => HDR10PLUS,
            "SDR" => SDR,
            _ => 0,
        }
    }
}

fn calculate_quality_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let q = match data.quality.as_deref() {
        Some(q) => q,
        None => return 0,
    };
    settings
        .custom_ranks
        .quality_rank(q)
        .map(|cr| cr.resolve(defaults::quality_default(q)))
        .unwrap_or(0)
}

fn calculate_codec_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let codec = match data.codec.as_deref() {
        Some(c) => c,
        None => return 0,
    };
    settings
        .custom_ranks
        .codec_rank(codec)
        .map(|cr| cr.resolve(defaults::codec_default(codec)))
        .unwrap_or(0)
}

fn calculate_hdr_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let cr = &settings.custom_ranks;
    let mut score: i64 = data
        .hdr
        .iter()
        .map(|h| {
            cr.hdr_rank(h)
                .map(|cr| cr.resolve(defaults::hdr_default(h)))
                .unwrap_or(0)
        })
        .sum();

    if data.bit_depth.is_some() {
        score += cr.hdr.bit10.resolve(defaults::BIT_10);
    }
    score
}

fn calculate_audio_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let cr = &settings.custom_ranks;
    data.audio
        .iter()
        .map(|a| {
            cr.audio_rank(a)
                .map(|cr| cr.resolve(defaults::audio_default(a)))
                .unwrap_or(0)
        })
        .sum()
}

fn calculate_channels_rank(data: &ParsedData, settings: &RankSettings) -> i64 {
    let cr = &settings.custom_ranks;
    data.channels
        .iter()
        .map(|c| match c.as_str() {
            "5.1" | "7.1" => cr.audio.surround.resolve(defaults::SURROUND),
            "stereo" | "2.0" => cr.audio.stereo.resolve(defaults::STEREO),
            "mono" => cr.audio.mono.resolve(defaults::MONO),
            _ => 0,
        })
        .sum()
}

fn calculate_extra_ranks(data: &ParsedData, settings: &RankSettings) -> i64 {
    let cr = &settings.custom_ranks;
    let checks: &[(bool, &crate::settings::CustomRank, i64)] = &[
        (data.three_d, &cr.extras.three_d, defaults::THREE_D),
        (data.converted, &cr.extras.converted, defaults::CONVERTED),
        (data.documentary, &cr.extras.documentary, defaults::DOCUMENTARY),
        (data.dubbed, &cr.extras.dubbed, defaults::DUBBED),
        (data.edition.is_some(), &cr.extras.edition, defaults::EDITION),
        (data.hardcoded, &cr.extras.hardcoded, defaults::HARDCODED),
        (data.network.is_some(), &cr.extras.network, defaults::NETWORK),
        (data.proper, &cr.extras.proper, defaults::PROPER),
        (data.repack, &cr.extras.repack, defaults::REPACK),
        (data.retail, &cr.extras.retail, defaults::RETAIL),
        (data.subbed, &cr.extras.subbed, defaults::SUBBED),
        (data.upscaled, &cr.extras.upscaled, defaults::UPSCALED),
        (data.site.is_some(), &cr.extras.site, defaults::SITE),
        (data.size.is_some(), &cr.trash.size, defaults::SIZE),
        (data.scene, &cr.extras.scene, defaults::SCENE),
        (data.uncensored, &cr.extras.uncensored, defaults::UNCENSORED),
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
    let matches = settings
        .preferred
        .iter()
        .filter_map(|p| regex::Regex::new(p).ok())
        .any(|re| re.is_match(&data.raw_title));
    if matches { 10000 } else { 0 }
}

fn calculate_preferred_langs(data: &ParsedData, settings: &RankSettings) -> i64 {
    if settings.languages.preferred.is_empty() {
        return 0;
    }
    if data.languages.iter().any(|l| settings.languages.preferred.contains(l)) {
        10000
    } else {
        0
    }
}

pub fn get_rank(data: &ParsedData, settings: &RankSettings) -> (i64, HashMap<String, i64>) {
    let mut parts = HashMap::new();
    let mut rank: i64 = 0;

    let categories: &[(&str, i64)] = &[
        ("quality", calculate_quality_rank(data, settings)),
        ("hdr", calculate_hdr_rank(data, settings)),
        ("channels", calculate_channels_rank(data, settings)),
        ("audio", calculate_audio_rank(data, settings)),
        ("codec", calculate_codec_rank(data, settings)),
        ("extras", calculate_extra_ranks(data, settings)),
    ];

    for &(name, score) in categories {
        parts.insert(name.into(), score);
        rank += score;
    }

    for (name, score) in [
        ("preferred_patterns", calculate_preferred(data, settings)),
        ("preferred_languages", calculate_preferred_langs(data, settings)),
    ] {
        if score != 0 {
            parts.insert(name.into(), score);
        }
        rank += score;
    }

    (rank, parts)
}
