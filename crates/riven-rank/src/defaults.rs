//! Built-in default scores for every `CustomRank` field.

use crate::settings::{
    AudioRanks, CustomRank, CustomRanksConfig, ExtrasRanks, HdrRanks, QualityRanks, RipsRanks,
    TrashRanks,
};

const fn score(rank: i64) -> CustomRank {
    CustomRank::scored(true, rank)
}

/// The score each `CustomRank` falls back to when a profile leaves `rank`
/// unset. Only `rank` is meaningful here; `fetch` is unused.
pub static DEFAULT_SCORES: CustomRanksConfig = CustomRanksConfig {
    quality: QualityRanks {
        av1: score(500),
        avc: score(500),
        bluray: score(100),
        dvd: score(-5000),
        hdtv: score(-5000),
        hevc: score(500),
        mpeg: score(-1000),
        remux: score(10000),
        vhs: score(-10000),
        web: score(100),
        webdl: score(200),
        webmux: score(-10000),
        xvid: score(-10000),
    },
    rips: RipsRanks {
        bdrip: score(-5000),
        brrip: score(-10000),
        dvdrip: score(-5000),
        hdrip: score(-10000),
        ppvrip: score(-10000),
        satrip: score(-10000),
        tvrip: score(-10000),
        uhdrip: score(-5000),
        vhsrip: score(-10000),
        webdlrip: score(-10000),
        webrip: score(-1000),
    },
    hdr: HdrRanks {
        bit10: score(100),
        dolby_vision: score(3000),
        hdr: score(2000),
        hdr10plus: score(2100),
        sdr: score(0),
    },
    audio: AudioRanks {
        aac: score(100),
        atmos: score(1000),
        dolby_digital: score(50),
        dolby_digital_plus: score(150),
        dts_lossy: score(100),
        dts_lossless: score(2000),
        flac: score(0),
        mono: score(0),
        mp3: score(-1000),
        stereo: score(0),
        surround: score(0),
        truehd: score(2000),
    },
    extras: ExtrasRanks {
        three_d: score(-10000),
        converted: score(-1000),
        commentary: score(0),
        documentary: score(-250),
        dubbed: score(-1000),
        edition: score(100),
        hardcoded: score(0),
        network: score(0),
        proper: score(20),
        repack: score(20),
        retail: score(0),
        site: score(-10000),
        subbed: score(0),
        upscaled: score(-10000),
        scene: score(0),
        uncensored: score(0),
    },
    trash: TrashRanks {
        cam: score(-10000),
        clean_audio: score(-10000),
        pdtv: score(-10000),
        r5: score(-10000),
        screener: score(-10000),
        size: score(-10000),
        telecine: score(-10000),
        telesync: score(-10000),
    },
};

/// Nested JSON matching `custom_ranks` structure, used to inject `"default": N`
/// into each `CustomRank` entry in the GraphQL `rankSettings` response.
#[must_use]
pub fn default_category_map() -> serde_json::Value {
    let mut map = serde_json::to_value(&DEFAULT_SCORES).unwrap_or_default();
    for category in map.as_object_mut().into_iter().flat_map(|m| m.values_mut()) {
        for entry in category
            .as_object_mut()
            .into_iter()
            .flat_map(|m| m.values_mut())
        {
            *entry = entry["rank"].take();
        }
    }
    map
}
