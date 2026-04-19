#![allow(
    clippy::missing_panics_doc,
    clippy::struct_excessive_bools,
    clippy::too_many_lines
)]

mod detect;
mod languages;
pub(crate) mod patterns;
mod title;

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

use detect::{
    detect_anime, detect_network, detect_scene, detect_trash, is_anime_group, is_false_group,
};
use languages::{LANG_PATTERNS, translate_langs};
use patterns::{
    RE_3D, RE_ADULT, RE_AUDIO_AAC, RE_AUDIO_ATMOS, RE_AUDIO_DD, RE_AUDIO_DD_PLUS,
    RE_AUDIO_DTS_LOSSLESS, RE_AUDIO_DTS_LOSSY, RE_AUDIO_FLAC, RE_AUDIO_HQ_CLEAN, RE_AUDIO_MP3,
    RE_AUDIO_OPUS, RE_AUDIO_PCM, RE_AUDIO_TRUEHD, RE_AUDIO_TRUEHD_BARE, RE_BIT_DEPTH_8,
    RE_BIT_DEPTH_10, RE_BIT_DEPTH_12, RE_BITRATE, RE_CHAN_20, RE_CHAN_51, RE_CHAN_71, RE_CHAN_MONO,
    RE_CHAN_STEREO, RE_CODEC_264_BARE, RE_CODEC_265_BARE, RE_CODEC_AV1, RE_CODEC_AVC,
    RE_CODEC_HEVC, RE_CODEC_MPEG, RE_CODEC_XVID, RE_COMMENTARY, RE_COMPLETE,
    RE_COMPLETE_COLLECTION, RE_CONTAINER, RE_CONVERTED, RE_COUNTRY, RE_DATE_COMPACT, RE_DATE_DMY,
    RE_DATE_DMY_MONTH, RE_DATE_DMY_MONTH_SHORT, RE_DATE_DMY_SHORT, RE_DATE_MDY_SHORT, RE_DATE_YMD,
    RE_DATE_YMD_SHORT, RE_DOCUMENTARY, RE_DUBBED, RE_DVB, RE_EDITION, RE_EP_NUM_BARE,
    RE_EPISODE_ANIME_BARE_SINGLE, RE_EPISODE_CODE_HEX, RE_EPISODE_CODE_NUM, RE_EPISODE_CONSECUTIVE,
    RE_EPISODE_CROSSREF, RE_EPISODE_CROSSREF_RANGE, RE_EPISODE_FULL, RE_EPISODE_OF,
    RE_EPISODE_RANGE, RE_EPISODE_RANGE_BARE, RE_EPISODE_RANGE_BARE_HYPHEN, RE_EPISODE_RANGE_PAREN,
    RE_EPISODE_RUSSIAN, RE_EPISODE_RUSSIAN_OF, RE_EPISODE_SE, RE_EPISODE_STANDALONE, RE_EPISODE_TR,
    RE_EXTENDED, RE_EXTENSION, RE_EXTRAS, RE_EXTRAS_ED, RE_EXTRAS_NC, RE_EXTRAS_NCED,
    RE_EXTRAS_NCOP, RE_EXTRAS_OP, RE_EXTRAS_OVA, RE_GROUP_BRACKET, RE_GROUP_DASH, RE_GROUP_PAREN,
    RE_HARDCODED, RE_HDR_DV, RE_HDR_HDR, RE_HDR_HDR10PLUS, RE_HDR_SDR, RE_PART, RE_PPV,
    RE_PPV_FIGHT, RE_PROPER, RE_Q_BDRIP, RE_Q_BLURAY, RE_Q_BLURAY_REMUX1, RE_Q_BLURAY_REMUX2,
    RE_Q_BLURAY_REMUX3, RE_Q_BRRIP, RE_Q_CAM, RE_Q_CAM_FALSE, RE_Q_DVD, RE_Q_DVDRIP, RE_Q_HDRIP,
    RE_Q_HDTV, RE_Q_HDTVRIP, RE_Q_PDTV, RE_Q_PPVRIP, RE_Q_PRE_DVD, RE_Q_R5, RE_Q_REMUX,
    RE_Q_SATRIP, RE_Q_SCR, RE_Q_TELECINE, RE_Q_TELESYNC, RE_Q_TVRIP, RE_Q_UHDRIP, RE_Q_VHS,
    RE_Q_VHSRIP, RE_Q_WEB, RE_Q_WEBDL, RE_Q_WEBDLRIP, RE_Q_WEBMUX, RE_Q_WEBRIP, RE_REGION,
    RE_REGION_DISC, RE_REMASTERED, RE_REPACK, RE_RES_1280, RE_RES_1920, RE_RES_3840, RE_RES_DIGITS,
    RE_RES_FHD, RE_RES_GENERIC, RE_RES_PREFIXED_480, RE_RES_PREFIXED_720, RE_RES_PREFIXED_1080,
    RE_RES_PREFIXED_2160, RE_RES_QHD, RE_RES_TYPO, RE_RES_WXH, RE_RETAIL, RE_SEASON_EP_COMPACT, RE_SEASON_FULL,
    RE_SEASON_MULTI, RE_SEASON_ORDINAL, RE_SEASON_PT, RE_SEASON_RANGE, RE_SEASON_RUSSIAN,
    RE_SEASON_RUSSIAN2, RE_SEASON_SE, RE_SEASON_TR, RE_SEASON_TV, RE_SITE, RE_SITE_BRACKET,
    RE_SITE_DOMAIN, RE_SITE_KNOWN, RE_SIZE, RE_SPRINT, RE_SUBBED, RE_UNCENSORED, RE_UNRATED,
    RE_UPSCALED, RE_UPSCALED_AI, RE_UPSCALED_SPECIFIC, RE_VOLUME, RE_YEAR, RE_YEAR_RANGE,
    RE_YEAR_RANGE_SHORT,
};
pub(crate) use title::normalize_title;
use title::{extract_title, normalize_edition};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedData {
    pub raw_title: String,
    pub parsed_title: String,
    pub normalized_title: String,
    pub trash: bool,
    pub adult: bool,
    pub anime: bool,
    pub year: Option<i32>,
    pub resolution: String,
    pub seasons: Vec<i32>,
    pub episodes: Vec<i32>,
    pub complete: bool,
    pub volumes: Vec<i32>,
    pub languages: Vec<String>,
    pub quality: Option<String>,
    pub hdr: Vec<String>,
    pub codec: Option<String>,
    pub audio: Vec<String>,
    pub channels: Vec<String>,
    pub dubbed: bool,
    pub subbed: bool,
    pub date: Option<String>,
    pub group: Option<String>,
    pub edition: Option<String>,
    pub bit_depth: Option<String>,
    pub bitrate: Option<String>,
    pub network: Option<String>,
    pub extended: bool,
    pub converted: bool,
    pub hardcoded: bool,
    pub region: Option<String>,
    pub ppv: bool,
    pub three_d: bool,
    pub site: Option<String>,
    pub size: Option<String>,
    pub proper: bool,
    pub repack: bool,
    pub retail: bool,
    pub upscaled: bool,
    pub remastered: bool,
    pub unrated: bool,
    pub uncensored: bool,
    pub documentary: bool,
    pub commentary: bool,
    pub episode_code: Option<String>,
    pub part: Option<i32>,
    pub country: Option<String>,
    pub container: Option<String>,
    pub extension: Option<String>,
    pub extras: Vec<String>,
    pub torrent: bool,
    pub scene: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ParseOptions {
    pub translate_languages: bool,
}

impl ParsedData {
    #[must_use]
    pub const fn media_type(&self) -> &str {
        if !self.seasons.is_empty() || !self.episodes.is_empty() {
            "show"
        } else {
            "movie"
        }
    }

    /// Merge another `ParsedData` into this one, preferring non-empty/Some values from self.
    pub fn merge(&mut self, other: Self) {
        if self.parsed_title.is_empty() {
            self.parsed_title = other.parsed_title;
            self.normalized_title = other.normalized_title;
        }
        if self.resolution == "unknown" {
            self.resolution = other.resolution;
        }
        self.year = self.year.or(other.year);
        self.quality = self.quality.take().or(other.quality);
        self.codec = self.codec.take().or(other.codec);
        self.bit_depth = self.bit_depth.take().or(other.bit_depth);
        self.bitrate = self.bitrate.take().or(other.bitrate);
        self.container = self.container.take().or(other.container);
        self.extension = self.extension.take().or(other.extension);
        self.group = self.group.take().or(other.group);
        self.edition = self.edition.take().or(other.edition);
        self.region = self.region.take().or(other.region);
        self.network = self.network.take().or(other.network);
        self.site = self.site.take().or(other.site);
        self.country = self.country.take().or(other.country);
        self.part = self.part.or(other.part);

        extend_sorted(&mut self.seasons, other.seasons);
        extend_sorted(&mut self.episodes, other.episodes);
        extend_sorted(&mut self.volumes, other.volumes);
        extend_sorted(&mut self.languages, other.languages);
        extend_sorted(&mut self.audio, other.audio);
        extend_sorted(&mut self.hdr, other.hdr);
        extend_sorted(&mut self.channels, other.channels);
        extend_sorted(&mut self.extras, other.extras);

        self.trash |= other.trash;
        self.adult |= other.adult;
        self.anime |= other.anime;
        self.complete |= other.complete;
        self.dubbed |= other.dubbed;
        self.subbed |= other.subbed;
        self.extended |= other.extended;
        self.converted |= other.converted;
        self.hardcoded |= other.hardcoded;
        self.proper |= other.proper;
        self.repack |= other.repack;
        self.retail |= other.retail;
        self.upscaled |= other.upscaled;
        self.remastered |= other.remastered;
        self.unrated |= other.unrated;
        self.uncensored |= other.uncensored;
        self.documentary |= other.documentary;
        self.commentary |= other.commentary;
        self.scene |= other.scene;
    }
}

fn push_unique(vec: &mut Vec<String>, val: &str) {
    if !vec.iter().any(|v| v == val) {
        vec.push(val.to_string());
    }
}

fn extend_sorted<T: Ord>(dst: &mut Vec<T>, src: Vec<T>) {
    dst.extend(src);
    dst.sort_unstable();
    dst.dedup();
}

/// Extract numbers from a regex with capture groups 1 (required) and 2 (optional range end).
/// Callers sort and dedup once after all extraction passes.
fn extract_numbers(re: &Regex, raw: &str, target: &mut Vec<i32>) {
    for cap in re.captures_iter(raw) {
        if let Ok(n1) = cap[1].parse::<i32>() {
            target.push(n1);
            if let Some(m2) = cap.get(2)
                && let Ok(n2) = m2.as_str().parse::<i32>()
            {
                for n in n1..=n2 {
                    target.push(n);
                }
            }
        }
    }
}

fn push_episode_range(start: i32, end: i32, target: &mut Vec<i32>) {
    if start > end || start == 0 || end - start > 2000 {
        return;
    }
    if (1900..=2099).contains(&start) && (1900..=2099).contains(&end) {
        return;
    }
    for n in start..=end {
        target.push(n);
    }
}

fn extract_episode_ranges(re: &Regex, raw: &str, target: &mut Vec<i32>) {
    for cap in re.captures_iter(raw) {
        if let (Ok(start), Ok(end)) = (cap[1].parse::<i32>(), cap[2].parse::<i32>()) {
            push_episode_range(start, end, target);
        }
    }
}

fn has_anime_context(data: &ParsedData, raw: &str) -> bool {
    static RE_LONG_RUNNING_ANIME: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\b(?:naruto|one[ ._-]*piece|bleach|dragon[ ._-]*ball|detective[ ._-]*conan|hunter[ ._-]*x[ ._-]*hunter)\b")
            .unwrap()
    });

    data.episode_code.is_some()
        || data.group.as_deref().is_some_and(is_anime_group)
        || RE_LONG_RUNNING_ANIME.is_match(raw)
}

fn maybe_has_season_markers(raw: &str) -> bool {
    raw.contains(['S', 's', 'X', 'x'])
        || raw.contains("Season")
        || raw.contains("season")
        || raw.contains("temporada")
        || raw.contains("Temporada")
        || raw.contains("Sezon")
        || raw.contains("sezon")
        || raw.contains("Сезон")
        || raw.contains("сезон")
        || raw.contains("TV-")
}

fn maybe_has_episode_markers(raw: &str) -> bool {
    raw.contains(['E', 'e', 'X', 'x', '~'])
        || raw.contains("Episode")
        || raw.contains("episode")
        || raw.contains("Episodes")
        || raw.contains("episodes")
        || raw.contains("Cap")
        || raw.contains("cap")
        || raw.contains("Böl")
        || raw.contains("böl")
        || raw.contains("Ser")
        || raw.contains("ser")
        || raw.contains(" of ")
}

/// Detect resolution from raw title string.
fn detect_resolution(raw: &str) -> String {
    // Ordered by specificity: dimension-based first, then prefixed, then generic
    let priority_checks: &[(&Regex, &str)] = &[
        (&RE_RES_3840, "2160p"),
        (&RE_RES_1920, "1080p"),
        (&RE_RES_1280, "720p"),
        (&RE_RES_QHD, "1440p"),
        (&RE_RES_FHD, "1080p"),
        (&RE_RES_PREFIXED_2160, "2160p"),
        (&RE_RES_PREFIXED_1080, "1080p"),
        (&RE_RES_PREFIXED_720, "720p"),
        (&RE_RES_PREFIXED_480, "480p"),
    ];

    for (re, res) in priority_checks {
        if re.is_match(raw) {
            return res.to_string();
        }
    }

    // Typo correction: "4800p" → "480p", "10800p" → "1080p"
    if let Some(cap) = RE_RES_TYPO.captures(raw) {
        return format!("{}p", &cap[1]);
    }

    // Generic: use last occurrence (e.g., "720p...1080p" → "1080p")
    if let Some(last) = RE_RES_GENERIC.find_iter(raw).last() {
        let lower = last.as_str().to_lowercase();
        return if lower == "4k" {
            "2160p".to_string()
        } else {
            lower
        };
    }

    // Digit-based (e.g., "1080i")
    if let Some(cap) = RE_RES_DIGITS.captures(raw) {
        let scan = cap[2].to_lowercase();
        return match &cap[1] {
            "3840" | "2160" => "2160p",
            "1080" if scan == "i" => "1080i",
            "1080" => "1080p",
            "720" if scan == "i" => "720i",
            "720" => "720p",
            "576" => "576p",
            "480" => "480p",
            "360" => "360p",
            "240" => "240p",
            num => return format!("{num}{scan}"),
        }
        .to_string();
    }

    // WxH fallback: classify by height (e.g. "704x400" → "480p", "852x480" → "480p")
    if let Some(cap) = RE_RES_WXH.captures(raw)
        && let Ok(height) = cap[1].parse::<u32>()
    {
        return match height {
            h if h >= 2160 => "2160p",
            h if h >= 1080 => "1080p",
            h if h >= 720 => "720p",
            h if h >= 480 => "480p",
            h if h >= 360 => "360p",
            _ => "240p",
        }
        .to_string();
    }

    "unknown".to_string()
}

/// Detect quality from raw title string. First match wins (ordered by priority).
fn detect_quality(raw: &str) -> Option<String> {
    // Special cases that need multi-regex logic
    if RE_Q_TELESYNC.is_match(raw) {
        return Some("TeleSync".into());
    }
    if RE_Q_TELECINE.is_match(raw) {
        return Some("TeleCine".into());
    }
    if RE_Q_SCR.is_match(raw) || RE_Q_PRE_DVD.is_match(raw) {
        return Some("SCR".into());
    }
    if (RE_Q_BLURAY_REMUX1.is_match(raw) && RE_Q_REMUX.is_match(raw))
        || RE_Q_BLURAY_REMUX2.is_match(raw)
        || RE_Q_BLURAY_REMUX3.is_match(raw)
    {
        return Some("BluRay REMUX".into());
    }
    if RE_Q_REMUX.is_match(raw) {
        return Some("REMUX".into());
    }
    if RE_Q_BLURAY.is_match(raw) && !RE_Q_BRRIP.is_match(raw) {
        return Some("BluRay".into());
    }

    // Simple single-regex checks (order matters)
    let simple_checks: &[(&Regex, &str)] = &[
        (&RE_Q_UHDRIP, "UHDRip"),
        (&RE_Q_HDRIP, "HDRip"),
        (&RE_Q_BRRIP, "BRRip"),
        (&RE_Q_BDRIP, "BDRip"),
        (&RE_Q_DVDRIP, "DVDRip"),
        (&RE_Q_VHSRIP, "VHSRip"),
        (&RE_Q_DVD, "DVD"),
        (&RE_Q_VHS, "VHS"),
        (&RE_Q_PPVRIP, "PPVRip"),
        (&RE_Q_HDTVRIP, "HDTVRip"),
        (&RE_Q_SATRIP, "SATRip"),
        (&RE_Q_TVRIP, "TVRip"),
        (&RE_Q_R5, "R5"),
        (&RE_Q_WEBMUX, "WEBMux"),
        (&RE_Q_WEBRIP, "WEBRip"),
        (&RE_Q_WEBDLRIP, "WEB-DLRip"),
        (&RE_Q_WEBDL, "WEB-DL"),
        (&RE_Q_WEB, "WEB"),
    ];
    for (re, name) in simple_checks {
        if re.is_match(raw) {
            return Some(name.to_string());
        }
    }

    // CAM with false-positive guard
    if RE_Q_CAM.is_match(raw) && !RE_Q_CAM_FALSE.is_match(raw) {
        return Some("CAM".into());
    }
    // S-print → CAM
    if RE_SPRINT.is_match(raw) {
        return Some("CAM".into());
    }
    if RE_Q_PDTV.is_match(raw) {
        return Some("PDTV".into());
    }
    // DVB → HDTV
    if RE_DVB.is_match(raw) {
        return Some("HDTV".into());
    }
    if RE_Q_HDTV.is_match(raw) {
        return Some("HDTV".into());
    }

    None
}

/// Detect codec from raw title string.
fn detect_codec(raw: &str) -> Option<String> {
    let checks: &[(&Regex, &str)] = &[
        (&RE_CODEC_AVC, "avc"),
        (&RE_CODEC_HEVC, "hevc"),
        (&RE_CODEC_XVID, "xvid"),
        (&RE_CODEC_AV1, "av1"),
        (&RE_CODEC_MPEG, "mpeg"),
    ];
    if let Some((_, name)) = checks.iter().find(|(re, _)| re.is_match(raw)) {
        return Some(name.to_string());
    }
    // Bare 264/265
    if RE_CODEC_264_BARE.is_match(raw) {
        return Some("avc".into());
    }
    if RE_CODEC_265_BARE.is_match(raw) {
        return Some("hevc".into());
    }
    None
}

/// Detect audio formats from raw title string.
fn detect_audio(raw: &str, audio: &mut Vec<String>) {
    if RE_AUDIO_HQ_CLEAN.is_match(raw) {
        push_unique(audio, "HQ Clean Audio");
    }
    if RE_AUDIO_DTS_LOSSLESS.is_match(raw) {
        push_unique(audio, "DTS Lossless");
    }
    if RE_AUDIO_DTS_LOSSY.is_match(raw) && !audio.iter().any(|a| a == "DTS Lossless") {
        push_unique(audio, "DTS Lossy");
    }
    if RE_AUDIO_ATMOS.is_match(raw) {
        push_unique(audio, "Atmos");
    }
    if RE_AUDIO_TRUEHD.is_match(raw) || RE_AUDIO_TRUEHD_BARE.is_match(raw) {
        push_unique(audio, "TrueHD");
    }
    if RE_AUDIO_FLAC.is_match(raw) {
        push_unique(audio, "FLAC");
    }
    if RE_AUDIO_DD_PLUS.is_match(raw) {
        push_unique(audio, "Dolby Digital Plus");
    }
    if RE_AUDIO_DD.is_match(raw) && !audio.iter().any(|a| a == "Dolby Digital Plus") {
        push_unique(audio, "Dolby Digital");
    }
    if RE_AUDIO_AAC.is_match(raw) {
        push_unique(audio, "AAC");
    }
    if RE_AUDIO_PCM.is_match(raw) {
        push_unique(audio, "PCM");
    }
    if RE_AUDIO_OPUS.is_match(raw) {
        push_unique(audio, "OPUS");
    }
    if RE_AUDIO_MP3.is_match(raw) {
        push_unique(audio, "MP3");
    }
}

/// Detect HDR formats from raw title string.
fn detect_hdr(raw: &str, hdr: &mut Vec<String>) {
    if RE_HDR_DV.is_match(raw) {
        push_unique(hdr, "DV");
    }
    if RE_HDR_HDR10PLUS.is_match(raw) {
        push_unique(hdr, "HDR10+");
    }
    if RE_HDR_HDR.is_match(raw) && !hdr.iter().any(|h| h == "HDR10+") {
        push_unique(hdr, "HDR");
    }
    if RE_HDR_SDR.is_match(raw) {
        push_unique(hdr, "SDR");
    }
}

/// Detect channels from raw title string.
fn detect_channels(raw: &str, channels: &mut Vec<String>) {
    let checks: &[(&Regex, &str)] = &[
        (&RE_CHAN_71, "7.1"),
        (&RE_CHAN_51, "5.1"),
        (&RE_CHAN_20, "2.0"),
        (&RE_CHAN_STEREO, "stereo"),
        (&RE_CHAN_MONO, "mono"),
    ];
    for (re, name) in checks {
        if re.is_match(raw) {
            push_unique(channels, name);
        }
    }
}

/// Detect bit depth from raw title string.
fn detect_bit_depth(raw: &str) -> Option<String> {
    if RE_BIT_DEPTH_12.is_match(raw) {
        Some("12bit".into())
    } else if RE_BIT_DEPTH_10.is_match(raw) {
        Some("10bit".into())
    } else if RE_BIT_DEPTH_8.is_match(raw) {
        Some("8bit".into())
    } else {
        None
    }
}

fn two_digit_year_to_full(yy: &str) -> Option<i32> {
    let yy = yy.parse::<i32>().ok()?;
    Some(if yy <= 68 { 2000 + yy } else { 1900 + yy })
}

fn month_number(month: &str) -> Option<&'static str> {
    let lower = month.to_ascii_lowercase();
    match lower.as_str() {
        m if m.starts_with("jan") => Some("01"),
        m if m.starts_with("feb") => Some("02"),
        m if m.starts_with("mar") => Some("03"),
        m if m.starts_with("apr") => Some("04"),
        "may" => Some("05"),
        m if m.starts_with("jun") => Some("06"),
        m if m.starts_with("jul") => Some("07"),
        m if m.starts_with("aug") => Some("08"),
        m if m.starts_with("sep") => Some("09"),
        m if m.starts_with("oct") => Some("10"),
        m if m.starts_with("nov") => Some("11"),
        m if m.starts_with("dec") => Some("12"),
        _ => None,
    }
}

fn followed_by_four_digit_year(raw: &str, index: usize) -> bool {
    let tail = &raw[index..];
    tail.len() >= 4 && tail.chars().take(4).all(|ch| ch.is_ascii_digit())
}

/// Detect date from raw title string, trying multiple formats.
fn detect_date(raw: &str) -> Option<String> {
    // YYYY-MM-DD / YYYY.MM.DD
    if let Some(cap) = RE_DATE_YMD.captures(raw) {
        if cap[2] == cap[4] {
            return Some(format!("{}-{}-{}", &cap[1], &cap[3], &cap[5]));
        }
    }
    // DD-MM-YYYY
    if let Some(cap) = RE_DATE_DMY.captures(raw) {
        if cap[2] == cap[4] {
            return Some(format!("{}-{}-{}", &cap[5], &cap[3], &cap[1]));
        }
    }
    // YYYYMMDD (compact)
    if let Some(cap) = RE_DATE_COMPACT.captures(raw) {
        return Some(format!("{}-{}-{}", &cap[1], &cap[2], &cap[3]));
    }
    // 13 Feb 2016 / 9th Dec 2019 / 16-Feb-2017
    if let Some(cap) = RE_DATE_DMY_MONTH.captures(raw)
        && let Some(month) = month_number(&cap[2])
    {
        return Some(format!("{}-{}-{:0>2}", &cap[3], month, &cap[1]));
    }
    if let Some(cap) = RE_DATE_DMY_MONTH_SHORT.captures(raw)
        && let (Some(month), Some(year)) = (month_number(&cap[2]), two_digit_year_to_full(&cap[3]))
    {
        return Some(format!("{year}-{}-{:0>2}", month, &cap[1]));
    }
    // 2-digit date forms follow PTT order and are only matched away from the title start.
    if let Some(cap) = RE_DATE_MDY_SHORT.captures(raw)
        && cap[2] == cap[4]
        && !followed_by_four_digit_year(raw, cap.get(0).map_or(0, |m| m.end()))
        && let Some(year) = two_digit_year_to_full(&cap[5])
    {
        return Some(format!("{year}-{}-{}", &cap[1], &cap[3]));
    }
    if let Some(cap) = RE_DATE_YMD_SHORT.captures(raw)
        && cap[2] == cap[4]
        && !followed_by_four_digit_year(raw, cap.get(0).map_or(0, |m| m.end()))
        && let Some(year) = two_digit_year_to_full(&cap[1])
    {
        return Some(format!("{year}-{}-{}", &cap[3], &cap[5]));
    }
    if let Some(cap) = RE_DATE_DMY_SHORT.captures(raw)
        && cap[2] == cap[4]
        && !followed_by_four_digit_year(raw, cap.get(0).map_or(0, |m| m.end()))
        && let Some(year) = two_digit_year_to_full(&cap[5])
    {
        return Some(format!("{year}-{}-{}", &cap[3], &cap[1]));
    }
    None
}

/// Detect episode code (CRC32 hash) from raw title.
fn detect_episode_code(raw: &str) -> Option<String> {
    if let Some(cap) = RE_EPISODE_CODE_HEX.captures(raw) {
        return Some(cap[1].to_uppercase());
    }
    if let Some(cap) = RE_EPISODE_CODE_NUM.captures(raw) {
        return Some(cap[1].to_uppercase());
    }
    None
}

/// Detect anime extras from raw title.
fn detect_anime_extras(raw: &str, extras: &mut Vec<String>) {
    if RE_EXTRAS_NCED.is_match(raw) {
        push_unique(extras, "NCED");
    }
    if RE_EXTRAS_NCOP.is_match(raw) {
        push_unique(extras, "NCOP");
    }
    if RE_EXTRAS_NC.is_match(raw) && !extras.iter().any(|e| e == "NCED" || e == "NCOP") {
        push_unique(extras, "NC");
    }
    if RE_EXTRAS_OVA.is_match(raw) {
        push_unique(extras, "OVA");
    }
    if RE_EXTRAS_ED.is_match(raw) {
        push_unique(extras, "ED");
    }
    if RE_EXTRAS_OP.is_match(raw) {
        push_unique(extras, "OP");
    }
}

fn canonical_extra(extra: &str) -> String {
    match extra.to_ascii_lowercase().as_str() {
        "featurette" | "featurettes" => "Featurette".to_string(),
        "sample" => "Sample".to_string(),
        "trailer" | "trailers" => "Trailer".to_string(),
        "deleted scene" | "deleted scenes" => "Deleted Scene".to_string(),
        other => {
            if other.contains("featurette") {
                "Featurette".to_string()
            } else if other.contains("deleted") {
                "Deleted Scene".to_string()
            } else {
                extra.to_string()
            }
        }
    }
}

fn detect_standard_extras(raw: &str, extras: &mut Vec<String>) {
    let year_pos = RE_YEAR.find(raw).map(|m| m.start());

    for cap in RE_EXTRAS.captures_iter(raw) {
        let Some(m) = cap.get(1) else {
            continue;
        };
        let extra = canonical_extra(m.as_str());

        let should_keep = match extra.as_str() {
            "Featurette" | "Sample" => year_pos.is_none_or(|pos| m.start() > pos),
            "Trailer" => {
                year_pos.is_none_or(|pos| m.start() > pos)
                    && !raw[m.end()..].to_ascii_lowercase().contains("park")
                    && !raw[m.end()..].to_ascii_lowercase().contains("and")
            }
            _ => true,
        };

        if should_keep {
            push_unique(extras, &extra);
        }
    }
}

/// Detect site from additional patterns beyond the prefix pattern.
fn detect_site_extra(raw: &str) -> Option<String> {
    if let Some(m) = RE_SITE_KNOWN.find(raw) {
        return Some(m.as_str().to_lowercase());
    }
    if let Some(cap) = RE_SITE_BRACKET.captures(raw) {
        return Some(cap[1].trim().to_string());
    }
    if let Some(m) = RE_SITE_DOMAIN.find(raw) {
        return Some(m.as_str().to_string());
    }
    None
}

fn bracket_group_is_site(raw: &str, candidate: &str) -> bool {
    if RE_SITE_DOMAIN.is_match(candidate) || RE_SITE_BRACKET.is_match(&format!("[{candidate}]")) {
        return true;
    }

    if let Some(caps) = RE_GROUP_BRACKET.captures(raw)
        && caps.get(1).is_some_and(|m| m.as_str() == candidate)
    {
        let rest = &raw[caps.get(0).unwrap().end()..];
        if rest.trim_start().starts_with('-') && candidate.contains('.') {
            return true;
        }
    }

    false
}

fn detect_bitrate(raw: &str) -> Option<String> {
    let caps = RE_BITRATE.captures(raw)?;
    let m = caps.get(1)?;
    let mut bitrate = m.as_str().to_lowercase();

    if bitrate.contains('.')
        && m.start() >= 2
        && raw.as_bytes()[m.start() - 1] == b'.'
        && raw.as_bytes()[m.start() - 2].is_ascii_digit()
    {
        let number_end = bitrate
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(bitrate.len());
        let (number, unit) = bitrate.split_at(number_end);
        if let Some((_, frac)) = number.split_once('.')
            && !frac.is_empty()
        {
            bitrate = format!("{frac}{unit}");
        }
    }

    Some(bitrate)
}

#[must_use]
pub fn parse(raw_title: &str) -> ParsedData {
    parse_with_options(raw_title, ParseOptions::default())
}

pub fn parse_with_options(raw_title: &str, options: ParseOptions) -> ParsedData {
    let mut data = ParsedData {
        raw_title: raw_title.to_string(),
        resolution: "unknown".to_string(),
        ..Default::default()
    };

    // Site detection (must be first - strip from working title)
    let working_title = if let Some(cap) = RE_SITE.captures(raw_title) {
        data.site = Some(cap[1].trim().to_string());
        raw_title[cap.get(0).unwrap().end()..].to_string()
    } else {
        static RE_SITE_BRACKET_PREFIX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^\[([^\]]+\.[^\]]+)\][\s._-]*").unwrap());
        if let Some(cap) = RE_SITE_BRACKET_PREFIX.captures(raw_title) {
            data.site = Some(cap[1].trim().to_string());
            raw_title[cap.get(0).unwrap().end()..].to_string()
        } else {
            raw_title.to_string()
        }
    };
    let raw = &working_title;

    // Extension & container
    data.extension = RE_EXTENSION.captures(raw).map(|c| c[1].to_lowercase());
    data.container = RE_CONTAINER.captures(raw).map(|c| c[1].to_lowercase());
    data.size = RE_SIZE.captures(raw).map(|c| c[1].to_string());
    data.bitrate = detect_bitrate(raw);

    // Torrent flag
    data.torrent =
        raw.to_lowercase().ends_with(".torrent") || data.extension.as_deref() == Some("torrent");

    // Date (try multiple formats)
    data.date = detect_date(raw);

    // Year — try year range first (for complete detection), then standalone
    if let Some(cap) = RE_YEAR_RANGE.captures(raw) {
        if let Ok(y) = cap[1].parse::<i32>() {
            data.year = Some(y);
            data.complete = true;
        }
    } else if let Some(cap) = RE_YEAR_RANGE_SHORT.captures(raw)
        && let Ok(y) = cap[1].parse::<i32>()
    {
        data.year = Some(y);
        data.complete = true;
    }
    if data.year.is_none() {
        // Skip a year at the very start of the string — it's part of the title (e.g. "2019 After...")
        for cap in RE_YEAR.captures_iter(raw) {
            let start = cap.get(0).map_or(0, |m| m.start());
            if start > 0 {
                if let Ok(y) = cap[1].parse::<i32>() {
                    data.year = Some(y);
                }
                break;
            }
        }
        // If no year found after position 0, fall back to the year at position 0
        if data.year.is_none() {
            data.year = RE_YEAR.captures(raw).and_then(|c| c[1].parse().ok());
        }
    }

    // Episode code (CRC32)
    data.episode_code = detect_episode_code(raw);

    // Resolution, quality, codec
    data.resolution = detect_resolution(raw);
    data.quality = detect_quality(raw);
    data.codec = detect_codec(raw);

    // Seasons (try each pattern in priority order)
    if maybe_has_season_markers(raw) {
        for cap in RE_SEASON_EP_COMPACT.captures_iter(raw) {
            if let Ok(s) = cap[1].parse::<i32>() {
                data.seasons.push(s);
            }
        }
        extract_numbers(&RE_SEASON_RANGE, raw, &mut data.seasons);
        if data.seasons.is_empty() {
            extract_numbers(&RE_SEASON_SE, raw, &mut data.seasons);
        }
        extract_numbers(&RE_SEASON_MULTI, raw, &mut data.seasons);
        extract_numbers(&RE_SEASON_FULL, raw, &mut data.seasons);
        // Ordinal: "1st season", "2nd season"
        extract_numbers(&RE_SEASON_ORDINAL, raw, &mut data.seasons);
        // Russian formats
        extract_numbers(&RE_SEASON_RUSSIAN, raw, &mut data.seasons);
        extract_numbers(&RE_SEASON_RUSSIAN2, raw, &mut data.seasons);
        // Portuguese
        extract_numbers(&RE_SEASON_PT, raw, &mut data.seasons);
        // Turkish
        extract_numbers(&RE_SEASON_TR, raw, &mut data.seasons);
        // ТВ-N
        extract_numbers(&RE_SEASON_TV, raw, &mut data.seasons);
        // Cross-reference format: group 1 = season
        for cap in RE_EPISODE_CROSSREF.captures_iter(raw) {
            if let Ok(s) = cap[1].parse::<i32>() {
                data.seasons.push(s);
            }
        }
    }
    data.seasons.sort_unstable();
    data.seasons.dedup();

    // Episodes (try each pattern in priority order)
    if maybe_has_episode_markers(raw) {
        for cap in RE_SEASON_EP_COMPACT.captures_iter(raw) {
            if let Ok(e) = cap[2].parse::<i32>() {
                data.episodes.push(e);
            }
        }
        extract_numbers(&RE_EPISODE_RANGE, raw, &mut data.episodes);
        if data.episodes.is_empty() {
            extract_numbers(&RE_EPISODE_RANGE_BARE, raw, &mut data.episodes);
        }
        if data.episodes.is_empty() {
            extract_numbers(&RE_EPISODE_RANGE_PAREN, raw, &mut data.episodes);
        }
        if data.episodes.is_empty() {
            extract_numbers(&RE_EPISODE_SE, raw, &mut data.episodes);
        }
        if data.episodes.is_empty() {
            extract_numbers(&RE_EPISODE_STANDALONE, raw, &mut data.episodes);
        }
        if data.episodes.is_empty() {
            extract_numbers(&RE_EPISODE_FULL, raw, &mut data.episodes);
        }
        if data.episodes.is_empty() {
            // "X of Y" pattern
            if let Some(cap) = RE_EPISODE_OF.captures(raw)
                && let Ok(n) = cap[1].parse::<i32>()
            {
                data.episodes.push(n);
            }
        }
        if data.episodes.is_empty() {
            // Russian "Серии: N of M"
            if let Some(cap) = RE_EPISODE_RUSSIAN_OF.captures(raw)
                && let Ok(n) = cap[1].parse::<i32>()
            {
                data.episodes.push(n);
            }
        }
        if data.episodes.is_empty() {
            // Russian episodes
            extract_numbers(&RE_EPISODE_RUSSIAN, raw, &mut data.episodes);
        }
        if data.episodes.is_empty() {
            // Turkish episodes
            extract_numbers(&RE_EPISODE_TR, raw, &mut data.episodes);
        }
        if data.episodes.is_empty() {
            for cap in RE_EPISODE_CROSSREF_RANGE.captures_iter(raw) {
                if let (Ok(start), Ok(end)) = (cap[2].parse::<i32>(), cap[3].parse::<i32>()) {
                    for ep in start..=end {
                        data.episodes.push(ep);
                    }
                }
            }
        }
        if data.episodes.is_empty() {
            // Cross-reference: episode is in group 2
            for cap in RE_EPISODE_CROSSREF.captures_iter(raw) {
                if let Ok(e) = cap[2].parse::<i32>() {
                    data.episodes.push(e);
                }
            }
        }
        // Catch consecutive episodes: S01E01E02E03
        for cap in RE_EPISODE_CONSECUTIVE.captures_iter(raw) {
            for ep_cap in RE_EP_NUM_BARE.captures_iter(&cap[1]) {
                if let Ok(n) = ep_cap[1].parse::<i32>() {
                    data.episodes.push(n);
                }
            }
        }
    }
    if data.episodes.is_empty()
        && (has_anime_context(&data, raw)
            || raw.contains("Complete")
            || raw.contains("complete")
            || raw.contains("Episodes")
            || raw.contains("episodes"))
    {
        extract_episode_ranges(&RE_EPISODE_RANGE_BARE_HYPHEN, raw, &mut data.episodes);
    }
    if data.episodes.is_empty()
        && let Some(cap) = RE_EPISODE_ANIME_BARE_SINGLE.captures_iter(raw).last()
    {
        let versioned = cap
            .get(0)
            .is_some_and(|m| m.as_str().to_ascii_lowercase().contains('v'));
        if (data.episode_code.is_some() || versioned || has_anime_context(&data, raw))
            && let Ok(n) = cap[1].parse::<i32>()
            && !(1900..=2099).contains(&n)
            && !matches!(n, 480 | 720 | 1080 | 2160)
        {
            data.episodes.push(n);
        }
    }
    data.episodes.sort_unstable();
    data.episodes.dedup();

    // Part number
    data.part = RE_PART.captures(raw).and_then(|c| c[1].parse().ok());

    // Audio, HDR, channels
    detect_audio(raw, &mut data.audio);
    detect_hdr(raw, &mut data.hdr);
    detect_channels(raw, &mut data.channels);

    // Bit depth (8, 10, 12)
    data.bit_depth = detect_bit_depth(raw);

    // 3D
    data.three_d = RE_3D.is_match(raw);

    // Complete (basic + collection patterns + year range already handled above)
    if !data.complete {
        data.complete = RE_COMPLETE.is_match(raw) || RE_COMPLETE_COLLECTION.is_match(raw);
    }

    // Volumes
    extract_numbers(&RE_VOLUME, raw, &mut data.volumes);
    data.volumes.sort_unstable();
    data.volumes.dedup();

    // Edition
    data.edition = RE_EDITION.captures(raw).and_then(|c| {
        let matched = c.get(1)?.as_str();
        let after = &raw[c.get(1)?.end()..];
        if matched.eq_ignore_ascii_case("uncut")
            && after
                .trim_start_matches(|ch: char| {
                    ch == '.' || ch == '_' || ch == '-' || ch.is_whitespace()
                })
                .to_ascii_lowercase()
                .starts_with("gems")
        {
            None
        } else {
            Some(normalize_edition(matched))
        }
    });

    // Region (R1-R9, PAL, NTSC, SECAM)
    data.region = RE_REGION_DISC
        .captures(raw)
        .or_else(|| RE_REGION.captures(raw))
        .map(|c| c[1].to_uppercase());

    // Network
    detect_network(raw, &mut data);

    // Languages
    for lp in LANG_PATTERNS.iter() {
        if lp.re.is_match(raw) {
            push_unique(&mut data.languages, lp.code);
        }
    }

    // Country
    data.country = RE_COUNTRY.captures(raw).map(|c| c[1].to_uppercase());

    // Anime extras (NCED, NCOP, NC, OVA, ED, OP)
    detect_anime_extras(raw, &mut data.extras);

    // Standard extras
    detect_standard_extras(raw, &mut data.extras);

    // Group detection — try multiple patterns
    data.group = RE_GROUP_DASH
        .captures(raw)
        .map(|c| c[1].to_string())
        .filter(|g| !is_false_group(g))
        .or_else(|| {
            RE_GROUP_PAREN
                .captures(raw)
                .map(|c| c[1].to_string())
                .filter(|g| !is_false_group(g))
        })
        .or_else(|| {
            RE_GROUP_BRACKET
                .captures(raw)
                .map(|c| c[1].to_string())
                .filter(|g| !is_false_group(g) && !bracket_group_is_site(raw, g))
        });

    // Boolean flags
    data.proper = RE_PROPER.is_match(raw);
    data.repack = RE_REPACK.is_match(raw);
    data.retail = RE_RETAIL.is_match(raw);
    data.upscaled = RE_UPSCALED.is_match(raw)
        || RE_UPSCALED_SPECIFIC.is_match(raw)
        || RE_UPSCALED_AI.is_match(raw);
    data.remastered = RE_REMASTERED.is_match(raw);
    data.extended = RE_EXTENDED.is_match(raw);
    data.converted = RE_CONVERTED.is_match(raw);
    data.unrated = RE_UNRATED.is_match(raw);
    data.uncensored = RE_UNCENSORED.is_match(raw);
    data.dubbed = RE_DUBBED.is_match(raw);
    data.subbed = RE_SUBBED.is_match(raw);
    data.hardcoded = RE_HARDCODED.is_match(raw);
    data.documentary = RE_DOCUMENTARY.is_match(raw);
    data.commentary = RE_COMMENTARY.is_match(raw);
    data.adult = RE_ADULT.is_match(raw);
    data.ppv = RE_PPV.is_match(raw) || RE_PPV_FIGHT.is_match(raw);
    data.scene = detect_scene(raw);

    // Site (additional patterns)
    if data.site.is_none() {
        data.site = detect_site_extra(raw);
    }

    // Trash detection
    data.trash = detect_trash(raw, &data);

    // Title extraction
    let title = extract_title(raw);
    data.normalized_title = normalize_title(&title);
    data.parsed_title = title;

    // Movie titles such as "Kill Bill: Vol. 1" should keep "Vol" in the title
    // without being treated as comic/manga volume metadata.
    if data.year.is_some()
        && data.seasons.is_empty()
        && data.episodes.is_empty()
        && data.parsed_title.to_lowercase().contains("vol")
    {
        data.volumes.clear();
    }

    // Anime detection (after group/episode_code/extras are populated)
    data.anime = detect_anime(raw, &data);

    if options.translate_languages {
        data.languages = translate_langs(&data.languages);
    }

    data
}
