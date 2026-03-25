mod detect;
mod languages;
pub(crate) mod patterns;
mod title;

use regex::Regex;
use serde::{Deserialize, Serialize};

use detect::{detect_network, detect_trash, is_false_group};
use languages::LANG_PATTERNS;
use patterns::*;
pub(crate) use title::normalize_title;
use title::{extract_title, normalize_edition};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ParsedData {
    pub raw_title: String,
    pub parsed_title: String,
    pub normalized_title: String,
    pub trash: bool,
    pub adult: bool,
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
    pub country: Option<String>,
    pub container: Option<String>,
    pub extension: Option<String>,
    pub extras: Vec<String>,
    pub torrent: bool,
    pub scene: bool,
}

impl ParsedData {
    pub fn media_type(&self) -> &str {
        if !self.seasons.is_empty() || !self.episodes.is_empty() {
            "show"
        } else {
            "movie"
        }
    }

    /// Merge another ParsedData into this one, preferring the other's non-empty/Some values.
    pub fn merge(&mut self, other: ParsedData) {
        if self.parsed_title.is_empty() {
            self.parsed_title = other.parsed_title;
            self.normalized_title = other.normalized_title;
        }
        if self.year.is_none() {
            self.year = other.year;
        }
        if self.resolution == "unknown" {
            self.resolution = other.resolution;
        }
        if self.quality.is_none() {
            self.quality = other.quality;
        }
        if self.codec.is_none() {
            self.codec = other.codec;
        }
        if self.bit_depth.is_none() {
            self.bit_depth = other.bit_depth;
        }
        if self.bitrate.is_none() {
            self.bitrate = other.bitrate;
        }
        if self.container.is_none() {
            self.container = other.container;
        }
        if self.extension.is_none() {
            self.extension = other.extension;
        }
        if self.group.is_none() {
            self.group = other.group;
        }
        if self.edition.is_none() {
            self.edition = other.edition;
        }
        if self.region.is_none() {
            self.region = other.region;
        }
        if self.network.is_none() {
            self.network = other.network;
        }
        if self.site.is_none() {
            self.site = other.site;
        }
        if self.country.is_none() {
            self.country = other.country;
        }

        // Extend vectors
        self.seasons.extend(other.seasons);
        self.seasons.sort();
        self.seasons.dedup();

        self.episodes.extend(other.episodes);
        self.episodes.sort();
        self.episodes.dedup();

        self.volumes.extend(other.volumes);
        self.volumes.sort();
        self.volumes.dedup();

        self.languages.extend(other.languages);
        self.languages.sort();
        self.languages.dedup();

        self.audio.extend(other.audio);
        self.audio.sort();
        self.audio.dedup();

        self.hdr.extend(other.hdr);
        self.hdr.sort();
        self.hdr.dedup();

        self.channels.extend(other.channels);
        self.channels.sort();
        self.channels.dedup();

        self.extras.extend(other.extras);
        self.extras.sort();
        self.extras.dedup();

        // Boolean Flags (OR-ed)
        self.trash |= other.trash;
        self.adult |= other.adult;
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

/// Extract numbers from a regex with capture groups 1 (required) and 2 (optional range end).
/// Pushes unique values into the target vec.
fn extract_numbers(re: &Regex, raw: &str, target: &mut Vec<i32>) {
    for cap in re.captures_iter(raw) {
        if let Ok(n1) = cap[1].parse::<i32>() {
            if !target.contains(&n1) {
                target.push(n1);
            }
            if let Some(m2) = cap.get(2) {
                if let Ok(n2) = m2.as_str().parse::<i32>() {
                    for n in n1..=n2 {
                        if !target.contains(&n) {
                            target.push(n);
                        }
                    }
                }
            }
        }
    }
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

    // Generic: use last occurrence (e.g., "720p...1080p" → "1080p")
    if let Some(last) = RE_RES_GENERIC.find_iter(raw).last() {
        let lower = last.as_str().to_lowercase();
        return if lower == "4k" { "2160p".to_string() } else { lower };
    }

    // Digit-based (e.g., "1080i")
    if let Some(cap) = RE_RES_DIGITS.captures(raw) {
        return match &cap[1] {
            "3840" | "2160" => "2160p",
            "1080" => "1080p",
            "720" => "720p",
            "576" => "576p",
            "480" => "480p",
            "360" => "360p",
            "240" => "240p",
            num => return format!("{num}p"),
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
    if RE_Q_PDTV.is_match(raw) {
        return Some("PDTV".into());
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
    checks
        .iter()
        .find(|(re, _)| re.is_match(raw))
        .map(|(_, name)| name.to_string())
}

/// Detect audio formats from raw title string.
fn detect_audio(raw: &str, audio: &mut Vec<String>) {
    if RE_AUDIO_HQ_CLEAN.is_match(raw) {
        push_unique(audio, "HQ Clean Audio");
    }
    if RE_AUDIO_DTS_LOSSLESS.is_match(raw) {
        push_unique(audio, "DTS Lossless");
    }
    if RE_AUDIO_DTS_LOSSY.is_match(raw) && !audio.contains(&"DTS Lossless".to_string()) {
        push_unique(audio, "DTS Lossy");
    }
    if RE_AUDIO_ATMOS.is_match(raw) {
        push_unique(audio, "Atmos");
    }
    if RE_AUDIO_TRUEHD.is_match(raw) {
        push_unique(audio, "TrueHD");
    }
    if RE_AUDIO_FLAC.is_match(raw) {
        push_unique(audio, "FLAC");
    }
    if RE_AUDIO_DD_PLUS.is_match(raw) {
        push_unique(audio, "Dolby Digital Plus");
    }
    if RE_AUDIO_DD.is_match(raw) && !audio.contains(&"Dolby Digital Plus".to_string()) {
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
    if RE_HDR_HDR.is_match(raw) && !hdr.contains(&"HDR10+".to_string()) {
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

pub fn parse(raw_title: &str) -> ParsedData {
    let mut data = ParsedData {
        raw_title: raw_title.to_string(),
        resolution: "unknown".to_string(),
        ..Default::default()
    };

    // Site detection (must be first - strip from working title)
    let working_title = if let Some(cap) = RE_SITE.captures(raw_title) {
        data.site = Some(cap[1].to_string());
        raw_title[cap.get(0).unwrap().end()..].to_string()
    } else {
        raw_title.to_string()
    };
    let raw = &working_title;

    // Simple captures
    data.extension = RE_EXTENSION.captures(raw).map(|c| c[1].to_lowercase());
    data.container = RE_CONTAINER.captures(raw).map(|c| c[1].to_lowercase());
    data.size = RE_SIZE.captures(raw).map(|c| c[1].to_string());
    data.bitrate = RE_BITRATE.captures(raw).map(|c| c[1].to_string());
    data.date = RE_DATE
        .captures(raw)
        .map(|c| format!("{}-{}-{}", &c[1], &c[2], &c[3]));
    data.year = RE_YEAR.captures(raw).and_then(|c| c[1].parse().ok());

    // Resolution, quality, codec
    data.resolution = detect_resolution(raw);
    data.quality = detect_quality(raw);
    data.codec = detect_codec(raw);

    // Seasons (try each pattern in priority order, stop when found)
    extract_numbers(&RE_SEASON_RANGE, raw, &mut data.seasons);
    if data.seasons.is_empty() {
        extract_numbers(&RE_SEASON_SE, raw, &mut data.seasons);
    }
    extract_numbers(&RE_SEASON_MULTI, raw, &mut data.seasons);
    extract_numbers(&RE_SEASON_FULL, raw, &mut data.seasons);
    // Cross-reference format: group 1 = season (no range)
    for cap in RE_EPISODE_CROSSREF.captures_iter(raw) {
        if let Ok(s) = cap[1].parse::<i32>() {
            if !data.seasons.contains(&s) {
                data.seasons.push(s);
            }
        }
    }
    data.seasons.sort();
    data.seasons.dedup();

    // Episodes (try each pattern in priority order, stop when found)
    extract_numbers(&RE_EPISODE_RANGE, raw, &mut data.episodes);
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
        // Cross-reference: episode is in group 2, but extract_numbers reads group 1.
        // Handle manually.
        for cap in RE_EPISODE_CROSSREF.captures_iter(raw) {
            if let Ok(e) = cap[2].parse::<i32>() {
                if !data.episodes.contains(&e) {
                    data.episodes.push(e);
                }
            }
        }
    }
    data.episodes.sort();
    data.episodes.dedup();

    // Audio, HDR, channels
    detect_audio(raw, &mut data.audio);
    detect_hdr(raw, &mut data.hdr);
    detect_channels(raw, &mut data.channels);

    // Bit depth
    if RE_BIT_DEPTH.is_match(raw) || RE_BIT_DEPTH_HDR10.is_match(raw) {
        data.bit_depth = Some("10bit".to_string());
    }

    // Simple flags
    data.three_d = RE_3D.is_match(raw);
    data.complete = RE_COMPLETE.is_match(raw);

    // Volumes
    extract_numbers(&RE_VOLUME, raw, &mut data.volumes);
    data.volumes.sort();
    data.volumes.dedup();

    // Edition
    data.edition = RE_EDITION.captures(raw).map(|c| normalize_edition(&c[1]));

    // Region
    data.region = RE_REGION.captures(raw).map(|c| c[1].to_string());

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

    // Extras
    for cap in RE_EXTRAS.captures_iter(raw) {
        push_unique(&mut data.extras, &cap[1]);
    }

    // Group
    data.group = RE_GROUP_DASH
        .captures(raw)
        .map(|c| c[1].to_string())
        .filter(|g| !is_false_group(g))
        .or_else(|| {
            RE_GROUP_BRACKET
                .captures(raw)
                .map(|c| c[1].to_string())
                .filter(|g| !is_false_group(g))
        });

    // Boolean flags
    data.proper = RE_PROPER.is_match(raw);
    data.repack = RE_REPACK.is_match(raw);
    data.retail = RE_RETAIL.is_match(raw);
    data.upscaled = RE_UPSCALED.is_match(raw);
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
    data.ppv = RE_PPV.is_match(raw);
    data.torrent = raw.to_lowercase().contains(".torrent");
    data.scene = RE_SCENE.is_match(raw);

    // Trash detection
    data.trash = detect_trash(raw, &data);

    // Title extraction
    let title = extract_title(raw);
    data.normalized_title = normalize_title(&title);
    data.parsed_title = title;

    data
}
