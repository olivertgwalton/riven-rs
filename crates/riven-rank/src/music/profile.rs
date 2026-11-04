//! Music quality profiles: which copies of an album are acceptable and which
//! is best, as a film profile says of resolutions and sources.

use serde::{Deserialize, Serialize};

use super::{AudioFormat, AudioSource, LossyQuality, MusicRelease};

/// One value for each kind of copy a release can be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PerFormat<T> {
    pub flac: T,
    pub alac: T,
    pub wav: T,
    pub dsd: T,
    pub mp3_320: T,
    pub mp3_v0: T,
    /// V2 and constant bitrates below 320.
    pub mp3_lower: T,
    /// An MP3 whose name does not say its bitrate.
    pub mp3: T,
    pub aac: T,
    pub opus: T,
    pub vorbis: T,
    /// A release whose name says nothing of its format.
    pub untagged: T,
}

impl<T: Copy> PerFormat<T> {
    pub const KEYS: [&'static str; 12] = [
        "flac",
        "alac",
        "wav",
        "dsd",
        "mp3_320",
        "mp3_v0",
        "mp3_lower",
        "mp3",
        "aac",
        "opus",
        "vorbis",
        "untagged",
    ];

    fn of(&self, release: &MusicRelease) -> T {
        match release.format {
            Some(AudioFormat::Flac) => self.flac,
            Some(AudioFormat::Alac) => self.alac,
            Some(AudioFormat::Wav) => self.wav,
            Some(AudioFormat::Dsd) => self.dsd,
            Some(AudioFormat::Mp3) => match release.lossy_quality {
                Some(LossyQuality::Cbr(kbps)) if kbps >= 320 => self.mp3_320,
                Some(LossyQuality::V0) => self.mp3_v0,
                Some(LossyQuality::Cbr(_) | LossyQuality::V2) => self.mp3_lower,
                None => self.mp3,
            },
            Some(AudioFormat::Aac) => self.aac,
            Some(AudioFormat::Opus) => self.opus,
            Some(AudioFormat::Vorbis) => self.vorbis,
            None => self.untagged,
        }
    }
}

/// One value for each source a release can be ripped from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PerSource<T> {
    pub cd: T,
    pub web: T,
    pub vinyl: T,
    pub sacd: T,
    pub cassette: T,
    /// A release whose name does not say where it came from.
    pub unknown: T,
}

impl<T: Copy> PerSource<T> {
    pub const KEYS: [&'static str; 6] = ["cd", "web", "vinyl", "sacd", "cassette", "unknown"];

    fn of(&self, release: &MusicRelease) -> T {
        match release.source {
            Some(AudioSource::Cd) => self.cd,
            Some(AudioSource::Web) => self.web,
            Some(AudioSource::Vinyl) => self.vinyl,
            Some(AudioSource::Sacd) => self.sacd,
            Some(AudioSource::Cassette) => self.cassette,
            None => self.unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BitDepthPreference {
    /// 16-bit lossless first: what plays everywhere, a third of the size.
    #[default]
    CdQuality,
    /// 24-bit lossless first.
    HiRes,
    /// Neither is preferred.
    Either,
}

impl BitDepthPreference {
    pub const ALL: [Self; 3] = [Self::CdQuality, Self::HiRes, Self::Either];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CdQuality => "cd_quality",
            Self::HiRes => "hi_res",
            Self::Either => "either",
        }
    }
}

/// What a bit depth preference adds to a lossless release that meets it.
const BIT_DEPTH_BONUS: i64 = 10;
/// What naming the album exactly adds over naming it only in part.
const EXACT_TITLE_BONUS: i64 = 3;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MusicRankSettings {
    /// Which formats are accepted.
    pub formats: PerFormat<bool>,
    pub format_ranks: PerFormat<i64>,
    /// Which sources are accepted.
    pub sources: PerSource<bool>,
    pub source_ranks: PerSource<i64>,
    pub bit_depth: BitDepthPreference,
}

impl Default for MusicRankSettings {
    fn default() -> Self {
        MusicProfile::Best.base_settings()
    }
}

impl MusicRankSettings {
    /// A profile's settings with what was saved for it laid over them: saved
    /// settings name only what was changed.
    pub fn from_overrides(base: Self, overrides: &serde_json::Value) -> serde_json::Result<Self> {
        let mut merged = serde_json::to_value(base)?;
        overlay(&mut merged, overrides);
        serde_json::from_value(merged)
    }

    /// The release's rank under these settings; `None` when they refuse it.
    pub fn rank(&self, release: &MusicRelease) -> Option<i64> {
        if !self.formats.of(release) || !self.sources.of(release) {
            return None;
        }
        let lossless = release.format.is_some_and(AudioFormat::is_lossless);
        let depth = match (self.bit_depth, release.bit_depth) {
            (BitDepthPreference::HiRes, Some(24)) => BIT_DEPTH_BONUS,
            (BitDepthPreference::CdQuality, Some(16) | None) if lossless => BIT_DEPTH_BONUS,
            _ => 0,
        };
        let exact = if release.names_album_exactly {
            EXACT_TITLE_BONUS
        } else {
            0
        };
        Some(self.format_ranks.of(release) + self.source_ranks.of(release) + depth + exact)
    }
}

/// The built-in music profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MusicProfile {
    /// Lossless at CD quality first, anything else when nothing lossless is
    /// posted.
    Best,
    /// Lossless only, 24-bit first.
    HiRes,
    /// Lossy only, for small files: MP3 320 and V0, AAC and Opus.
    Portable,
}

impl MusicProfile {
    pub const ALL: [Self; 3] = [Self::Best, Self::HiRes, Self::Portable];

    pub const fn id(self) -> &'static str {
        match self {
            Self::Best => "music_best",
            Self::HiRes => "music_hi_res",
            Self::Portable => "music_portable",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Best => "Best available",
            Self::HiRes => "Hi-res lossless",
            Self::Portable => "Portable",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::Best => "Lossless at CD quality first, lossy when nothing lossless is posted.",
            Self::HiRes => "Lossless only, 24-bit first.",
            Self::Portable => "Lossy only, for small files: MP3 320, V0, AAC and Opus.",
        }
    }

    pub fn base_settings(self) -> MusicRankSettings {
        let format_ranks = PerFormat {
            flac: 100,
            alac: 100,
            wav: 80,
            dsd: 70,
            mp3_320: 50,
            mp3_v0: 48,
            mp3_lower: 20,
            mp3: 35,
            aac: 45,
            opus: 40,
            vorbis: 38,
            untagged: 10,
        };
        let source_ranks = PerSource {
            cd: 5,
            web: 5,
            vinyl: -10,
            sacd: 0,
            cassette: -20,
            unknown: 0,
        };
        let every_source = PerSource {
            cd: true,
            web: true,
            vinyl: true,
            sacd: true,
            cassette: true,
            unknown: true,
        };
        let lossless = PerFormat {
            flac: true,
            alac: true,
            wav: true,
            dsd: true,
            ..PerFormat::default()
        };
        let lossy = PerFormat {
            mp3_320: true,
            mp3_v0: true,
            mp3: true,
            aac: true,
            opus: true,
            vorbis: true,
            ..PerFormat::default()
        };
        match self {
            Self::Best => MusicRankSettings {
                formats: PerFormat {
                    flac: true,
                    alac: true,
                    wav: true,
                    dsd: true,
                    mp3_lower: true,
                    untagged: true,
                    ..lossy
                },
                format_ranks,
                sources: every_source,
                source_ranks,
                bit_depth: BitDepthPreference::CdQuality,
            },
            Self::HiRes => MusicRankSettings {
                formats: lossless,
                format_ranks,
                sources: every_source,
                source_ranks,
                bit_depth: BitDepthPreference::HiRes,
            },
            Self::Portable => MusicRankSettings {
                formats: lossy,
                format_ranks,
                sources: every_source,
                source_ranks,
                bit_depth: BitDepthPreference::Either,
            },
        }
    }
}

fn overlay(base: &mut serde_json::Value, over: &serde_json::Value) {
    match (base, over) {
        (serde_json::Value::Object(base), serde_json::Value::Object(over)) => {
            for (key, value) in over {
                match base.get_mut(key) {
                    Some(slot) => overlay(slot, value),
                    None => {
                        base.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (slot, value) => *slot = value.clone(),
    }
}
