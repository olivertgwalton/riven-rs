use regex::Regex;
use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

fn default_hd_profile() -> QualityProfile {
    QualityProfile::Hd
}

/// Quality profile preset — determines sensible defaults for resolutions,
/// resolution tiebreakers, fetch rules, and score thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityProfile {
    /// 4K / Ultra HD — prioritises 2160p REMUX, Dolby Vision, lossless audio.
    /// Disables lower-quality sources and rips.
    UltraHd,
    /// 1080p / HD — balanced defaults for HD content. (default)
    #[default]
    Hd,
    /// 720p / Standard — permissive settings; enables more source types and
    /// a relaxed score threshold.
    Standard,
}

impl QualityProfile {
    pub fn id(self) -> &'static str {
        match self {
            Self::UltraHd => "ultra_hd",
            Self::Hd => "hd",
            Self::Standard => "standard",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::UltraHd => "4K / Ultra HD",
            Self::Hd => "Full HD",
            Self::Standard => "720p / Standard",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::UltraHd => {
                "Prioritises 2160p REMUX, Dolby Vision, and lossless audio. Disables rips and low-quality sources."
            }
            Self::Hd => {
                "Balanced defaults for FHD content. Priorities compatibility and availability."
            }
            Self::Standard => "Lower quality defaults. Relaxed thresholds for easier streaming.",
        }
    }

    pub const ALL: [QualityProfile; 3] = [Self::UltraHd, Self::Hd, Self::Standard];

    /// Return a [`RankSettings`] pre-configured for this profile.
    /// Lists (`require`, `exclude`, `preferred`) and `languages` are left
    /// at their zero-value defaults; callers may overlay them as needed.
    pub fn base_settings(self) -> RankSettings {
        match self {
            Self::UltraHd => ultra_hd_settings(),
            Self::Hd => hd_settings(),
            Self::Standard => standard_settings(),
        }
    }
}

// ── Built-in profile definitions ─────────────────────────────────────────────
//
// Each profile is a pure function returning a fully-specified `RankSettings`.
// Use `..T::default()` to inherit the HD baseline for anything not overridden.
// Adding a new profile = add a variant + add a function here + add a match arm.

/// 4K / Ultra HD — maximum quality, lossless audio, Dolby Vision.
fn ultra_hd_settings() -> RankSettings {
    RankSettings {
        profile: QualityProfile::UltraHd,
        resolutions: ResolutionSettings {
            r2160p: true,
            r1080p: true,
            r720p: false,
            r480p: false,
            r360p: false,
            unknown: false,
        },
        resolution_ranks: ResolutionRanks {
            r2160p: 100,
            r1440p: 50,
            r1080p: 10,
            r720p: 1,
            r480p: 0,
            r360p: 0,
            unknown: 0,
        },
        custom_ranks: CustomRanksConfig {
            // Source: REMUX > BluRay > WEB-DL > WEB; ban HDTV/DVD
            quality: QualityRanks {
                remux: CustomRank::scored(true, 10000),
                bluray: CustomRank::scored(true, 5000),
                webdl: CustomRank::scored(true, 2000),
                web: CustomRank::scored(true, 1000),
                hevc: CustomRank::scored(true, 1500),
                av1: CustomRank::scored(true, 1000),
                avc: CustomRank::scored(true, 300),
                hdtv: CustomRank::scored(false, -10000),
                dvd: CustomRank::scored(false, -10000),
                ..QualityRanks::default()
            },
            // HDR: Dolby Vision > HDR10+ > HDR; heavily penalise SDR
            hdr: HdrRanks {
                dolby_vision: CustomRank::scored(true, 5000),
                hdr10plus: CustomRank::scored(true, 4500),
                hdr: CustomRank::scored(true, 3500),
                sdr: CustomRank::scored(true, -3000),
                bit10: CustomRank::scored(true, 300),
            },
            // Audio: lossless first; penalise compressed/stereo/mono
            audio: AudioRanks {
                truehd: CustomRank::scored(true, 4000),
                atmos: CustomRank::scored(true, 3500),
                dts_lossless: CustomRank::scored(true, 3000),
                flac: CustomRank::scored(true, 1500),
                dolby_digital_plus: CustomRank::scored(true, 500),
                dts_lossy: CustomRank::scored(true, 400),
                dolby_digital: CustomRank::scored(true, 200),
                aac: CustomRank::scored(true, 100),
                surround: CustomRank::scored(true, 200),
                stereo: CustomRank::scored(true, -500),
                mono: CustomRank::scored(false, -2000),
                mp3: CustomRank::scored(false, -2000),
            },
            extras: ExtrasRanks {
                edition: CustomRank::scored(true, 300),
                retail: CustomRank::scored(true, 200),
                proper: CustomRank::scored(true, 100),
                repack: CustomRank::scored(true, 100),
                dubbed: CustomRank::scored(true, -2000),
                ..ExtrasRanks::default()
            },
            rips: RipsRanks::all_disabled(),
            ..CustomRanksConfig::default()
        },
        ..RankSettings::default()
    }
}

/// Full HD — 1080p SDR only. No HDR, no 4K, no 720p fallback.
fn hd_settings() -> RankSettings {
    RankSettings {
        profile: QualityProfile::Hd,
        resolutions: ResolutionSettings {
            r2160p: false,
            r1080p: true,
            r720p: false,
            r480p: false,
            r360p: false,
            unknown: false,
        },
        resolution_ranks: ResolutionRanks {
            r2160p: 0,
            r1440p: 0,
            r1080p: 100,
            r720p: 0,
            r480p: 0,
            r360p: 0,
            unknown: 0,
        },
        custom_ranks: CustomRanksConfig {
            hdr: HdrRanks {
                dolby_vision: CustomRank::scored(false, -10000),
                hdr10plus: CustomRank::scored(false, -10000),
                hdr: CustomRank::scored(false, -5000),
                sdr: CustomRank::scored(true, 100),
                bit10: CustomRank::scored(false, 0),
            },
            ..CustomRanksConfig::default()
        },
        ..RankSettings::default()
    }
}

/// 720p / Standard — permissive; availability over purity, rips enabled.
fn standard_settings() -> RankSettings {
    RankSettings {
        profile: QualityProfile::Standard,
        resolutions: ResolutionSettings {
            r2160p: false,
            r1080p: false,
            r720p: true,
            r480p: true,
            r360p: false,
            unknown: true,
        },
        resolution_ranks: ResolutionRanks {
            r2160p: 0,
            r1440p: 1,
            r1080p: 2,
            r720p: 7,
            r480p: 5,
            r360p: 3,
            unknown: 4,
        },
        custom_ranks: CustomRanksConfig {
            // Source: more equal; HDTV/DVD enabled, REMUX disabled
            quality: QualityRanks {
                webdl: CustomRank::scored(true, 400),
                bluray: CustomRank::scored(true, 350),
                web: CustomRank::scored(true, 300),
                hdtv: CustomRank::scored(true, 200),
                dvd: CustomRank::scored(true, 100),
                remux: CustomRank::scored(false, 200),
                hevc: CustomRank::scored(true, 200),
                avc: CustomRank::scored(true, 200),
                ..QualityRanks::default()
            },
            // HDR: SDR only; all HDR formats disabled
            hdr: HdrRanks {
                dolby_vision: CustomRank::scored(false, -10000),
                hdr10plus: CustomRank::scored(false, -10000),
                hdr: CustomRank::scored(false, -10000),
                sdr: CustomRank::scored(true, 500),
                bit10: CustomRank::scored(false, -5000),
            },
            // Audio: decent quality, less strict
            audio: AudioRanks {
                atmos: CustomRank::scored(true, 400),
                truehd: CustomRank::scored(true, 400),
                dts_lossless: CustomRank::scored(true, 350),
                flac: CustomRank::scored(true, 200),
                dolby_digital_plus: CustomRank::scored(true, 150),
                dts_lossy: CustomRank::scored(true, 150),
                dolby_digital: CustomRank::scored(true, 100),
                aac: CustomRank::scored(true, 100),
                ..AudioRanks::default()
            },
            extras: ExtrasRanks {
                proper: CustomRank::scored(true, 20),
                repack: CustomRank::scored(true, 20),
                ..ExtrasRanks::default()
            },
            // Rips enabled at lower scores
            rips: RipsRanks {
                webrip: CustomRank::scored(true, -400),
                brrip: CustomRank::scored(true, -500),
                hdrip: CustomRank::scored(true, -600),
                ..RipsRanks::default()
            },
            ..CustomRanksConfig::default()
        },
        options: RankOptions {
            remove_ranks_under: -50000,
            ..RankOptions::default()
        },
        ..RankSettings::default()
    }
}

fn default_title_similarity() -> f64 {
    0.85
}

fn default_remove_ranks_under() -> i64 {
    -10000
}

/// Custom rank for a specific attribute.
///
/// `rank` is optional: `None` means "use the built-in default score from
/// `crate::defaults`"; `Some(n)` overrides it with `n`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomRank {
    #[serde(default = "default_true")]
    pub fetch: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<i64>,
}

impl CustomRank {
    fn new(fetch: bool) -> Self {
        Self { fetch, rank: None }
    }

    fn scored(fetch: bool, rank: i64) -> Self {
        Self {
            fetch,
            rank: Some(rank),
        }
    }

    /// Returns the custom rank if set, otherwise the built-in default score.
    #[inline]
    pub fn resolve(&self, default: i64) -> i64 {
        self.rank.unwrap_or(default)
    }
}

/// Quality custom ranks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QualityRanks {
    pub av1: CustomRank,
    pub avc: CustomRank,
    pub bluray: CustomRank,
    pub dvd: CustomRank,
    pub hdtv: CustomRank,
    pub hevc: CustomRank,
    pub mpeg: CustomRank,
    pub remux: CustomRank,
    pub vhs: CustomRank,
    pub web: CustomRank,
    pub webdl: CustomRank,
    pub webmux: CustomRank,
    pub xvid: CustomRank,
}

impl Default for QualityRanks {
    fn default() -> Self {
        Self {
            av1: CustomRank::new(false),
            avc: CustomRank::new(true),
            bluray: CustomRank::new(true),
            dvd: CustomRank::new(false),
            hdtv: CustomRank::new(true),
            hevc: CustomRank::new(true),
            mpeg: CustomRank::new(false),
            remux: CustomRank::new(false),
            vhs: CustomRank::new(false),
            web: CustomRank::new(true),
            webdl: CustomRank::new(true),
            webmux: CustomRank::new(false),
            xvid: CustomRank::new(false),
        }
    }
}

/// Rips custom ranks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RipsRanks {
    pub bdrip: CustomRank,
    pub brrip: CustomRank,
    pub dvdrip: CustomRank,
    pub hdrip: CustomRank,
    pub ppvrip: CustomRank,
    pub satrip: CustomRank,
    pub tvrip: CustomRank,
    pub uhdrip: CustomRank,
    pub vhsrip: CustomRank,
    pub webdlrip: CustomRank,
    pub webrip: CustomRank,
}

impl Default for RipsRanks {
    fn default() -> Self {
        Self {
            bdrip: CustomRank::new(false),
            brrip: CustomRank::new(true),
            dvdrip: CustomRank::new(false),
            hdrip: CustomRank::new(true),
            ppvrip: CustomRank::new(false),
            satrip: CustomRank::new(false),
            tvrip: CustomRank::new(false),
            uhdrip: CustomRank::new(false),
            vhsrip: CustomRank::new(false),
            webdlrip: CustomRank::new(false),
            webrip: CustomRank::new(true),
        }
    }
}

impl RipsRanks {
    pub fn all_disabled() -> Self {
        Self {
            bdrip: CustomRank::new(false),
            brrip: CustomRank::new(false),
            dvdrip: CustomRank::new(false),
            hdrip: CustomRank::new(false),
            ppvrip: CustomRank::new(false),
            satrip: CustomRank::new(false),
            tvrip: CustomRank::new(false),
            uhdrip: CustomRank::new(false),
            vhsrip: CustomRank::new(false),
            webdlrip: CustomRank::new(false),
            webrip: CustomRank::new(false),
        }
    }
}

/// HDR custom ranks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HdrRanks {
    pub bit10: CustomRank,
    pub dolby_vision: CustomRank,
    pub hdr: CustomRank,
    pub hdr10plus: CustomRank,
    pub sdr: CustomRank,
}

impl Default for HdrRanks {
    fn default() -> Self {
        Self {
            bit10: CustomRank::new(true),
            dolby_vision: CustomRank::new(false),
            hdr: CustomRank::new(true),
            hdr10plus: CustomRank::new(true),
            sdr: CustomRank::new(true),
        }
    }
}

/// Audio custom ranks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AudioRanks {
    pub aac: CustomRank,
    pub atmos: CustomRank,
    pub dolby_digital: CustomRank,
    pub dolby_digital_plus: CustomRank,
    pub dts_lossy: CustomRank,
    pub dts_lossless: CustomRank,
    pub flac: CustomRank,
    pub mono: CustomRank,
    pub mp3: CustomRank,
    pub stereo: CustomRank,
    pub surround: CustomRank,
    pub truehd: CustomRank,
}

impl Default for AudioRanks {
    fn default() -> Self {
        Self {
            aac: CustomRank::new(true),
            atmos: CustomRank::new(true),
            dolby_digital: CustomRank::new(true),
            dolby_digital_plus: CustomRank::new(true),
            dts_lossy: CustomRank::new(true),
            dts_lossless: CustomRank::new(true),
            flac: CustomRank::new(true),
            mono: CustomRank::new(false),
            mp3: CustomRank::new(false),
            stereo: CustomRank::new(true),
            surround: CustomRank::new(true),
            truehd: CustomRank::new(true),
        }
    }
}

/// Extras custom ranks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExtrasRanks {
    pub three_d: CustomRank,
    pub converted: CustomRank,
    pub commentary: CustomRank,
    pub documentary: CustomRank,
    pub dubbed: CustomRank,
    pub edition: CustomRank,
    pub hardcoded: CustomRank,
    pub network: CustomRank,
    pub proper: CustomRank,
    pub repack: CustomRank,
    pub retail: CustomRank,
    pub site: CustomRank,
    pub subbed: CustomRank,
    pub upscaled: CustomRank,
    pub scene: CustomRank,
    pub uncensored: CustomRank,
}

impl Default for ExtrasRanks {
    fn default() -> Self {
        Self {
            three_d: CustomRank::new(false),
            converted: CustomRank::new(false),
            commentary: CustomRank::new(false),
            documentary: CustomRank::new(false),
            dubbed: CustomRank::new(true),
            edition: CustomRank::new(true),
            hardcoded: CustomRank::new(true),
            network: CustomRank::new(true),
            proper: CustomRank::new(true),
            repack: CustomRank::new(true),
            retail: CustomRank::new(true),
            site: CustomRank::new(false),
            subbed: CustomRank::new(true),
            upscaled: CustomRank::new(false),
            scene: CustomRank::new(true),
            uncensored: CustomRank::new(true),
        }
    }
}

/// Trash custom ranks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TrashRanks {
    pub cam: CustomRank,
    pub clean_audio: CustomRank,
    pub pdtv: CustomRank,
    pub r5: CustomRank,
    pub screener: CustomRank,
    pub size: CustomRank,
    pub telecine: CustomRank,
    pub telesync: CustomRank,
}

impl Default for TrashRanks {
    fn default() -> Self {
        Self {
            cam: CustomRank::new(false),
            clean_audio: CustomRank::new(false),
            pdtv: CustomRank::new(false),
            r5: CustomRank::new(false),
            screener: CustomRank::new(false),
            size: CustomRank::new(false),
            telecine: CustomRank::new(false),
            telesync: CustomRank::new(false),
        }
    }
}

/// All custom rank categories
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CustomRanksConfig {
    pub quality: QualityRanks,
    pub rips: RipsRanks,
    pub hdr: HdrRanks,
    pub audio: AudioRanks,
    pub extras: ExtrasRanks,
    pub trash: TrashRanks,
}

impl CustomRanksConfig {
    /// Look up the CustomRank for a quality/rip/trash string.
    /// Returns None for unknown quality strings.
    pub fn quality_rank(&self, quality: &str) -> Option<&CustomRank> {
        match quality {
            "WEB" => Some(&self.quality.web),
            "WEB-DL" => Some(&self.quality.webdl),
            "BluRay" => Some(&self.quality.bluray),
            "HDTV" => Some(&self.quality.hdtv),
            "VHS" => Some(&self.quality.vhs),
            "WEBMux" => Some(&self.quality.webmux),
            "BluRay REMUX" | "REMUX" => Some(&self.quality.remux),
            "DVD" => Some(&self.quality.dvd),
            "WEBRip" => Some(&self.rips.webrip),
            "WEB-DLRip" => Some(&self.rips.webdlrip),
            "UHDRip" => Some(&self.rips.uhdrip),
            "HDRip" => Some(&self.rips.hdrip),
            "DVDRip" => Some(&self.rips.dvdrip),
            "BDRip" => Some(&self.rips.bdrip),
            "BRRip" => Some(&self.rips.brrip),
            "VHSRip" => Some(&self.rips.vhsrip),
            "PPVRip" => Some(&self.rips.ppvrip),
            "SATRip" => Some(&self.rips.satrip),
            "TVRip" => Some(&self.rips.tvrip),
            "TeleCine" => Some(&self.trash.telecine),
            "TeleSync" => Some(&self.trash.telesync),
            "SCR" => Some(&self.trash.screener),
            "R5" => Some(&self.trash.r5),
            "CAM" => Some(&self.trash.cam),
            "PDTV" => Some(&self.trash.pdtv),
            _ => None,
        }
    }

    /// Look up the CustomRank for a codec string.
    pub fn codec_rank(&self, codec: &str) -> Option<&CustomRank> {
        match codec {
            "avc" => Some(&self.quality.avc),
            "hevc" => Some(&self.quality.hevc),
            "xvid" => Some(&self.quality.xvid),
            "av1" => Some(&self.quality.av1),
            "mpeg" => Some(&self.quality.mpeg),
            _ => None,
        }
    }

    /// Look up the CustomRank for an audio string.
    pub fn audio_rank(&self, audio: &str) -> Option<&CustomRank> {
        match audio {
            "AAC" => Some(&self.audio.aac),
            "Atmos" => Some(&self.audio.atmos),
            "Dolby Digital" => Some(&self.audio.dolby_digital),
            "Dolby Digital Plus" => Some(&self.audio.dolby_digital_plus),
            "DTS Lossy" => Some(&self.audio.dts_lossy),
            "DTS Lossless" => Some(&self.audio.dts_lossless),
            "FLAC" => Some(&self.audio.flac),
            "MP3" => Some(&self.audio.mp3),
            "TrueHD" => Some(&self.audio.truehd),
            "HQ Clean Audio" => Some(&self.trash.clean_audio),
            _ => None,
        }
    }

    /// Look up the CustomRank for an HDR string.
    pub fn hdr_rank(&self, hdr: &str) -> Option<&CustomRank> {
        match hdr {
            "DV" => Some(&self.hdr.dolby_vision),
            "HDR" => Some(&self.hdr.hdr),
            "HDR10+" => Some(&self.hdr.hdr10plus),
            "SDR" => Some(&self.hdr.sdr),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ResolutionSettings {
    pub r2160p: bool,
    pub r1080p: bool,
    pub r720p: bool,
    pub r480p: bool,
    pub r360p: bool,
    pub unknown: bool,
}

impl Default for ResolutionSettings {
    fn default() -> Self {
        Self {
            r2160p: false,
            r1080p: true,
            r720p: true,
            r480p: false,
            r360p: false,
            unknown: true,
        }
    }
}

/// Tiebreaker scores used when two streams have identical quality rank scores.
/// Higher value = higher priority when ranks are equal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ResolutionRanks {
    pub r2160p: i32,
    pub r1440p: i32,
    pub r1080p: i32,
    pub r720p: i32,
    pub r480p: i32,
    pub r360p: i32,
    pub unknown: i32,
}

impl Default for ResolutionRanks {
    fn default() -> Self {
        Self {
            r2160p: 7,
            r1440p: 6,
            r1080p: 5,
            r720p: 4,
            r480p: 3,
            r360p: 2,
            unknown: 1,
        }
    }
}

impl ResolutionRanks {
    pub fn rank_for(&self, resolution: &str) -> i32 {
        match resolution {
            "2160p" => self.r2160p,
            "1440p" => self.r1440p,
            "1080p" => self.r1080p,
            "720p" => self.r720p,
            "480p" => self.r480p,
            "360p" => self.r360p,
            _ => self.unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RankOptions {
    #[serde(default = "default_title_similarity")]
    pub title_similarity: f64,
    #[serde(default = "default_true")]
    pub remove_all_trash: bool,
    #[serde(default = "default_remove_ranks_under")]
    pub remove_ranks_under: i64,
    #[serde(default)]
    pub remove_unknown_languages: bool,
    #[serde(default = "default_true")]
    pub allow_english_in_languages: bool,
    #[serde(default = "default_true")]
    pub remove_adult_content: bool,
    /// When `true` (default), fetch checks fail as soon as one check fails.
    /// When `false`, all checks run and every failure is collected — useful for
    /// diagnostics or building a full list of reasons a torrent was rejected.
    #[serde(default = "default_true")]
    pub enable_fetch_speed_mode: bool,
}

impl Default for RankOptions {
    fn default() -> Self {
        Self {
            title_similarity: default_title_similarity(),
            remove_all_trash: true,
            remove_ranks_under: -10000,
            remove_unknown_languages: false,
            allow_english_in_languages: true,
            remove_adult_content: true,
            enable_fetch_speed_mode: true,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LanguageSettings {
    pub required: Vec<String>,
    pub allowed: Vec<String>,
    pub exclude: Vec<String>,
    pub preferred: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RankSettings {
    /// Active quality profile. Stored as metadata; does not auto-apply on the
    /// backend — call [`QualityProfile::base_settings`] to build preset defaults.
    #[serde(default = "default_hd_profile", rename = "quality_profile")]
    pub profile: QualityProfile,
    pub require: Vec<String>,
    pub exclude: Vec<String>,
    pub preferred: Vec<String>,
    /// Pre-compiled `require` patterns. Populated by [`RankSettings::prepare`].
    /// Not serialised — rebuilt from `require` on each call to `prepare`.
    #[serde(skip)]
    pub require_compiled: Vec<Regex>,
    /// Pre-compiled `exclude` patterns. Populated by [`RankSettings::prepare`].
    #[serde(skip)]
    pub exclude_compiled: Vec<Regex>,
    /// Pre-compiled `preferred` patterns. Populated by [`RankSettings::prepare`].
    pub resolutions: ResolutionSettings,
    pub resolution_ranks: ResolutionRanks,
    #[serde(skip)]
    pub preferred_compiled: Vec<Regex>,
    pub options: RankOptions,
    pub languages: LanguageSettings,
    pub custom_ranks: CustomRanksConfig,
}

impl Default for RankSettings {
    fn default() -> Self {
        Self {
            profile: QualityProfile::Hd,
            require: Vec::new(),
            exclude: Vec::new(),
            preferred: Vec::new(),
            require_compiled: Vec::new(),
            exclude_compiled: Vec::new(),
            resolutions: ResolutionSettings::default(),
            resolution_ranks: ResolutionRanks::default(),
            preferred_compiled: Vec::new(),
            options: RankOptions::default(),
            languages: LanguageSettings::default(),
            custom_ranks: CustomRanksConfig::default(),
        }
    }
}

fn compile_patterns(patterns: &[String]) -> Vec<Regex> {
    patterns.iter().filter_map(|p| Regex::new(p).ok()).collect()
}

impl RankSettings {
    /// Compile all regex pattern lists into their pre-compiled counterparts.
    ///
    /// Call this once after deserialising settings and before passing them to
    /// the ranking pipeline. The compiled fields are skipped by serde, so they
    /// must be rebuilt on every load.
    pub fn prepare(mut self) -> Self {
        self.require_compiled = compile_patterns(&self.require);
        self.exclude_compiled = compile_patterns(&self.exclude);
        self.preferred_compiled = compile_patterns(&self.preferred);
        self
    }
}
