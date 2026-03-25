use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

fn default_title_similarity() -> f64 {
    0.7
}

fn default_remove_ranks_under() -> i64 {
    -10000
}

/// Custom rank for a specific attribute
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomRank {
    #[serde(default = "default_true")]
    pub fetch: bool,
    #[serde(default)]
    pub use_custom_rank: bool,
    #[serde(default)]
    pub rank: i64,
}

impl CustomRank {
    fn new(fetch: bool) -> Self {
        Self {
            fetch,
            use_custom_rank: false,
            rank: 0,
        }
    }

    /// Returns the custom rank if enabled, otherwise the default score.
    pub fn resolve(&self, default: i64) -> i64 {
        if self.use_custom_rank {
            self.rank
        } else {
            default
        }
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CustomRanksConfig {
    pub quality: QualityRanks,
    pub rips: RipsRanks,
    pub hdr: HdrRanks,
    pub audio: AudioRanks,
    pub extras: ExtrasRanks,
    pub trash: TrashRanks,
}

impl Default for CustomRanksConfig {
    fn default() -> Self {
        Self {
            quality: QualityRanks::default(),
            rips: RipsRanks::default(),
            hdr: HdrRanks::default(),
            audio: AudioRanks::default(),
            extras: ExtrasRanks::default(),
            trash: TrashRanks::default(),
        }
    }
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
    pub require: Vec<String>,
    pub exclude: Vec<String>,
    pub preferred: Vec<String>,
    pub resolutions: ResolutionSettings,
    pub resolution_ranks: ResolutionRanks,
    pub options: RankOptions,
    pub languages: LanguageSettings,
    pub custom_ranks: CustomRanksConfig,
}

impl Default for RankSettings {
    fn default() -> Self {
        Self {
            require: Vec::new(),
            exclude: Vec::new(),
            preferred: Vec::new(),
            resolutions: ResolutionSettings::default(),
            resolution_ranks: ResolutionRanks::default(),
            options: RankOptions::default(),
            languages: LanguageSettings::default(),
            custom_ranks: CustomRanksConfig::default(),
        }
    }
}
