/// Flat default scores for every `CustomRank` field.
///
/// A single source of truth for all
/// default scores.  Scoring functions call `model.quality_score(q)` etc. and
/// fall back to the value here when no custom override is set.
#[derive(Debug, Clone)]
pub struct RankingModel {
    // Quality (incl. codec — both live in CustomRanksConfig.quality)
    pub av1: i64,
    pub avc: i64,
    pub bluray: i64,
    pub dvd: i64,
    pub hdtv: i64,
    pub hevc: i64,
    pub mpeg: i64,
    pub remux: i64,
    pub vhs: i64,
    pub web: i64,
    pub webdl: i64,
    pub webmux: i64,
    pub xvid: i64,
    // Rips
    pub bdrip: i64,
    pub brrip: i64,
    pub dvdrip: i64,
    pub hdrip: i64,
    pub ppvrip: i64,
    pub satrip: i64,
    pub tvrip: i64,
    pub uhdrip: i64,
    pub vhsrip: i64,
    pub webdlrip: i64,
    pub webrip: i64,
    // HDR
    pub bit10: i64,
    pub dolby_vision: i64,
    pub hdr: i64,
    pub hdr10plus: i64,
    pub sdr: i64,
    // Audio
    pub aac: i64,
    pub atmos: i64,
    pub dolby_digital: i64,
    pub dolby_digital_plus: i64,
    pub dts_lossy: i64,
    pub dts_lossless: i64,
    pub flac: i64,
    pub mono: i64,
    pub mp3: i64,
    pub stereo: i64,
    pub surround: i64,
    pub truehd: i64,
    // Extras
    pub three_d: i64,
    pub converted: i64,
    pub commentary: i64,
    pub documentary: i64,
    pub dubbed: i64,
    pub edition: i64,
    pub hardcoded: i64,
    pub network: i64,
    pub proper: i64,
    pub repack: i64,
    pub retail: i64,
    pub site: i64,
    pub subbed: i64,
    pub upscaled: i64,
    pub scene: i64,
    pub uncensored: i64,
    // Trash
    pub cam: i64,
    pub clean_audio: i64,
    pub pdtv: i64,
    pub r5: i64,
    pub screener: i64,
    pub size: i64,
    pub telecine: i64,
    pub telesync: i64,
}

impl Default for RankingModel {
    fn default() -> Self {
        Self {
            // Quality
            av1: 500,
            avc: 500,
            bluray: 100,
            dvd: -5000,
            hdtv: -5000,
            hevc: 500,
            mpeg: -1000,
            remux: 10000,
            vhs: -10000,
            web: 100,
            webdl: 200,
            webmux: -10000,
            xvid: -10000,
            // Rips
            bdrip: -5000,
            brrip: -10000,
            dvdrip: -5000,
            hdrip: -10000,
            ppvrip: -10000,
            satrip: -10000,
            tvrip: -10000,
            uhdrip: -5000,
            vhsrip: -10000,
            webdlrip: -10000,
            webrip: -1000,
            // HDR
            bit10: 100,
            dolby_vision: 3000,
            hdr: 2000,
            hdr10plus: 2100,
            sdr: 0,
            // Audio
            aac: 100,
            atmos: 1000,
            dolby_digital: 50,
            dolby_digital_plus: 150,
            dts_lossy: 100,
            dts_lossless: 2000,
            flac: 0,
            mono: 0,
            mp3: -1000,
            stereo: 0,
            surround: 0,
            truehd: 2000,
            // Extras
            three_d: -10000,
            converted: -1000,
            commentary: 0,
            documentary: -250,
            dubbed: -1000,
            edition: 100,
            hardcoded: 0,
            network: 0,
            proper: 20,
            repack: 20,
            retail: 0,
            site: -10000,
            subbed: 0,
            upscaled: -10000,
            scene: 0,
            uncensored: 0,
            // Trash
            cam: -10000,
            clean_audio: -10000,
            pdtv: -10000,
            r5: -10000,
            screener: -10000,
            size: -10000,
            telecine: -10000,
            telesync: -10000,
        }
    }
}

impl RankingModel {
    /// Default score for a quality/rip/trash quality string.
    #[inline]
    pub fn quality_score(&self, q: &str) -> i64 {
        match q {
            "WEB" => self.web,
            "WEB-DL" => self.webdl,
            "BluRay" => self.bluray,
            "HDTV" => self.hdtv,
            "VHS" => self.vhs,
            "WEBMux" => self.webmux,
            "BluRay REMUX" | "REMUX" => self.remux,
            "DVD" => self.dvd,
            "WEBRip" => self.webrip,
            "WEB-DLRip" => self.webdlrip,
            "UHDRip" => self.uhdrip,
            "HDRip" => self.hdrip,
            "DVDRip" => self.dvdrip,
            "BDRip" => self.bdrip,
            "BRRip" => self.brrip,
            "VHSRip" => self.vhsrip,
            "PPVRip" => self.ppvrip,
            "SATRip" => self.satrip,
            "TVRip" => self.tvrip,
            "TeleCine" => self.telecine,
            "TeleSync" => self.telesync,
            "SCR" => self.screener,
            "R5" => self.r5,
            "CAM" => self.cam,
            "PDTV" => self.pdtv,
            _ => 0,
        }
    }

    /// Default score for a codec string.
    #[inline]
    pub fn codec_score(&self, codec: &str) -> i64 {
        match codec {
            "avc" => self.avc,
            "hevc" => self.hevc,
            "xvid" => self.xvid,
            "av1" => self.av1,
            "mpeg" => self.mpeg,
            _ => 0,
        }
    }

    /// Default score for an audio string.
    #[inline]
    pub fn audio_score(&self, audio: &str) -> i64 {
        match audio {
            "AAC" => self.aac,
            "Atmos" => self.atmos,
            "Dolby Digital" => self.dolby_digital,
            "Dolby Digital Plus" => self.dolby_digital_plus,
            "DTS Lossy" => self.dts_lossy,
            "DTS Lossless" => self.dts_lossless,
            "FLAC" => self.flac,
            "MP3" => self.mp3,
            "TrueHD" => self.truehd,
            "HQ Clean Audio" => self.clean_audio,
            _ => 0,
        }
    }

    /// Default score for an HDR string.
    #[inline]
    pub fn hdr_score(&self, hdr: &str) -> i64 {
        match hdr {
            "DV" => self.dolby_vision,
            "HDR" => self.hdr,
            "HDR10+" => self.hdr10plus,
            "SDR" => self.sdr,
            _ => 0,
        }
    }

    /// Nested JSON matching `custom_ranks` structure, used to inject `"default": N`
    /// into each CustomRank entry in the GraphQL `rankSettings` response.
    pub fn to_category_map(&self) -> serde_json::Value {
        serde_json::json!({
            "quality": {
                "av1": self.av1, "avc": self.avc, "bluray": self.bluray,
                "dvd": self.dvd, "hdtv": self.hdtv, "hevc": self.hevc,
                "mpeg": self.mpeg, "remux": self.remux, "vhs": self.vhs,
                "web": self.web, "webdl": self.webdl, "webmux": self.webmux,
                "xvid": self.xvid,
            },
            "rips": {
                "bdrip": self.bdrip, "brrip": self.brrip, "dvdrip": self.dvdrip,
                "hdrip": self.hdrip, "ppvrip": self.ppvrip, "satrip": self.satrip,
                "tvrip": self.tvrip, "uhdrip": self.uhdrip, "vhsrip": self.vhsrip,
                "webdlrip": self.webdlrip, "webrip": self.webrip,
            },
            "hdr": {
                "bit10": self.bit10, "dolby_vision": self.dolby_vision,
                "hdr": self.hdr, "hdr10plus": self.hdr10plus, "sdr": self.sdr,
            },
            "audio": {
                "aac": self.aac, "atmos": self.atmos,
                "dolby_digital": self.dolby_digital,
                "dolby_digital_plus": self.dolby_digital_plus,
                "dts_lossy": self.dts_lossy, "dts_lossless": self.dts_lossless,
                "flac": self.flac, "mono": self.mono, "mp3": self.mp3,
                "stereo": self.stereo, "surround": self.surround, "truehd": self.truehd,
            },
            "extras": {
                "three_d": self.three_d, "converted": self.converted,
                "commentary": self.commentary, "documentary": self.documentary,
                "dubbed": self.dubbed, "edition": self.edition,
                "hardcoded": self.hardcoded, "network": self.network,
                "proper": self.proper, "repack": self.repack, "retail": self.retail,
                "site": self.site, "subbed": self.subbed, "upscaled": self.upscaled,
                "scene": self.scene, "uncensored": self.uncensored,
            },
            "trash": {
                "cam": self.cam, "clean_audio": self.clean_audio, "pdtv": self.pdtv,
                "r5": self.r5, "screener": self.screener, "size": self.size,
                "telecine": self.telecine, "telesync": self.telesync,
            },
        })
    }
}
