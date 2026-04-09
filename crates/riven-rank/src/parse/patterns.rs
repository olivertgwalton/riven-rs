use regex::Regex;
use std::sync::LazyLock;

// =============================================================================
// Year
// =============================================================================

pub(crate) static RE_YEAR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|[\.\s\(\[,])((?:19|20)\d{2})(?:[\.\s\)\],]|$)").unwrap());

/// Year range like "2000-2020" — used for complete detection and first-year extraction.
pub(crate) static RE_YEAR_RANGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b((?:19|20)\d{2})\s?-\s?((?:19|20)\d{2})\b").unwrap());

/// Partial year range like "(2000-05)" in brackets.
pub(crate) static RE_YEAR_RANGE_SHORT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[(\[]\s?((?:19\d|20[012])\d)\s?-\s?(\d{2})\s?[)\]]").unwrap());

// =============================================================================
// Resolution
// =============================================================================

pub(crate) static RE_RES_3840: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)3840\s*[x×]\s*\d+").unwrap());
pub(crate) static RE_RES_1920: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)1920\s*[x×]\s*\d+").unwrap());
pub(crate) static RE_RES_1280: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)1280\s*[x×]\s*\d+").unwrap());
pub(crate) static RE_RES_QHD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:W?QHD|QuadHD)\b").unwrap());
pub(crate) static RE_RES_FHD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:Full[\s.]*HD|FHD)\b").unwrap());
pub(crate) static RE_RES_PREFIXED_2160: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BD|UHD|HD|M)\s*(?:2160p|4k)\b").unwrap());
pub(crate) static RE_RES_PREFIXED_1080: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BD|HD|M)\s*1080p?\b").unwrap());
pub(crate) static RE_RES_PREFIXED_720: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BD|HD|M)\s*720p?\b").unwrap());
pub(crate) static RE_RES_PREFIXED_480: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BD|HD|M)\s*480p?\b").unwrap());
pub(crate) static RE_RES_GENERIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:4k|2160p|1080[pi]|720p|480p)\b").unwrap());
pub(crate) static RE_RES_DIGITS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(240|360|480|576|720|1080|2160|3840)([pi])\b").unwrap());
/// Typo correction: "4800p" → 480p, "10800p" → 1080p, "21600p" → 2160p
pub(crate) static RE_RES_TYPO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(480|720|1080|2160)0[pi]").unwrap());

// =============================================================================
// Season
// =============================================================================

pub(crate) static RE_SEASON_SE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:^|[.\s\[(])S(\d{1,4})(?:(?:\s*[&+]\s*S?|\s*-\s*S)(\d{1,4}))*").unwrap()
});
pub(crate) static RE_SEASON_FULL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:season|saison|temporada|сезон|staffel|seizoen|series|temp|sezon)[\s._]*(\d{1,3})\b(?:[\s._]*[-&+][\s._]*(?:season|saison|temporada|сезон|staffel|seizoen|series|temp|sezon)?[\s._]*(\d{1,3})\b)*").unwrap()
});
pub(crate) static RE_SEASON_RANGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[.\s\[(])S(\d{1,3})\s*[-–—]+\s*S(\d{1,3})\b").unwrap());
pub(crate) static RE_SEASON_MULTI: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:season|saison|temporada|сезон|staffel|seizoen)s?[\s._]*(\d{1,3})\b[\s._]*(?:[,&+][\s._]*(\d{1,3})\b)+")
        .unwrap()
});
/// Ordinal season: "1st season", "2nd season", "3rd season"
pub(crate) static RE_SEASON_ORDINAL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d{1,2})[\s._]*(?:st|nd|rd|th)[\s._]*season\b").unwrap());
/// Russian season format: "1-й Сезон"
pub(crate) static RE_SEASON_RUSSIAN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(\d{1,2})(?:-?й)?\s*[Сс]езон").unwrap());
/// Russian season format: "Сезон 1"
pub(crate) static RE_SEASON_RUSSIAN2: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)[Сс]езон:?\s*№?(\d{1,2})").unwrap());
/// Portuguese season: "1ª temporada"
pub(crate) static RE_SEASON_PT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(\d{1,2})[°ºªa]?\s*temporada").unwrap());
/// Turkish season: "2.Sezon", "2 Sezon"
pub(crate) static RE_SEASON_TR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d{1,2})[.\s]*sezon\b").unwrap());
/// Compact season/episode pair: "(S4-24)"
pub(crate) static RE_SEASON_EP_COMPACT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bS(\d{1,3})-(\d{1,4})\b").unwrap());
/// ТВ-N format
pub(crate) static RE_SEASON_TV: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bТВ-(\d{1,2})\b").unwrap());

// =============================================================================
// Episode
// =============================================================================

pub(crate) static RE_EPISODE_SE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:S\d{1,4})\s*E(\d{1,4})(?:\s*[-+&]\s*E?(\d{1,4}))*").unwrap()
});
pub(crate) static RE_EPISODE_STANDALONE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bE(\d{1,4})(?:\s*[-+&]\s*E?(\d{1,4}))*(?:\b|[.\s])").unwrap()
});
pub(crate) static RE_EPISODE_FULL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:episodes?|eps?|episodio|episodios|épisode|folge|Эпизод|Серия|сер\.?|cap(?:itulo)?|epis[oó]dio)[\s._]*[-:#№]?[\s._]*(\d{1,4})(?:[\s._]*[-+&][\s._]*(?:episodes?|eps?|episodio|episodios|épisode|folge)?[\s._]*(\d{1,4}))*",
    )
    .unwrap()
});
/// Turkish episode: "7.Bölüm", "7 Bölum"
pub(crate) static RE_EPISODE_TR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d{1,4})[.\s]*b[oö]l(?:u|ü)m\b").unwrap());
/// Crossref range: "06x01-08"
pub(crate) static RE_EPISODE_CROSSREF_RANGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d{1,2})[xх](\d{2,3})\s*-\s*(\d{2,3})\b").unwrap());
pub(crate) static RE_EPISODE_CROSSREF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d{1,2})[xх](\d{2,3})\b").unwrap());
pub(crate) static RE_EPISODE_RANGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bE(\d{1,4})\s*-\s*E(\d{1,4})\b").unwrap());
/// Anime-style bare range: "01 ~ 12", "00~25"
pub(crate) static RE_EPISODE_RANGE_BARE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:^|[\s\[(\-])(\d{1,3})\s*~\s*(\d{1,3})(?:$|[\s\])])").unwrap()
});
/// Parenthesized bare range: "(01 - 12)"
pub(crate) static RE_EPISODE_RANGE_PAREN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\((\d{1,3})\s*-\s*(\d{1,3})\)").unwrap());

/// Consecutive multi-episode without separator: S01E01E02E03
pub(crate) static RE_EPISODE_CONSECUTIVE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)S\d{1,4}((?:E\d{1,4}){2,})").unwrap());
/// Bare E## extractor — only used on consecutive blocks
pub(crate) static RE_EP_NUM_BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)E(\d{1,4})").unwrap());

/// "X of Y" pattern: "(16 of 26)" → episode 16
pub(crate) static RE_EPISODE_OF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:\[|\()(\d+)\s+(?:of|из|iz)\s+\d+(?:\]|\))").unwrap());

/// Russian episode: "Серии: 1 of 10"
pub(crate) static RE_EPISODE_RUSSIAN_OF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)Серии:\s+(\d+)\s+(?:of|из|iz)\s+\d+").unwrap());

/// Russian episode: "1-я серия"
pub(crate) static RE_EPISODE_RUSSIAN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(\d{1,3})(?:-?я)?\s*(?:ser(?:i?[iyja]|\b)|[Сс]ер(?:ии|ия|\.)?)").unwrap()
});

/// Part number: "Part 1", "Pt.2", "Part.1"
pub(crate) static RE_PART: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:Part|Pt)\.?\s*(\d{1,2})\b").unwrap());

// =============================================================================
// Episode code (CRC32 / checksum in brackets)
// =============================================================================

/// 8-char hex code in brackets: [5E46AC39]
pub(crate) static RE_EPISODE_CODE_HEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[\[\(]([A-Fa-f0-9]{8})[\]\)]").unwrap());
/// 8-digit numeric code in brackets: [12345678]
pub(crate) static RE_EPISODE_CODE_NUM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[\[\(]([0-9]{8})[\]\)]").unwrap());

// =============================================================================
// Quality (ordered per PTT handler order)
// =============================================================================

pub(crate) static RE_Q_TELESYNC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:HD[ .\-]*)?T(?:ELE)?S(?:YNC)?(?:Rip)?\b").unwrap());
pub(crate) static RE_Q_TELECINE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:HD[ .\-]*)?T(?:ELE)?C(?:INE)?(?:Rip)?\b").unwrap());
pub(crate) static RE_Q_SCR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:DVD?|BD|BR|HD)?[ .\-]*Scr(?:eener)?\b").unwrap());
pub(crate) static RE_Q_PRE_DVD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bP(?:RE)?[ .\-]*(?:HD|DVD)(?:Rip)?\b").unwrap());
pub(crate) static RE_Q_BLURAY_REMUX1: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bBlu[ .\-]*Ray\b").unwrap());
pub(crate) static RE_Q_BLURAY_REMUX2: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:BD|BR|UHD)[- ]?remux").unwrap());
pub(crate) static RE_Q_BLURAY_REMUX3: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:remux.*)\bBlu[ .\-]*Ray\b").unwrap());
pub(crate) static RE_Q_REMUX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bremux\b").unwrap());
pub(crate) static RE_Q_BLURAY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bBlu[ .\-]*Ray\b").unwrap());
pub(crate) static RE_Q_UHDRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bUHD[ .\-]*Rip\b").unwrap());
pub(crate) static RE_Q_HDRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:\bHD[ .\-]*Rip\b|\bMicro\s*HD\b)").unwrap());
pub(crate) static RE_Q_BRRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BR|Blu[ .\-]*Ray)[ .\-]*Rip\b").unwrap());
pub(crate) static RE_Q_BDRIP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:\bBD[ .\-]*Rip\b|\bBDR\b|\bBD-RM\b|[(\[]BD[\]). ,\-])").unwrap()
});
pub(crate) static RE_Q_DVDRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:HD[ .\-]*)?DVD[ .\-]*Rip\b").unwrap());
pub(crate) static RE_Q_VHSRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bVHS[ .\-]*Rip?\b").unwrap());
pub(crate) static RE_Q_DVD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bDVD(?:R\d?|.*Mux)?\b").unwrap());
pub(crate) static RE_Q_VHS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bVHS\b").unwrap());
pub(crate) static RE_Q_PPVRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bPPV[ .\-]*Rip\b").unwrap());
pub(crate) static RE_Q_HDTVRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bHDTV[ .\-]*Rip\b").unwrap());
pub(crate) static RE_Q_SATRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bSAT[ .\-]*Rip\b").unwrap());
pub(crate) static RE_Q_TVRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bTV[ .\-]*Rip\b").unwrap());
pub(crate) static RE_Q_R5: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bR[56]\b").unwrap());
pub(crate) static RE_Q_WEBMUX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:DL|WEB|BD|BR)MUX\b").unwrap());
pub(crate) static RE_Q_WEBRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bWEB[ ._\-]*Rip\b").unwrap());
pub(crate) static RE_Q_WEBDLRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bWEB[ ._\-]?DL[ ._\-]?Rip\b").unwrap());
pub(crate) static RE_Q_WEBDL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bWEB[ ._\-]*(?:DL|\.BDrip|\.DLRIP)\b").unwrap());
pub(crate) static RE_Q_WEB: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[\s.\-\[(])WEB(?:[\s.\-\])]|$)").unwrap());
pub(crate) static RE_Q_CAM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:H[DQ][ .\-]*)?CAM(?:H[DQ])?(?:[ .\-]*Rip|Rp)?\b").unwrap()
});
pub(crate) static RE_Q_CAM_FALSE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bCAM.?(?:S|E|\()\d+").unwrap());
pub(crate) static RE_Q_PDTV: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bPDTV\b").unwrap());
pub(crate) static RE_Q_HDTV: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[\W_])HD(?:.?TV)?(?:$|[\W_])").unwrap());

// =============================================================================
// Codec
// =============================================================================

pub(crate) static RE_CODEC_AVC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[\W_])(?:x\.?264|h\.?264|AVC)(?:$|[\W_])").unwrap());
pub(crate) static RE_CODEC_HEVC: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:^|[\W_])(?:x\.?265|h\.?265|HEVC(?:10)?)(?:$|[\W_])").unwrap()
});
pub(crate) static RE_CODEC_XVID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[\W_])(?:xvid|divx)(?:$|[\W_])").unwrap());
pub(crate) static RE_CODEC_AV1: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[\W_])AV1(?:$|[\W_])").unwrap());
pub(crate) static RE_CODEC_MPEG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[\W_])mpe?g\d*(?:$|[\W_])").unwrap());
/// Bare 264/265 not preceded by x or h
pub(crate) static RE_CODEC_264_BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\W264\W").unwrap());
pub(crate) static RE_CODEC_265_BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\W265\W").unwrap());

// =============================================================================
// Audio
// =============================================================================

pub(crate) static RE_AUDIO_DTS_LOSSLESS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:DTS[ .\-]?HD[ .\-]?(?:MA|Master\s*Audio)|DTS[ .\-]?X|DTS[ .\-]?Lossless)\b",
    )
    .unwrap()
});
pub(crate) static RE_AUDIO_DTS_LOSSY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bDTS(?:[ .\-]?HD(?:[ .\-]?HR)?)?\b").unwrap());
pub(crate) static RE_AUDIO_ATMOS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:Dolby[ .\-]*)?Atmos\b").unwrap());
pub(crate) static RE_AUDIO_TRUEHD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:True[ .\-]?HD|\.True\.)\b").unwrap());
pub(crate) static RE_AUDIO_TRUEHD_BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bTRUE\b").unwrap());
pub(crate) static RE_AUDIO_FLAC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bFLAC(?:\d\.\d)?(?:x\d+)?\b").unwrap());
pub(crate) static RE_AUDIO_DD_PLUS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(?:\b(?:DD2?P(?:\d(?:\.\d)?)?|DDP(?:\d(?:\.\d)?)?|Dolby\s*Digital\s*Plus|DD\s*Plus|E[ .\-]?AC[ .\-]?3|EAC3)\b|\bDD\+(?:\d(?:\.\d)?)?(?:$|[\s)\].,_-]))(?:-S\d+)?",
    )
    .unwrap()
});
pub(crate) static RE_AUDIO_DD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:DD(?:\d(?:\.\d)?)?|Dolby\s*Digital|DolbyD|AC[ .\-]?3(?:x2)?(?:-S\d+)?)\b")
        .unwrap()
});
pub(crate) static RE_AUDIO_AAC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bQ?Q?AAC(?:x?\d+\.?\d*)?").unwrap());
pub(crate) static RE_AUDIO_PCM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b[LP]?PCM\b").unwrap());
pub(crate) static RE_AUDIO_OPUS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bOPUS\b").unwrap());
pub(crate) static RE_AUDIO_MP3: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bMP3\b").unwrap());
pub(crate) static RE_AUDIO_HQ_CLEAN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bHQ[\s.]*Clean[\s.]*Audio\b").unwrap());

// =============================================================================
// Channels
// =============================================================================

pub(crate) static RE_CHAN_71: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b7[. \-]1\b").unwrap());
pub(crate) static RE_CHAN_51: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:5[\.\s]1(?:ch|-S\d+|x[2-4])?|5\W1(?:x[2-4])?)").unwrap());
pub(crate) static RE_CHAN_20: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\+?2[\.\s]0(?:x[2-4])?\b").unwrap());
pub(crate) static RE_CHAN_STEREO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bstereo\b").unwrap());
pub(crate) static RE_CHAN_MONO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bmono\b").unwrap());

// =============================================================================
// HDR
// =============================================================================

pub(crate) static RE_HDR_DV: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bDV\b|dolby[ .\-]*vision|\bDoVi\b").unwrap());
pub(crate) static RE_HDR_HDR10PLUS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)HDR10(?:\+|[-\.\s]?plus)").unwrap());
pub(crate) static RE_HDR_HDR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bHDR(?:10)?\b").unwrap());
pub(crate) static RE_HDR_SDR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bSDR\b").unwrap());

// =============================================================================
// Bit depth
// =============================================================================

/// 10-bit detection
pub(crate) static RE_BIT_DEPTH_10: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:\b10[- ]?bit\b|\bHi10[Pp]?\b|\bHEVC\s*10\b|\bHDR10\b|\b(?:HEVC|[XH]\.?265)10(?:[- ]?bit)?\b)").unwrap()
});
/// 8-bit detection
pub(crate) static RE_BIT_DEPTH_8: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b8[- ]?bit\b").unwrap());
/// 12-bit detection
pub(crate) static RE_BIT_DEPTH_12: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b12[- ]?bit\b").unwrap());

// =============================================================================
// Group
// =============================================================================

pub(crate) static RE_GROUP_DASH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"- ?([^\-. \[]+[^\-. \[)\]\d][^\-. \[)\]]*)(?:\[[\w.-]+])?(?:\.\w{2,4}$|$)")
        .unwrap()
});
pub(crate) static RE_GROUP_BRACKET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\[([^\]]+)\]").unwrap());
pub(crate) static RE_GROUP_PAREN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\(([\w-]+)\)(?:$|\.\w{2,4}$)").unwrap());

// =============================================================================
// Misc
// =============================================================================

pub(crate) static RE_SIZE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d+(?:\.\d+)?\s*(?:GB|MB|TB|KB))\b").unwrap());
pub(crate) static RE_EXTENSION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\.(3g2|3gp|avi|flv|mkv|mk3d|mov|mp2|mp4|m4v|mpe|mpeg|mpg|mpv|webm|wmv|ogm|divx|ts|m2ts|iso|vob|sub|idx|ttxt|txt|smi|srt|ssa|ass|vtt|nfo|html|torrent)$").unwrap()
});
pub(crate) static RE_CONTAINER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\.?[\[(]?\b(MKV|AVI|MP4|WMV|MPG|MPEG|FLV|MOV|WEBM|TS|M4V|OGM|DIVX)\b[\])]?")
        .unwrap()
});
pub(crate) static RE_3D: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:3D|SBS|half[- ]?(?:OU|SBS)|HSBS|BluRay3D|BD3D)\b").unwrap()
});
pub(crate) static RE_COMPLETE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:complete|full\s*series|complete\s*series|integrale?|intégrale?|completa)\b",
    )
    .unwrap()
});
/// Additional complete patterns: box set, collection, trilogy, etc.
pub(crate) static RE_COMPLETE_COLLECTION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:(?:movie|complete)\s*\.?\s*collection|(?:complete|dvd)?\s*box\s*set|mini\s*series|duology|trilogy|quadr[oi]logy|tetralogy|pentalogy|hexalogy|heptalogy|anthology|kolekcja|saga\b)").unwrap()
});

// =============================================================================
// Date — multiple formats matching PTT
// =============================================================================

/// YYYY-MM-DD / YYYY.MM.DD / YYYY/MM/DD
pub(crate) static RE_DATE_YMD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|\W)((?:19[6-9]|20[012])\d)[.\-/\\](0[1-9]|1[012])[.\-/\\](0[1-9]|[12]\d|3[01])(?:\W|$)").unwrap()
});
/// DD-MM-YYYY
pub(crate) static RE_DATE_DMY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|\W)(0[1-9]|[12]\d|3[01])[.\-/\\](0[1-9]|1[012])[.\-/\\]((?:19[6-9]|20[012])\d)(?:\W|$)").unwrap()
});
/// YYYYMMDD (compact)
pub(crate) static RE_DATE_COMPACT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|\W)(20[012]\d)(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])(?:\W|$)").unwrap()
});

pub(crate) static RE_BITRATE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d+(?:\.\d+)?\s*(?:[KkMm]bps|[KkMm]b/s))\b").unwrap());
pub(crate) static RE_REGION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:^|[\W_])(R[0-9]J?|Region\s*[0-9]|PAL|NTSC|SECAM)(?:$|[\W_])").unwrap()
});
pub(crate) static RE_REGION_DISC: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:^|[\W_])(?:DVD)?(R[0-9]J?|Region\s*[0-9])(?:$|[\W_])").unwrap()
});
pub(crate) static RE_SITE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
            r"^((?:(?:www?[.,]?\s*[\w-]+(?:[ .][\w-]+)+)|(?:(?:www?[.,])?[\w-]+\.[\w-]+(?:\.[\w-]+)?)))\s+-\s*",
        )
        .unwrap()
});
/// Additional site patterns
pub(crate) static RE_SITE_DOMAIN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bwww?.?(?:\w+-)*\w+[.\s](?:com|org|net|ms|tv|mx|co|party|vip|nu|pics)\b")
        .unwrap()
});
pub(crate) static RE_SITE_KNOWN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)rarbg|torrentleech|(?:the)?piratebay").unwrap());
pub(crate) static RE_SITE_BRACKET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[([^\]]+\.[^\]]+)\](?:\.\w{2,4}$|\s|$)").unwrap());
pub(crate) static RE_VOLUME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:vol(?:ume)?\.?\s*)(\d{1,3})(?:\s*[-&]\s*(?:vol(?:ume)?\.?\s*)?(\d{1,3}))?",
    )
    .unwrap()
});
pub(crate) static RE_EDITION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(\d{2,3}(?:th)?[\.\s\-+_/(),]*Anniversary[\.\s\-+_/(),]*(?:Edition|Ed)?|Ultimate[\.\s\-+_/(),]*Edition|Extended[\.\s\-+_/(),]*Director'?s|(?:custom\s*\.?\s*)?Extended|Director'?s[\s.\-]*Cut|Collector'?s(?:\s*Edition)?|Theatrical|Uncut|IMAX(?:\s*Edition)?|\.?Diamond\.\s*|Remaster(?:ed)?|Criterion[\.\s\-+_/(),]*(?:Collection|Edition)|Final\s*Cut|Limited\s*Edition|Deluxe\s*Edition|Special\s*Edition)\b").unwrap()
});
pub(crate) static RE_COUNTRY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(US|UK|AU|NZ|CA|IE|FR|DE|ES|IT|NL|BE|AT|CH|SE|NO|DK|FI|JP|KR|CN|TW|HK|IN|BR|MX|AR|CL|CO|RU|PL|CZ|HU|RO|BG|HR|RS|SK|SI|UA|GR|TR|TH|PH|MY|SG|ID|VN)\b").unwrap()
});
pub(crate) static RE_NON_ENGLISH_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[\p{Han}\p{Katakana}\p{Hiragana}\p{Cyrillic}\p{Arabic}\p{Thai}\p{Hangul}\p{Devanagari}\p{Bengali}\p{Tamil}\p{Telugu}\p{Malayalam}\p{Kannada}\p{Gujarati}()\s.。，、·【】★「」『』]+").unwrap()
});
pub(crate) static RE_EXTRAS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(trailer|sample|featurettes?|behind\s*the\s*scenes|deleted\s*scenes?|bonus|extras?|interview|commentary|making\s*of|bloopers?|gag\s*reel)\b").unwrap()
});
/// Anime-specific extras
pub(crate) static RE_EXTRAS_NCED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bNCED\b").unwrap());
pub(crate) static RE_EXTRAS_NCOP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bNCOP\b").unwrap());
pub(crate) static RE_EXTRAS_NC: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\bNC\b").unwrap());
pub(crate) static RE_EXTRAS_OVA: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bOVA\b").unwrap());
pub(crate) static RE_EXTRAS_ED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bED\d?v?\d?\b").unwrap());
pub(crate) static RE_EXTRAS_OP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bOPv?\d*\b").unwrap());

pub(crate) static RE_TITLE_QUALITY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:\b(?:HD[ .\-]*)?T(?:ELE)?S(?:YNC)?(?:Rip)?|\b(?:HD[ .\-]*)?T(?:ELE)?C(?:INE)?(?:Rip)?|\bBlu[ .\-]*Ray|\bremux|\bWEB[ .\-]*(?:DL|Rip)|\bWEB\b|\bHDTV|\bDVD(?:Rip)?|\bBDRip|\bBRRip|\bHDRip|\bCAM|\bPDTV|\bSAT[ .\-]*Rip|\bVHS)\b").unwrap()
});

// =============================================================================
// Trash detection
// =============================================================================

pub(crate) static RE_DVB: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bDVB\b").unwrap());
pub(crate) static RE_LEAKED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bLEAKED\b").unwrap());
pub(crate) static RE_SPRINT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:H[DQ][ .\-]*)?S[ .\-]*print\b").unwrap());
pub(crate) static RE_R6: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bR6\b").unwrap());
pub(crate) static RE_THREESIXTYP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)threesixtyp").unwrap());

// =============================================================================
// Boolean flags
// =============================================================================

pub(crate) static RE_PROPER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:REAL\.)?PROPER\b").unwrap());
pub(crate) static RE_REPACK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:REPACK|RERIP)\b").unwrap());
pub(crate) static RE_RETAIL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bRetail\b").unwrap());
pub(crate) static RE_UPSCALED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:AI[ .\-]?)?(?:Upscal(?:e[Dd]?|ing)|Enhanced?)\b").unwrap()
});
pub(crate) static RE_UPSCALED_SPECIFIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:iris2|regrade|ups(?:uhd|fhd|hd|4k))\b").unwrap());
pub(crate) static RE_UPSCALED_AI: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b\.AI\.\b").unwrap());
pub(crate) static RE_REMASTERED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bRemaster(?:ed)?\b").unwrap());
pub(crate) static RE_EXTENDED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bEXTENDED\b").unwrap());
pub(crate) static RE_CONVERTED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bCONVERT(?:ED)?\b").unwrap());
pub(crate) static RE_UNRATED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bUNRATED\b").unwrap());
pub(crate) static RE_UNCENSORED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bUNCENSORED\b").unwrap());
pub(crate) static RE_DUBBED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:DUBBED|DUAL(?:[ .\-]*(?:AU?|[AÁ]UDIO|LINE))?|DUBS?|DUBBING|DUBLADO|FAN\s*DUB|MULTI)\b").unwrap()
});
pub(crate) static RE_SUBBED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:SUBBED|SUBS?|SUBTITL(?:E[DS]?|ING)|multi(?:ple)?[ .\-]*(?:su$|sub\w*)|msub)\b",
    )
    .unwrap()
});
pub(crate) static RE_HARDCODED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:HC|HARDCODED)\b").unwrap());
pub(crate) static RE_DOCUMENTARY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bDOCU(?:menta?ry)?\b").unwrap());
pub(crate) static RE_COMMENTARY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bCOMMENTARY\b").unwrap());
pub(crate) static RE_ADULT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:XXX|xxx|Xxx)\b").unwrap());
pub(crate) static RE_PPV: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bPPV\b").unwrap());
pub(crate) static RE_PPV_FIGHT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bFight[.\s_-]*Nights?\b").unwrap());
pub(crate) static RE_SCENE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:-CAKES|-GGEZ|-GGWP|-GLHF|-GOSSIP|-NAISU|-KOGI|-PECULATE|-SLOT|-EDITH|-ETHEL|-ELEANOR|-B2B|-SPAMnEGGS|-FTP|-DiRT|-SYNCOPY|-BAE|-SuccessfulCrab|-NHTFS|-SURCODE|-B0MBARDIERS)\b").unwrap()
});
/// Scene detection: resolution + WEB marker (not WEB-DL) — caller must exclude WEB-DL
pub(crate) static RE_SCENE_WEB: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b\d{3,4}p\b.*[_. ]WEB[_. ]").unwrap());

// Network patterns are defined as a table in detect.rs
