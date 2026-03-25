use regex::Regex;
use std::sync::LazyLock;

// Year
pub(crate) static RE_YEAR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|[\.\s\(\[,])((?:19|20)\d{2})(?:[\.\s\)\],]|$)").unwrap());

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

pub(crate) static RE_RES_3840: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)3840\s*[x×]\s*\d+").unwrap());
pub(crate) static RE_RES_1920: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)1920\s*[x×]\s*\d+").unwrap());
pub(crate) static RE_RES_1280: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)1280\s*[x×]\s*\d+").unwrap());
pub(crate) static RE_RES_QHD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:W?QHD)\b").unwrap());
pub(crate) static RE_RES_FHD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:Full\s*HD|FHD)\b").unwrap());
pub(crate) static RE_RES_PREFIXED_2160: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BD|UHD|HD|M)\s*(?:2160p|4k)\b").unwrap());
pub(crate) static RE_RES_PREFIXED_1080: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BD|HD|M)\s*1080p?\b").unwrap());
pub(crate) static RE_RES_PREFIXED_720: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BD|HD|M)\s*720p?\b").unwrap());
pub(crate) static RE_RES_PREFIXED_480: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:BD|HD|M)\s*480p?\b").unwrap());
pub(crate) static RE_RES_GENERIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:4k|2160p|1080p|720p|480p)\b").unwrap());
pub(crate) static RE_RES_DIGITS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(240|360|480|576|720|1080|2160|3840)[pi]\b").unwrap());

// ---------------------------------------------------------------------------
// Season
// ---------------------------------------------------------------------------

pub(crate) static RE_SEASON_SE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bS(\d{1,4})(?:\s*[&+\-]\s*S?(\d{1,4}))*").unwrap());
pub(crate) static RE_SEASON_FULL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:season|saison|temporada|сезон|staffel)\s*(\d{1,3})(?:\s*[-&+]\s*(?:season|saison|temporada|сезон|staffel)?\s*(\d{1,3}))*").unwrap()
});
pub(crate) static RE_SEASON_RANGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bS(\d{1,3})\s*-\s*S(\d{1,3})\b").unwrap());
pub(crate) static RE_SEASON_MULTI: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:season|saison|temporada|сезон|staffel)s?\s*(\d{1,3})\s*(?:[,&+]\s*(\d{1,3}))+")
        .unwrap()
});

// ---------------------------------------------------------------------------
// Episode
// ---------------------------------------------------------------------------

pub(crate) static RE_EPISODE_SE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:S\d{1,4})\s*E(\d{1,4})(?:\s*[-+&]\s*E?(\d{1,4}))*").unwrap()
});
pub(crate) static RE_EPISODE_STANDALONE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bE(\d{1,4})(?:\s*[-+&]\s*E?(\d{1,4}))*(?:\b|[.\s])").unwrap()
});
pub(crate) static RE_EPISODE_FULL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:episode|ep|episodio|épisode|folge|episodio)\s*(\d{1,4})(?:\s*[-+&]\s*(?:episode|ep|episodio|épisode|folge)?\s*(\d{1,4}))*",
    )
    .unwrap()
});
pub(crate) static RE_EPISODE_CROSSREF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d{1,2})x(\d{2,3})\b").unwrap());
pub(crate) static RE_EPISODE_RANGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bE(\d{1,4})\s*-\s*E(\d{1,4})\b").unwrap());

// Consecutive multi-episode without separator: S01E01E02E03
pub(crate) static RE_EPISODE_CONSECUTIVE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)S\d{1,4}((?:E\d{1,4}){2,})").unwrap());
// Bare E## extractor — only used on consecutive blocks
pub(crate) static RE_EP_NUM_BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)E(\d{1,4})").unwrap());

// Part number: "Part 1", "Pt.2", "Part.1"
pub(crate) static RE_PART: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:Part|Pt)\.?\s*(\d{1,2})\b").unwrap());

// ---------------------------------------------------------------------------
// Quality (ordered per PTT handler order)
// ---------------------------------------------------------------------------

pub(crate) static RE_Q_TELESYNC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:HD[ .\-]*)?T(?:ELE)?S(?:YNC)?(?:Rip)?\b").unwrap());
pub(crate) static RE_Q_TELECINE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:HD[ .\-]*)?T(?:ELE)?C(?:INE)?(?:Rip)?\b").unwrap());
pub(crate) static RE_Q_SCR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:DVD?|BD|BR|HD)?[ .\-]*Scr(?:eener)?\b").unwrap());
pub(crate) static RE_Q_PRE_DVD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bPre[ .\-]*DVD\b").unwrap());
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
pub(crate) static RE_Q_VHS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bVHS\b").unwrap());
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
    LazyLock::new(|| Regex::new(r"(?i)\bWEB[ .\-]*Rip\b").unwrap());
pub(crate) static RE_Q_WEBDLRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bWEB[ .\-]?DL[ .\-]?Rip\b").unwrap());
pub(crate) static RE_Q_WEBDL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bWEB[ .\-]*(?:DL|\.BDrip|\.DLRIP)\b").unwrap());
pub(crate) static RE_Q_WEB: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[\s.\-\[(])WEB(?:[\s.\-\])]|$)").unwrap());
pub(crate) static RE_Q_CAM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:H[DQ][ .\-]*)?CAM(?:H[DQ])?(?:[ .\-]*Rip|Rp)?\b")
        .unwrap()
});
pub(crate) static RE_Q_CAM_FALSE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bCAM.?(?:S|E|\()\d+")
        .unwrap()
});
pub(crate) static RE_Q_PDTV: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bPDTV\b").unwrap());
pub(crate) static RE_Q_HDTV: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bHD(?:.?TV)?\b").unwrap());

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

pub(crate) static RE_CODEC_AVC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:x\.?264|h\.?264|AVC)\b").unwrap());
pub(crate) static RE_CODEC_HEVC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:x\.?265|h\.?265|HEVC)\b").unwrap());
pub(crate) static RE_CODEC_XVID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:xvid|divx)\b").unwrap());
pub(crate) static RE_CODEC_AV1: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bAV1\b").unwrap());
pub(crate) static RE_CODEC_MPEG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bmpeg[24]?\b").unwrap());

// ---------------------------------------------------------------------------
// Audio
// ---------------------------------------------------------------------------

pub(crate) static RE_AUDIO_DTS_LOSSLESS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:DTS[ .\-]?HD[ .\-]?(?:MA|Master\s*Audio)|DTS[ .\-]?X|DTS[ .\-]?Lossless)\b")
        .unwrap()
});
pub(crate) static RE_AUDIO_DTS_LOSSY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bDTS(?:[ .\-]?HD(?:[ .\-]?HR)?)?\b").unwrap()
});
pub(crate) static RE_AUDIO_ATMOS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bAtmos\b").unwrap());
pub(crate) static RE_AUDIO_TRUEHD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bTrue[ .\-]?HD\b").unwrap());
pub(crate) static RE_AUDIO_FLAC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bFLAC\b").unwrap());
pub(crate) static RE_AUDIO_DD_PLUS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:DD[P+]|DDP?\d|Dolby\s*Digital\s*Plus|E[ .\-]?AC[ .\-]?3|EAC3)\b")
        .unwrap()
});
pub(crate) static RE_AUDIO_DD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:DD|AC[ .\-]?3|Dolby\s*Digital)\b").unwrap()
});
pub(crate) static RE_AUDIO_AAC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bAAC(?:\d(?:\.\d)?)?\b").unwrap());
pub(crate) static RE_AUDIO_PCM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b[LP]?PCM\b").unwrap());
pub(crate) static RE_AUDIO_OPUS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bOPUS\b").unwrap());
pub(crate) static RE_AUDIO_MP3: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bMP3\b").unwrap());
pub(crate) static RE_AUDIO_HQ_CLEAN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bHQ\s*Clean\s*Audio\b").unwrap());

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

pub(crate) static RE_CHAN_71: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b7[. ]1\b").unwrap());
pub(crate) static RE_CHAN_51: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b5[. ]1\b").unwrap());
pub(crate) static RE_CHAN_20: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b2[. ]0\b").unwrap());
pub(crate) static RE_CHAN_STEREO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bstereo\b").unwrap());
pub(crate) static RE_CHAN_MONO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bmono\b").unwrap());

// ---------------------------------------------------------------------------
// HDR
// ---------------------------------------------------------------------------

pub(crate) static RE_HDR_DV: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:Dolby[ .\-]*Vision|DV|DoVi)\b").unwrap()
});
pub(crate) static RE_HDR_HDR10PLUS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:HDR10\+|HDR10Plus)\b").unwrap());
pub(crate) static RE_HDR_HDR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bHDR(?:10)?\b").unwrap());
pub(crate) static RE_HDR_SDR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bSDR\b").unwrap());

// ---------------------------------------------------------------------------
// Bit depth
// ---------------------------------------------------------------------------

pub(crate) static RE_BIT_DEPTH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:10[- ]?bit|Hi10[Pp]?|HEVC\s*10)\b").unwrap());
pub(crate) static RE_BIT_DEPTH_HDR10: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bHDR10").unwrap());

// ---------------------------------------------------------------------------
// Group
// ---------------------------------------------------------------------------

pub(crate) static RE_GROUP_DASH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"-([A-Za-z0-9_]+)(?:\.[a-zA-Z]{2,4})?$").unwrap());
pub(crate) static RE_GROUP_BRACKET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\[([^\]]+)\]").unwrap());

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

pub(crate) static RE_SIZE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(\d+(?:\.\d+)?\s*(?:GB|MB|TB|KB))").unwrap());
pub(crate) static RE_EXTENSION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\.(mkv|mp4|avi|wmv|flv|mov|webm|ts|m4v|mpg|mpeg)$").unwrap());
pub(crate) static RE_CONTAINER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(mkv|mp4|avi|wmv|flv|mov|webm|ts|m4v)\b").unwrap());
pub(crate) static RE_3D: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:3D|SBS|half[- ]?(?:OU|SBS))\b").unwrap());
pub(crate) static RE_COMPLETE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:complete|full\s*series|complete\s*series|integrale|completa)\b").unwrap()
});
pub(crate) static RE_DATE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b((?:19|20)\d{2})[.\-/](0[1-9]|1[0-2])[.\-/](0[1-9]|[12]\d|3[01])\b").unwrap()
});
pub(crate) static RE_BITRATE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d+(?:\.\d+)?\s*(?:[KkMm]bps|[KkMm]b/s))\b").unwrap());
pub(crate) static RE_REGION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(R[0-9]|Region\s*[0-9])\b").unwrap());
pub(crate) static RE_SITE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:www\.)?([\w]+\.[\w]+)\s*-\s*").unwrap());
pub(crate) static RE_VOLUME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:vol(?:ume)?\.?\s*)(\d{1,3})(?:\s*[-&]\s*(?:vol(?:ume)?\.?\s*)?(\d{1,3}))?")
        .unwrap()
});
pub(crate) static RE_EDITION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b((?:Anniversary|Ultimate|Diamond|Collectors?|Special)\s*Edition|Directors?\s*Cut|Extended\s*Edition|Extended\s*Cut|Theatrical(?:\s*(?:Cut|Edition))?|Uncut|IMAX(?:\s*Edition)?|Remastered|Criterion\s*(?:Collection|Edition)|Final\s*Cut|Limited\s*Edition|Deluxe\s*Edition)\b").unwrap()
});
pub(crate) static RE_COUNTRY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(US|UK|AU|NZ|CA|IE|FR|DE|ES|IT|NL|BE|AT|CH|SE|NO|DK|FI|JP|KR|CN|TW|HK|IN|BR|MX|AR|CL|CO|RU|PL|CZ|HU|RO|BG|HR|RS|SK|SI|UA|GR|TR|TH|PH|MY|SG|ID|VN)\b").unwrap()
});
pub(crate) static RE_NON_ENGLISH_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[\p{Han}\p{Katakana}\p{Hiragana}\p{Cyrillic}\p{Arabic}\p{Thai}\p{Hangul}\p{Devanagari}\p{Bengali}\p{Tamil}\p{Telugu}\p{Malayalam}\p{Kannada}\p{Gujarati}()\[\]\s.。，、·]+").unwrap()
});
pub(crate) static RE_EXTRAS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(trailer|sample|featurette|behind\s*the\s*scenes|deleted\s*scenes?|bonus|extras?|interview|commentary|making\s*of|bloopers?|gag\s*reel)\b").unwrap()
});
pub(crate) static RE_TITLE_QUALITY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:\b(?:HD[ .\-]*)?T(?:ELE)?S(?:YNC)?(?:Rip)?|\b(?:HD[ .\-]*)?T(?:ELE)?C(?:INE)?(?:Rip)?|\bBlu[ .\-]*Ray|\bremux|\bWEB[ .\-]*(?:DL|Rip)|\bWEB\b|\bHDTV|\bDVD|\bBDRip|\bBRRip|\bHDRip|\bCAM|\bPDTV|\bSAT[ .\-]*Rip|\bPPV|\bVHS)\b").unwrap()
});

// ---------------------------------------------------------------------------
// Trash detection
// ---------------------------------------------------------------------------

pub(crate) static RE_DVB: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bDVB\b").unwrap());
pub(crate) static RE_LEAKED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bLEAKED\b").unwrap());
pub(crate) static RE_SPRINT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bS[ .\-]*print\b").unwrap());
pub(crate) static RE_R6: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bR6\b").unwrap());

// ---------------------------------------------------------------------------
// Boolean flags
// ---------------------------------------------------------------------------

pub(crate) static RE_PROPER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bPROPER\b").unwrap());
pub(crate) static RE_REPACK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bREPACK\b").unwrap());
pub(crate) static RE_RETAIL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bRETAIL\b").unwrap());
pub(crate) static RE_UPSCALED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bUPSCAL(?:E[Dd]?)?\b").unwrap());
pub(crate) static RE_REMASTERED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bREMASTER(?:ED)?\b").unwrap());
pub(crate) static RE_EXTENDED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bEXTENDED\b").unwrap());
pub(crate) static RE_CONVERTED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bCONVERT(?:ED)?\b").unwrap());
pub(crate) static RE_UNRATED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bUNRATED\b").unwrap());
pub(crate) static RE_UNCENSORED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bUNCENSORED\b").unwrap());
pub(crate) static RE_DUBBED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:DUBBED|DUAL(?:[ .\-]*AUDIO)?)\b").unwrap());
pub(crate) static RE_SUBBED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:SUBBED|SUBS?|SUBTITL(?:E[DS]?|ING))\b").unwrap());
pub(crate) static RE_HARDCODED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:HARDCODED|HC)\b").unwrap());
pub(crate) static RE_DOCUMENTARY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:DOCUMENTARY|DOCU)\b").unwrap());
pub(crate) static RE_COMMENTARY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bCOMMENTARY\b").unwrap());
pub(crate) static RE_ADULT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:XXX|PORN|ADULT)\b").unwrap());
pub(crate) static RE_PPV: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bPPV\b").unwrap());
pub(crate) static RE_SCENE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bSCENE\b").unwrap());

// Network patterns are defined as a table in detect.rs
