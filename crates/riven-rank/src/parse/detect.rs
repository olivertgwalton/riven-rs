use regex::Regex;
use std::sync::LazyLock;

use super::patterns::*;
use super::ParsedData;

// =============================================================================
// Network detection
// =============================================================================

static NETWORK_TABLE: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    [
        (r"(?i)\b(?:ATVP|ATV\+|Apple\s*TV\+?)\b", "Apple TV"),
        (r"(?i)\b(?:AMZN|Amazon)\b", "Amazon"),
        (r"(?i)\b(?:NF|Netflix)\b", "Netflix"),
        (r"(?i)\b(?:NICK(?:elodeon)?)\b", "Nickelodeon"),
        (r"(?i)\b(?:DSNP|DNSP|Disney\s*\+?|D\+)\b", "Disney"),
        (r"(?i)\b(?:HMAX|HBO(?:\s*Max)?)\b", "HBO"),
        (r"(?i)\bHULU\b", "Hulu"),
        (r"(?i)\b(?:PCOK|Peacock)\b", "Peacock"),
        (r"(?i)\b(?:PMTP|Paramount\+?)\b", "Paramount"),
        (r"(?i)\bCBS\b", "CBS"),
        (r"(?i)\bNBC\b", "NBC"),
        (r"(?i)\bAMC\b", "AMC"),
        (r"(?i)\bPBS\b", "PBS"),
        (r"(?i)\b(?:CC|Comedy\s*Central)\b", "Comedy Central"),
        (r"(?i)\b(?:CRAV|Crave)\b", "Crave"),
        (r"(?i)\b(?:DCU|DC\s*Universe)\b", "DC Universe"),
        (r"(?i)\b(?:DSNY|DisneyNOW)\b", "DisneyNOW"),
        (r"(?i)\bESPN\b", "ESPN"),
        (r"(?i)\bFOX\b", "FOX"),
        (r"(?i)\b(?:FUNI|Funimation)\b", "Funimation"),
        (
            r"(?i)\b(?:RED|YouTube\s*(?:Red|Premium))\b",
            "YouTube Premium",
        ),
        (r"(?i)\bSTAN\b", "Stan"),
        (r"(?i)\b(?:STZ|STARZ)\b", "STARZ"),
        (r"(?i)\b(?:SHO|Showtime)\b", "Showtime"),
        (r"(?i)\bVRV\b", "VRV"),
        (r"(?i)\b(?:Crunchyroll|[. \-]CR[. \-])\b", "Crunchyroll"),
        (r"(?i)\b(?:iT|iTunes)\b", "iTunes"),
        (r"(?i)\bVUDU\b", "VUDU"),
        (r"(?i)\bROKU\b", "Roku"),
        (r"(?i)\bTVNZ\b", "TVNZ"),
        (r"(?i)\bVICE\b", "VICE"),
        (r"(?i)\bSony\b", "Sony"),
        (r"(?i)\bHallmark\b", "Hallmark"),
        (r"(?i)\bAdult\s*\.?\s*Swim\b", "Adult Swim"),
        (r"(?i)\b(?:Animal\s*\.?\s*Planet|ANPL)\b", "Animal Planet"),
        (r"(?i)\bCartoon\s*\.?\s*Network\b", "Cartoon Network"),
    ]
    .into_iter()
    .map(|(pat, name)| (Regex::new(pat).unwrap(), name))
    .collect()
});

/// Detect the streaming network/service from the raw title.
pub(crate) fn detect_network(raw: &str, data: &mut ParsedData) {
    for (re, name) in NETWORK_TABLE.iter() {
        if re.is_match(raw) {
            data.network = Some(name.to_string());
            return;
        }
    }
}

// =============================================================================
// False group detection
// =============================================================================

/// Check if a detected group name is actually a false positive (codec, format, etc.).
pub(crate) fn is_false_group(group: &str) -> bool {
    let lower = group.to_lowercase();
    matches!(
        lower.as_str(),
        "mkv"
            | "mp4"
            | "avi"
            | "wmv"
            | "flv"
            | "mov"
            | "webm"
            | "ts"
            | "m4v"
            | "720p"
            | "1080p"
            | "2160p"
            | "480p"
            | "4k"
            | "x264"
            | "x265"
            | "h264"
            | "h265"
            | "hevc"
            | "avc"
            | "xvid"
            | "divx"
            | "av1"
            | "aac"
            | "ac3"
            | "dts"
            | "flac"
            | "mp3"
            | "opus"
            | "pcm"
            | "lpcm"
            | "atmos"
            | "truehd"
            | "eac3"
            | "sdr"
            | "hdr"
            | "hdr10"
            | "dv"
            | "dovi"
            | "eng"
            | "english"
            | "proper"
            | "repack"
            | "retail"
            | "extended"
            | "remastered"
    ) || group == "-"
        || group.is_empty()
}

// =============================================================================
// Trash detection
// =============================================================================

const TRASH_QUALITIES: &[&str] = &["CAM", "TeleSync", "TeleCine", "SCR", "R5", "PDTV"];

const TRASH_EXTRAS: &[&str] = &["sample", "trailer", "deleted scene"];

/// Detect whether the torrent is trash based on quality and other markers.
pub(crate) fn detect_trash(raw: &str, data: &ParsedData) -> bool {
    data.quality
        .as_deref()
        .map_or(false, |q| TRASH_QUALITIES.contains(&q))
        || RE_SPRINT.is_match(raw)
        || RE_Q_PRE_DVD.is_match(raw)
        || RE_DVB.is_match(raw)
        || RE_Q_SATRIP.is_match(raw)
        || RE_LEAKED.is_match(raw)
        || RE_R6.is_match(raw)
        || RE_THREESIXTYP.is_match(raw)
        || data.audio.iter().any(|a| a == "HQ Clean Audio")
        || data
            .extras
            .iter()
            .any(|e| TRASH_EXTRAS.contains(&e.to_lowercase().as_str()))
}

// =============================================================================
// Scene detection
// =============================================================================

/// Detect if this is a scene release based on group names and patterns.
pub(crate) fn detect_scene(raw: &str) -> bool {
    RE_SCENE.is_match(raw) || (RE_SCENE_WEB.is_match(raw) && !RE_Q_WEBDL.is_match(raw))
}

// =============================================================================
// Anime detection
// =============================================================================

/// Known anime fansub/encoding groups.
static ANIME_GROUPS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    vec![
        "HorribleSubs",
        "SubsPlease",
        "Erai-raws",
        "EMBER",
        "ASW",
        "Judas",
        "Tsundere-Raws",
        "Anime Time",
        "AnimeRG",
        "SSA",
        "Cleo",
        "Beatrice-Raws",
        "Asuka-Raws",
        "Fumi-Raws",
        "Moozzi2",
        "Commie",
        "Final8",
        "Anime Land",
        "DameDesuYo",
        "FFF",
        "GJM",
        "Kaleido",
        "CherryPie",
        "Vivid",
        "Coalgirls",
        "DarkDream",
        "Elysium",
        "Underwater",
        "UTW",
        "WhyNot",
        "Zurako",
        "SALLiANCE",
        "[DB]",
        "DeadFish",
        "SallySubs",
        "VCB-Studio",
        "Reaktor",
        "CBM",
        "CTR",
        "MTBB",
        "Arid",
        "AkihitoSubs",
        "LostYears",
        "ToonsHub",
        "DKB",
        "Hakata Ramen",
        "YURASUKA",
        "Yameii",
        "Tenrai-Sensei",
        "Ember",
        "ADN",
    ]
});

/// Detect if this is an anime release based on groups and patterns.
pub(crate) fn detect_anime(raw: &str, data: &ParsedData) -> bool {
    // Check for anime episode code (CRC32)
    if data.episode_code.is_some() {
        return true;
    }

    // Check for known anime groups
    if let Some(group) = &data.group {
        let lower = group.to_lowercase();
        for anime_group in ANIME_GROUPS.iter() {
            if lower == anime_group.to_lowercase() || lower.contains(&anime_group.to_lowercase()) {
                return true;
            }
        }
    }

    // Check for anime extras
    if data
        .extras
        .iter()
        .any(|e| matches!(e.as_str(), "NCED" | "NCOP" | "NC" | "OVA" | "ED" | "OP"))
    {
        return true;
    }

    // Check for anime keyword
    static RE_ANIME: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)\b(?:anime?|anim)\b").unwrap());
    RE_ANIME.is_match(raw)
}
