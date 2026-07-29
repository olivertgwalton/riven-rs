//! Filename heuristics shared across crates that never see each other's types.
//!
//! `riven-queue` and `riven-usenet` both need to reason about release
//! filenames, and neither depends on the other — so these live here rather than
//! being copied into both.

/// Heuristic check for an obfuscated filename — random hash/blob stems with no
/// release-name structure. Used to decide whether to fall back to the NZB
/// release title instead of trusting the filename.
///
/// Flags:
/// - `abc.xyz...` placeholder prefix emitted by some indexers.
/// - 32-char hex stem (md5/etag-like).
/// - 40+ char hex/dot stems.
/// - 24+ char alphanumeric stems with no separators (covers iVy/FLUX
///   `VfYc6l3ibzTHwlPkvX1hocwymwUNt6yt`-style names).
///
/// Deliberately conservative: only flags a stem that is plausibly random. A
/// real release name always carries at least one separator (`.`, ` `, `-`,
/// `_`), so excluding those covers every well-formed scene/p2p release.
pub fn looks_obfuscated(filename: &str) -> bool {
    let stem = match filename.rfind('.') {
        Some(i) if i > 0 => &filename[..i],
        _ => filename,
    };
    if stem.is_empty() {
        return false;
    }
    if stem.starts_with("abc.xyz") {
        return true;
    }
    let lower = stem.to_ascii_lowercase();
    let is_hex = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit());
    if stem.len() == 32 && is_hex(&lower) {
        return true;
    }
    if lower.len() >= 40 && lower.chars().all(|c| c.is_ascii_hexdigit() || c == '.') {
        return true;
    }
    if stem.len() >= 24
        && !stem.contains(['.', ' ', '-', '_'])
        && stem.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::looks_obfuscated;

    #[test]
    fn flags_random_stems() {
        assert!(looks_obfuscated("VfYc6l3ibzTHwlPkvX1hocwymwUNt6yt.mkv"));
        assert!(looks_obfuscated("d41d8cd98f00b204e9800998ecf8427e.mkv"));
        assert!(looks_obfuscated("abc.xyz.something.mkv"));
    }

    #[test]
    fn keeps_well_formed_release_names() {
        assert!(!looks_obfuscated(
            "Some.Show.S01E01.1080p.WEB-DL.DDP5.1.H.264-GROUP.mkv"
        ));
        assert!(!looks_obfuscated("Movie Title (2019) 2160p.mkv"));
        assert!(!looks_obfuscated("short.mkv"));
        assert!(!looks_obfuscated(""));
    }
}
