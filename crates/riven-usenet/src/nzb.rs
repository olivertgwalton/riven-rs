//! NZB file parser.
//!
//! NZB is XML describing which Usenet articles, in which newsgroups, compose
//! a given binary. We extract just the bits the streamer needs:
//! per-file ordered segments + the article message-ids and the (encoded) byte
//! count the poster declared.

use std::collections::HashMap;

use quick_xml::Reader;
use quick_xml::events::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbFile {
    pub subject: String,
    pub poster: String,
    pub groups: Vec<String>,
    pub segments: Vec<NzbSegment>,
}

/// Parsed NZB document: the head metadata (`<meta type="...">` entries) plus
/// the per-file segment lists. Head metadata is keyed by the `type` attribute
/// (lowercased) and carries the inner text value — typical entries are
/// `title`, `password`, `category`, `tag`. Both decypharr and nzbdav use this
/// for fallback naming and password-protected archives.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NzbDocument {
    pub meta: HashMap<String, String>,
    pub files: Vec<NzbFile>,
}

impl NzbDocument {
    /// Best-effort release title for the NZB. Used as a fallback when inner
    /// filenames are obfuscated. Priority mirrors decypharr's
    /// `determineNZBName`: meta `name` → meta `title` → first file's
    /// subject-derived filename (without extension).
    pub fn release_title(&self) -> Option<String> {
        if let Some(name) = self.meta.get("name").filter(|s| !s.is_empty()) {
            return Some(name.clone());
        }
        if let Some(title) = self.meta.get("title").filter(|s| !s.is_empty()) {
            return Some(title.clone());
        }
        let first = self.files.first()?;
        let raw = filename_from_subject(&first.subject);
        // Strip the extension — the caller will reattach its own.
        let stem = match raw.rfind('.') {
            Some(i) if i > 0 => raw[..i].to_string(),
            _ => raw,
        };
        if stem.is_empty() { None } else { Some(stem) }
    }

    /// Password to apply to encrypted archive entries, if any. Sourced from
    /// `<meta type="password">`. nzbdav additionally accepts a `{{pw}}.nzb` /
    /// `password=pw.nzb` filename suffix; that's the caller's responsibility
    /// (the NZB body doesn't carry the filename).
    pub fn password(&self) -> Option<&str> {
        self.meta
            .get("password")
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NzbSegment {
    /// Per-NZB-spec, the encoded article size in bytes (yEnc payload + a few
    /// bytes of header overhead). Decoded size is ~2% smaller; we use this
    /// as an offset proxy until we've actually fetched a segment.
    pub bytes: u64,
    pub number: u32,
    /// Article message-id, without surrounding `<>`.
    pub message_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum NzbError {
    #[error("xml parse error: {0}")]
    Xml(#[from] quick_xml::Error),
    #[error("malformed NZB: {0}")]
    Malformed(&'static str),
}

/// Parse an NZB document. Tolerant: ignores unknown elements, skips files with
/// no segments rather than failing the whole document. Drops head metadata —
/// callers that need it should use [`parse_nzb_document`] instead.
pub fn parse_nzb(xml: &str) -> Result<Vec<NzbFile>, NzbError> {
    parse_nzb_document(xml).map(|d| d.files)
}

/// Full NZB parse: head `<meta>` entries plus per-file segments. Tolerant in
/// the same way as [`parse_nzb`]; head metadata is best-effort and missing
/// entries don't fail the parse.
pub fn parse_nzb_document(xml: &str) -> Result<NzbDocument, NzbError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut meta: HashMap<String, String> = HashMap::new();
    let mut cur_meta_type: Option<String> = None;
    let mut files: Vec<NzbFile> = Vec::new();
    let mut cur_file: Option<NzbFile> = None;
    let mut cur_segment: Option<NzbSegment> = None;
    let mut in_group = false;
    let mut text_target: Option<&'static str> = None;

    loop {
        match reader.read_event()? {
            Event::Eof => break,
            Event::Start(e) => match e.name().as_ref() {
                b"file" => {
                    let mut f = NzbFile {
                        subject: String::new(),
                        poster: String::new(),
                        groups: Vec::new(),
                        segments: Vec::new(),
                    };
                    for attr in e.attributes().flatten() {
                        let val = attr.unescape_value().ok().map(|v| v.into_owned());
                        match (attr.key.as_ref(), val) {
                            (b"subject", Some(v)) => f.subject = v,
                            (b"poster", Some(v)) => f.poster = v,
                            _ => {}
                        }
                    }
                    cur_file = Some(f);
                }
                b"segment" => {
                    let mut s = NzbSegment {
                        bytes: 0,
                        number: 0,
                        message_id: String::new(),
                    };
                    for attr in e.attributes().flatten() {
                        let val = attr.unescape_value().ok().map(|v| v.into_owned());
                        match (attr.key.as_ref(), val) {
                            (b"bytes", Some(v)) => s.bytes = v.parse().unwrap_or(0),
                            (b"number", Some(v)) => s.number = v.parse().unwrap_or(0),
                            _ => {}
                        }
                    }
                    cur_segment = Some(s);
                    text_target = Some("segment");
                }
                b"group" => {
                    in_group = true;
                    text_target = Some("group");
                }
                b"meta" => {
                    // `<meta type="title">Release Name</meta>` style head
                    // metadata. Only entries with a non-empty `type` attribute
                    // are kept; the value is captured on the corresponding
                    // text/cdata event.
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"type"
                            && let Ok(v) = attr.unescape_value()
                        {
                            let key = v.trim().to_ascii_lowercase();
                            if !key.is_empty() {
                                cur_meta_type = Some(key);
                            }
                        }
                    }
                    text_target = Some("meta");
                }
                _ => {}
            },
            Event::Text(t) => {
                let bytes = t.into_inner();
                let text = String::from_utf8_lossy(&bytes).into_owned();
                match text_target.take() {
                    Some("segment") => {
                        if let Some(seg) = cur_segment.as_mut() {
                            seg.message_id = text.trim().trim_matches(|c| c == '<' || c == '>').to_string();
                        }
                    }
                    Some("group") if in_group => {
                        if let Some(file) = cur_file.as_mut() {
                            let g = text.trim().to_string();
                            if !g.is_empty() {
                                file.groups.push(g);
                            }
                        }
                    }
                    Some("meta") => {
                        if let Some(key) = cur_meta_type.take() {
                            let val = text.trim().to_string();
                            if !val.is_empty() {
                                meta.insert(key, val);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Event::CData(c) => {
                let text = String::from_utf8_lossy(c.as_ref()).into_owned();
                match text_target.take() {
                    Some("segment") => {
                        if let Some(seg) = cur_segment.as_mut() {
                            seg.message_id = text
                                .trim()
                                .trim_matches(|c| c == '<' || c == '>')
                                .to_string();
                        }
                    }
                    Some("meta") => {
                        if let Some(key) = cur_meta_type.take() {
                            let val = text.trim().to_string();
                            if !val.is_empty() {
                                meta.insert(key, val);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Event::End(e) => match e.name().as_ref() {
                b"segment" => {
                    if let (Some(file), Some(seg)) = (cur_file.as_mut(), cur_segment.take())
                        && !seg.message_id.is_empty()
                    {
                        file.segments.push(seg);
                    }
                    text_target = None;
                }
                b"group" => {
                    in_group = false;
                    text_target = None;
                }
                b"meta" => {
                    // Clear pending state in case the meta element had no
                    // text body — don't leak `meta` target into the next
                    // event.
                    cur_meta_type = None;
                    text_target = None;
                }
                b"file" => {
                    if let Some(mut file) = cur_file.take() {
                        file.segments.sort_by_key(|s| s.number);
                        if !file.segments.is_empty() {
                            files.push(file);
                        }
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    if files.is_empty() {
        return Err(NzbError::Malformed("no files with segments found"));
    }
    Ok(NzbDocument { meta, files })
}

/// Heuristic check for an obfuscated filename — random hash/blob stems with
/// no release-name structure. A real release name always has at least one
/// separator (`.`, ` `, `-`, `_`).
///
/// Flags:
/// - `abc.xyz...` placeholder prefix.
/// - 32-char hex stem (md5/etag-like).
/// - 40+ char hex/dot stems.
/// - 24+ char alphanumeric stems with no separators (covers iVy/FLUX
///   `VfYc6l3ibzTHwlPkvX1hocwymwUNt6yt`-style names).
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

/// Best-effort filename extractor for a yEnc subject. Used to detect RAR
/// volume patterns. Returns the subject verbatim if no quoted name is found.
pub fn filename_from_subject(subject: &str) -> String {
    if let Some(start) = subject.find('"')
        && let Some(rel_end) = subject[start + 1..].find('"')
    {
        return subject[start + 1..start + 1 + rel_end].to_string();
    }
    subject
        .split_whitespace()
        .find(|t| t.contains('.'))
        .unwrap_or(subject)
        .to_string()
}

/// Decompose a RAR volume filename into `(base, volume_index)`.
///
/// The `base` is what decypharr's `groupFiles` calls the "group key": every
/// volume of the same archive normalises to the same base, so multi-set NZBs
/// (e.g. season packs where each episode has its own RAR set) split cleanly
/// into one group per inner archive.
///
/// Strips `.partNN.rar`, `.rNN`, or plain `.rar` from the filename to derive
/// the base. Returns `None` if the filename isn't a recognised RAR volume
/// name.
pub fn rar_volume_info(filename: &str) -> Option<(String, u32)> {
    let lower = filename.to_ascii_lowercase();
    let bytes = lower.as_bytes();

    // .partNN.rar (NN any width)
    if let Some(rar_pos) = lower.rfind(".rar")
        && rar_pos + 4 == lower.len()
    {
        let prefix = &lower[..rar_pos];
        if let Some(part_pos) = prefix.rfind(".part") {
            let num = &prefix[part_pos + 5..];
            if !num.is_empty()
                && num.bytes().all(|b| b.is_ascii_digit())
                && let Ok(n) = num.parse::<u32>()
                && n >= 1
            {
                return Some((prefix[..part_pos].to_string(), n - 1));
            }
        }
        // Plain `.rar` (volume 0 of an `.rNN`-style continuation set).
        return Some((prefix.to_string(), 0));
    }

    // .rNN / .rNNN (2- or 3-digit suffix after a leading `.r`).
    if bytes.len() >= 4 {
        for tail_len in [3, 4] {
            if bytes.len() < tail_len {
                continue;
            }
            let tail = &lower[lower.len() - tail_len..];
            if tail.starts_with(".r")
                && tail.bytes().skip(2).all(|b| b.is_ascii_digit())
                && let Ok(n) = tail[2..].parse::<u32>()
            {
                let base = lower[..lower.len() - tail_len].to_string();
                return Some((base, n + 1));
            }
        }
    }

    None
}

/// Group NZB files into RAR archive sets by their normalised base name.
///
/// Each returned `Vec<usize>` is the ordered (by volume index) indices of
/// one logical archive's volumes. A movie release with a single archive
/// produces one group; a season pack with one archive per episode produces
/// N groups. Non-RAR files (par2/sfv/nfo/.mkv) are excluded.
///
/// Mirrors decypharr's `groupFiles` (pkg/usenet/parser/parser.go:231).
pub fn detect_rar_volume_groups(files: &[NzbFile]) -> Vec<Vec<usize>> {
    let mut groups: HashMap<String, Vec<(u32, usize)>> = HashMap::new();
    for (idx, f) in files.iter().enumerate() {
        let filename = filename_from_subject(&f.subject);
        if let Some((base, vol)) = rar_volume_info(&filename) {
            groups.entry(base).or_default().push((vol, idx));
        }
    }
    let mut out: Vec<(String, Vec<usize>)> = groups
        .into_iter()
        .map(|(base, mut indexed)| {
            indexed.sort_by_key(|(vol, _)| *vol);
            (base, indexed.into_iter().map(|(_, i)| i).collect())
        })
        .collect();
    // Stable ordering by group base name — keeps virtual file order
    // deterministic across reingests of the same NZB.
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.into_iter().map(|(_, v)| v).collect()
}

/// True if the file's subject looks like a video/media payload rather than a
/// PAR2 / NFO / SFV companion file. Used to pick the right file to stream
/// from a multi-file NZB.
pub fn looks_like_media(file: &NzbFile) -> bool {
    let lower = file.subject.to_ascii_lowercase();
    const MEDIA_EXTS: &[&str] = &[
        ".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm", ".ts", ".m2ts", ".wmv",
    ];
    const SKIP_EXTS: &[&str] = &[".par2", ".nfo", ".sfv", ".srr", ".nzb"];
    if SKIP_EXTS.iter().any(|e| lower.contains(e)) {
        return false;
    }
    MEDIA_EXTS.iter().any(|e| lower.contains(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_nzb() {
        let xml = r#"<?xml version="1.0"?>
        <nzb>
          <file poster="me@example" subject="movie.mkv (1/2)">
            <groups><group>alt.binaries.movies</group></groups>
            <segments>
              <segment bytes="750000" number="1">abc@host</segment>
              <segment bytes="750000" number="2">def@host</segment>
            </segments>
          </file>
        </nzb>"#;
        let files = parse_nzb(xml).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].segments.len(), 2);
        assert_eq!(files[0].segments[0].message_id, "abc@host");
        assert_eq!(files[0].groups, vec!["alt.binaries.movies"]);
    }

    #[test]
    fn segments_are_sorted() {
        let xml = r#"<?xml version="1.0"?>
        <nzb><file poster="x" subject="x.mkv">
            <segments>
              <segment bytes="1" number="2">b@h</segment>
              <segment bytes="1" number="1">a@h</segment>
            </segments>
        </file></nzb>"#;
        let files = parse_nzb(xml).unwrap();
        assert_eq!(files[0].segments[0].number, 1);
        assert_eq!(files[0].segments[1].number, 2);
    }

    #[test]
    fn detects_media_extension() {
        let mut f = NzbFile {
            subject: "Some.Movie.2024.1080p.mkv".into(),
            poster: String::new(),
            groups: vec![],
            segments: vec![],
        };
        assert!(looks_like_media(&f));
        f.subject = "movie.par2".into();
        assert!(!looks_like_media(&f));
    }

    #[test]
    fn rar_volume_info_strips_base() {
        let (base, vol) = rar_volume_info("Show.S01E01.part01.rar").unwrap();
        assert_eq!(base, "show.s01e01");
        assert_eq!(vol, 0);

        let (base, vol) = rar_volume_info("Show.S01E01.part12.rar").unwrap();
        assert_eq!(base, "show.s01e01");
        assert_eq!(vol, 11);

        let (base, vol) = rar_volume_info("Show.S01E01.rar").unwrap();
        assert_eq!(base, "show.s01e01");
        assert_eq!(vol, 0);

        let (base, vol) = rar_volume_info("Show.S01E01.r05").unwrap();
        assert_eq!(base, "show.s01e01");
        assert_eq!(vol, 6);

        assert!(rar_volume_info("Show.S01E01.mkv").is_none());
        assert!(rar_volume_info("Show.S01E01.par2").is_none());
    }

    #[test]
    fn detects_multiple_rar_groups_for_season_pack() {
        // iVy-style season pack: one RAR set per episode. Should produce
        // one group per episode, not one giant set across all episodes.
        let mk = |s: &str| NzbFile {
            subject: format!(r#""{s}" yEnc"#),
            poster: String::new(),
            groups: vec![],
            segments: vec![],
        };
        let files = vec![
            mk("Show.S01E01.part01.rar"),
            mk("Show.S01E02.part01.rar"),
            mk("Show.S01E01.part02.rar"),
            mk("Show.S01E02.part02.rar"),
            mk("Show.S01E02.par2"),
            mk("Show.S01E01.par2"),
        ];
        let groups = detect_rar_volume_groups(&files);
        assert_eq!(groups.len(), 2, "expected two RAR groups, one per episode");
        // Each group has its own two volumes, ordered by volume index.
        for g in &groups {
            assert_eq!(g.len(), 2);
            let v0 = &files[g[0]].subject;
            let v1 = &files[g[1]].subject;
            assert!(v0.contains(".part01.rar"));
            assert!(v1.contains(".part02.rar"));
        }
        // Different base names → different groups.
        let g0_v0 = &files[groups[0][0]].subject;
        let g1_v0 = &files[groups[1][0]].subject;
        assert_ne!(g0_v0, g1_v0);
    }
}
