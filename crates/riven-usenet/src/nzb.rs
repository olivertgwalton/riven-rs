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
    pub segments: crate::segments::SegmentList,
}

/// Parsed NZB document: the head metadata (`<meta type="...">` entries) plus
/// the per-file segment lists. Head metadata is keyed by the `type` attribute
/// (lowercased) and carries the inner text value — typical entries are
/// `title`, `password`, `category`, `tag`. Used for fallback naming and
/// password-protected archives.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NzbDocument {
    pub meta: HashMap<String, String>,
    pub files: Vec<NzbFile>,
}

impl NzbDocument {
    /// Best-effort release title for the NZB. Used as a fallback when inner
    /// filenames are obfuscated. Priority: meta `name` → meta `title` → first
    /// file's subject-derived filename (without extension).
    pub fn release_title(&self) -> Option<String> {
        if let Some(name) = self.meta.get("name").filter(|s| !s.is_empty()) {
            return Some(name.clone());
        }
        if let Some(title) = self.meta.get("title").filter(|s| !s.is_empty()) {
            return Some(title.clone());
        }
        let first = self.files.first()?;
        let raw = filename_from_subject(&first.subject);
        let stem = match raw.rfind('.') {
            Some(i) if i > 0 => raw[..i].to_string(),
            _ => raw,
        };
        if stem.is_empty() { None } else { Some(stem) }
    }

    /// Password to apply to encrypted archive entries, if any. Sourced from
    /// `<meta type="password">`. A `{{pw}}.nzb` / `password=pw.nzb` filename
    /// suffix is the caller's responsibility (the NZB body doesn't carry the
    /// filename).
    pub fn password(&self) -> Option<&str> {
        self.meta
            .get("password")
            .map(std::string::String::as_str)
            .filter(|s| !s.is_empty())
    }
}

/// A segment as it appears in the NZB, before ordering.
///
/// `number` lives only here. It is load-bearing exactly once — sorting a file's
/// segments into order — after which it is the index and nothing reads it. Kept
/// out of [`NzbSegment`] because that is the form stored for every segment of
/// every release: 2 510 rows and 4 489 MB of it in this library, where the
/// field costs 8 bytes of struct (with padding) and its own JSON key.
///
/// Old rows still carry `"number"`. serde ignores unknown fields, so they
/// deserialise unchanged and simply stop paying for it when next written.
#[derive(Debug, Clone)]
struct ParsedSegment {
    number: u32,
    bytes: u64,
    message_id: String,
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

/// Release title without parsing the whole document — for log lines only.
///
/// [`parse_nzb_document`] materializes every segment of every file, which for
/// a season pack is tens of megabytes of allocation; naming a release in a
/// warning must never cost that. This reads the head `<meta>` entries and
/// stops at the first `<file>` subject, which is all [`NzbDocument::release_title`]
/// would have used anyway.
pub fn peek_release_title(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut meta: HashMap<String, String> = HashMap::new();
    let mut cur_meta_type: Option<String> = None;

    loop {
        match reader.read_event() {
            Err(_) | Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => match e.name().as_ref() {
                b"meta" => {
                    cur_meta_type = e
                        .attributes()
                        .flatten()
                        .find(|attr| attr.key.as_ref() == b"type")
                        .and_then(|attr| {
                            attr.normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                .ok()
                        })
                        .map(|v| v.to_ascii_lowercase());
                }
                b"file" => {
                    // Head metadata is complete by the first file element.
                    let subject = e
                        .attributes()
                        .flatten()
                        .find(|attr| attr.key.as_ref() == b"subject")
                        .and_then(|attr| {
                            attr.normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                .ok()
                        })
                        .map(std::borrow::Cow::into_owned)
                        .unwrap_or_default();
                    return title_from_meta(&meta).or_else(|| {
                        let raw = filename_from_subject(&subject);
                        let stem = match raw.rfind('.') {
                            Some(i) if i > 0 => raw[..i].to_string(),
                            _ => raw,
                        };
                        (!stem.is_empty()).then_some(stem)
                    });
                }
                _ => {}
            },
            Ok(Event::Text(t)) => {
                if let Some(key) = cur_meta_type.take()
                    && let Ok(v) = t.decode()
                {
                    meta.insert(key, v.into_owned());
                }
            }
            Ok(_) => {}
        }
    }
    title_from_meta(&meta)
}

fn title_from_meta(meta: &HashMap<String, String>) -> Option<String> {
    meta.get("name")
        .or_else(|| meta.get("title"))
        .filter(|s| !s.is_empty())
        .cloned()
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
    let mut cur_segment: Option<ParsedSegment> = None;
    // Ordered on `</file>`, then converted to the stored form.
    let mut cur_segments: Vec<ParsedSegment> = Vec::new();
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
                        segments: crate::segments::SegmentList::default(),
                    };
                    for attr in e.attributes().flatten() {
                        let val = attr
                            .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                            .ok()
                            .map(std::borrow::Cow::into_owned);
                        match (attr.key.as_ref(), val) {
                            (b"subject", Some(v)) => f.subject = v,
                            (b"poster", Some(v)) => f.poster = v,
                            _ => {}
                        }
                    }
                    cur_file = Some(f);
                }
                b"segment" => {
                    let mut s = ParsedSegment {
                        number: 0,
                        bytes: 0,
                        message_id: String::new(),
                    };
                    for attr in e.attributes().flatten() {
                        let val = attr
                            .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                            .ok()
                            .map(std::borrow::Cow::into_owned);
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
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"type"
                            && let Ok(v) = attr.normalized_value(quick_xml::XmlVersion::Implicit1_0)
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
                            seg.message_id = text
                                .trim()
                                .trim_matches(|c| c == '<' || c == '>')
                                .to_string();
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
                    if let (Some(_), Some(seg)) = (cur_file.as_ref(), cur_segment.take())
                        && !seg.message_id.is_empty()
                    {
                        cur_segments.push(seg);
                    }
                    text_target = None;
                }
                b"group" => {
                    in_group = false;
                    text_target = None;
                }
                b"meta" => {
                    cur_meta_type = None;
                    text_target = None;
                }
                b"file" => {
                    if let Some(mut file) = cur_file.take() {
                        // Sort while `number` still exists, then drop it.
                        cur_segments.sort_by_key(|s| s.number);
                        let mut builder = crate::segments::SegmentListBuilder::with_capacity(
                            cur_segments.len(),
                            cur_segments.len() * 48,
                        );
                        for seg in cur_segments.drain(..) {
                            builder.push(&seg.message_id, seg.bytes);
                        }
                        file.segments = builder.build();
                        if !file.segments.is_empty() {
                            files.push(file);
                        }
                    } else {
                        cur_segments.clear();
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

pub use riven_core::filename::looks_obfuscated;

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
/// The `base` is a group key — every volume of the same archive normalises to
/// the same base, so multi-set NZBs (e.g. season packs where each episode has
/// its own RAR set) split cleanly into one group per inner archive.
///
/// Strips `.partNN.rar`, `.rNN`, or plain `.rar` from the filename to derive
/// the base. Returns `None` if the filename isn't a recognised RAR volume
/// name.
pub fn rar_volume_info(filename: &str) -> Option<(String, u32)> {
    let lower = filename.to_ascii_lowercase();
    let bytes = lower.as_bytes();

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
        return Some((prefix.to_string(), 0));
    }

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
pub fn detect_rar_volume_groups(files: &[NzbFile]) -> Vec<Vec<usize>> {
    let names: Vec<String> = files
        .iter()
        .map(|f| filename_from_subject(&f.subject))
        .collect();
    detect_rar_volume_groups_by_name(&names)
}

/// Same grouping as [`detect_rar_volume_groups`], but keyed off caller-
/// supplied names rather than each file's own subject. Used when every
/// subject in the NZB is fully obfuscated (a bare hash with no surviving
/// extension at all, so nothing in the subject can identify a RAR volume)
/// and the real volume names have instead been recovered from the
/// release's own PAR2 `FileDesc` packets.
pub fn detect_rar_volume_groups_by_name(names: &[String]) -> Vec<Vec<usize>> {
    let mut groups: HashMap<String, Vec<(u32, usize)>> = HashMap::new();
    for (idx, filename) in names.iter().enumerate() {
        if let Some((base, vol)) = rar_volume_info(filename) {
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
    const SKIP_EXTS: &[&str] = &[".par2", ".nfo", ".sfv", ".srr", ".nzb", ".7z", ".zip"];
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
        assert_eq!(files[0].segments.id(0), Some("abc@host"));
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
        // `number` is gone from the stored form; ordering is what it bought,
        // so ordering is what this asserts. The document lists them 2 then 1.
        assert_eq!(files[0].segments.id(0), Some("a@h"));
        assert_eq!(files[0].segments.id(1), Some("b@h"));
    }

    #[test]
    fn detects_media_extension() {
        let mut f = NzbFile {
            subject: "Some.Movie.2024.1080p.mkv".into(),
            poster: String::new(),
            groups: vec![],
            segments: crate::segments::SegmentList::default(),
        };
        assert!(looks_like_media(&f));
        f.subject = "movie.par2".into();
        assert!(!looks_like_media(&f));
        f.subject = "CCal2PLGYYDFQNVyj.7z.041".into();
        assert!(!looks_like_media(&f));
        f.subject = "archive.zip".into();
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
        let mk = |s: &str| NzbFile {
            subject: format!(r#""{s}" yEnc"#),
            poster: String::new(),
            groups: vec![],
            segments: crate::segments::SegmentList::default(),
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
        for g in &groups {
            assert_eq!(g.len(), 2);
            let v0 = &files[g[0]].subject;
            let v1 = &files[g[1]].subject;
            assert!(v0.contains(".part01.rar"));
            assert!(v1.contains(".part02.rar"));
        }
        let g0_v0 = &files[groups[0][0]].subject;
        let g1_v0 = &files[groups[1][0]].subject;
        assert_ne!(g0_v0, g1_v0);
    }

    #[test]
    fn subject_based_grouping_finds_nothing_when_every_subject_is_a_bare_hash() {
        // Real-world obfuscated posts: the subject carries no extension at
        // all, not even a disguised one — `filename_from_subject` returns
        // just the hash, so `rar_volume_info` has nothing to parse.
        let mk = |s: &str| NzbFile {
            subject: format!(r#""{s}" yEnc (01/68)"#),
            poster: String::new(),
            groups: vec![],
            segments: crate::segments::SegmentList::default(),
        };
        let files = vec![
            mk("8badd0f960beffdba23932997b03f927"),
            mk("11bbffea29666529a3a0a5bee1cc6357"),
            mk("d00c1ce31c1e324cd1a73babfdae6872"),
        ];
        assert!(detect_rar_volume_groups(&files).is_empty());
    }

    #[test]
    fn detect_rar_volume_groups_by_name_rescues_par2_recovered_names() {
        // Same three volumes as above, but grouped by names recovered from
        // the release's PAR2 FileDesc packets instead of the (useless)
        // subjects — this is what `recover_rar_groups_from_par2` feeds in.
        let recovered = vec![
            "Show.S01E01.r00".to_string(),
            "Show.S01E01.r01".to_string(),
            "Show.S01E01.rar".to_string(),
        ];
        let groups = detect_rar_volume_groups_by_name(&recovered);
        assert_eq!(
            groups.len(),
            1,
            "expected the three volumes to form one set"
        );
        assert_eq!(
            groups[0],
            vec![2, 0, 1],
            "ordered rar, r00, r01 by volume index"
        );
    }
}
