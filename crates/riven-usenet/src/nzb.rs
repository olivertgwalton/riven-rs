//! NZB file parser.
//!
//! NZB is XML describing which Usenet articles, in which newsgroups, compose
//! a given binary. We extract just the bits the streamer needs:
//! per-file ordered segments + the article message-ids and the (encoded) byte
//! count the poster declared.

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
/// no segments rather than failing the whole document.
pub fn parse_nzb(xml: &str) -> Result<Vec<NzbFile>, NzbError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

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
                    _ => {}
                }
            }
            Event::CData(c) => {
                let text = String::from_utf8_lossy(c.as_ref()).into_owned();
                if matches!(text_target.take(), Some("segment")) {
                    if let Some(seg) = cur_segment.as_mut() {
                        seg.message_id = text.trim().trim_matches(|c| c == '<' || c == '>').to_string();
                    }
                }
            }
            Event::End(e) => match e.name().as_ref() {
                b"segment" => {
                    if let (Some(file), Some(seg)) = (cur_file.as_mut(), cur_segment.take()) {
                        if !seg.message_id.is_empty() {
                            file.segments.push(seg);
                        }
                    }
                    text_target = None;
                }
                b"group" => {
                    in_group = false;
                    text_target = None;
                }
                b"file" => {
                    if let Some(mut file) = cur_file.take() {
                        // Sort segments by `number` so cumulative offsets are correct.
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
    Ok(files)
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
}
