//! Shared NZB identity helpers and Newznab RSS parsing, used by every plugin
//! that scrapes or downloads via Newznab-compatible indexers (newznab,
//! stremthru, usenet).

use sha1::{Digest, Sha1};

pub const NZB_INFO_HASH_PREFIX: &str = "nzb-";
pub const NZB_URL_TTL_SECS: u64 = 60 * 60 * 24 * 7;

/// Build the synthetic info_hash used to identify an NZB throughout the rest
/// of the pipeline. The pipeline treats `info_hash` as opaque, so a stable
/// SHA-1 of the NZB URL is enough to dedupe and round-trip.
pub fn nzb_info_hash(nzb_url: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(nzb_url.as_bytes());
    format!("{NZB_INFO_HASH_PREFIX}{}", hex::encode(hasher.finalize()))
}

pub fn is_nzb_info_hash(info_hash: &str) -> bool {
    info_hash.starts_with(NZB_INFO_HASH_PREFIX)
}

pub fn nzb_url_redis_key(info_hash: &str) -> String {
    format!("riven:nzb:url:{info_hash}")
}

#[derive(Debug, Default)]
pub struct NewznabItem {
    pub title: String,
    pub nzb_url: String,
    pub size: Option<u64>,
}

/// Hand-rolled lightweight parser for Newznab RSS. Avoids adding a heavy XML
/// dependency just to pluck three fields per `<item>`.
pub fn parse_newznab_xml(body: &str) -> Vec<NewznabItem> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);

    let mut items = Vec::new();
    let mut current: Option<NewznabItem> = None;
    let mut text_target: Option<&'static str> = None;

    loop {
        match reader.read_event() {
            Err(_) | Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = e.name();
                let local = name.as_ref();
                match local {
                    b"item" => current = Some(NewznabItem::default()),
                    b"title" if current.is_some() => text_target = Some("title"),
                    _ => {}
                }
            }
            Ok(Event::Empty(e)) => {
                let name = e.name();
                let local = name.as_ref();
                let Some(item) = current.as_mut() else { continue };
                match local {
                    b"enclosure" => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"url"
                                && let Ok(v) = attr.unescape_value()
                            {
                                item.nzb_url = v.into_owned();
                            }
                        }
                    }
                    // Some indexers emit <link> as the NZB url; only adopt
                    // when we don't already have one from <enclosure>.
                    b"link" if item.nzb_url.is_empty() => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"href"
                                && let Ok(v) = attr.unescape_value()
                            {
                                item.nzb_url = v.into_owned();
                            }
                        }
                    }
                    b"newznab:attr" | b"torznab:attr" => {
                        let mut name_val: Option<String> = None;
                        let mut value_val: Option<String> = None;
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"name" => {
                                    name_val =
                                        attr.unescape_value().ok().map(std::borrow::Cow::into_owned);
                                }
                                b"value" => {
                                    value_val =
                                        attr.unescape_value().ok().map(std::borrow::Cow::into_owned);
                                }
                                _ => {}
                            }
                        }
                        if let (Some(n), Some(v)) = (name_val, value_val)
                            && n.eq_ignore_ascii_case("size")
                        {
                            item.size = v.parse().ok();
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(t)) => {
                if let (Some(target), Some(item)) = (text_target.take(), current.as_mut()) {
                    let text = String::from_utf8_lossy(t.as_ref()).into_owned();
                    if target == "title" && item.title.is_empty() {
                        item.title = text;
                    }
                }
            }
            Ok(Event::CData(t)) => {
                if let (Some(target), Some(item)) = (text_target.take(), current.as_mut()) {
                    let text = String::from_utf8_lossy(t.as_ref()).into_owned();
                    if target == "title" && item.title.is_empty() {
                        item.title = text;
                    }
                }
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                if name.as_ref() == b"item"
                    && let Some(item) = current.take()
                {
                    items.push(item);
                }
                text_target = None;
            }
            _ => {}
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_rss() {
        let body = r#"<?xml version="1.0"?>
        <rss><channel>
        <item>
          <title>Example.Movie.2024.1080p.WEB.x264</title>
          <enclosure url="https://idx.example/get/abc.nzb" length="1234" type="application/x-nzb"/>
          <newznab:attr name="size" value="2147483648"/>
        </item>
        </channel></rss>"#;
        let items = parse_newznab_xml(body);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "Example.Movie.2024.1080p.WEB.x264");
        assert_eq!(items[0].nzb_url, "https://idx.example/get/abc.nzb");
        assert_eq!(items[0].size, Some(2147483648));
    }

    #[test]
    fn info_hash_is_stable() {
        let a = nzb_info_hash("https://example/x.nzb");
        let b = nzb_info_hash("https://example/x.nzb");
        assert_eq!(a, b);
        assert!(is_nzb_info_hash(&a));
    }
}
