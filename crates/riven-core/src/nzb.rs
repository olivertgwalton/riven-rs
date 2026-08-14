//! Shared NZB identity helpers and Newznab RSS parsing, used by every plugin
//! that scrapes or downloads via Newznab-compatible indexers (newznab,
//! stremthru, usenet).

use sha1::{Digest, Sha1};

use crate::events::ScrapeRequest;
use crate::types::MediaItemType;

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

/// Container-local staging area for manually-uploaded `.nzb` files (Manual
/// Scrape's "upload an NZB" entry point). Deliberately `/tmp` rather than a
/// persisted volume: an upload lives only from the moment it's saved to the
/// moment `plugin-usenet` ingests it (or the hourly sweep in
/// [`sweep_stale_nzb_uploads`] catches whatever never gets that far) — never
/// across a restart, and never anything worth surviving one.
pub const NZB_UPLOAD_DIR: &str = "/tmp/riven-nzb-uploads";

/// Path prefix the upload-serving route is mounted at in `riven-api`. Shared
/// here (rather than `riven-api` reaching into `plugin-usenet`, or vice versa)
/// so the plugin can recognise "this NZB URL is one of our own temp uploads"
/// — and clean the file up once ingest no longer needs it — without either
/// crate depending on the other.
pub const NZB_UPLOAD_ROUTE_PREFIX: &str = "/internal/nzb-uploads/";

/// `true` only for the exact shape this crate's own upload route ever writes:
/// a v4 UUID plus `.nzb`, nothing else. This is the one check standing between
/// a filename and `Path::join`, so it is deliberately stricter than "contains
/// no `/`" — a bare `..` contains no `/` either, and would otherwise walk the
/// join straight out of [`NZB_UPLOAD_DIR`].
fn is_valid_upload_filename(name: &str) -> bool {
    name.strip_suffix(".nzb")
        .is_some_and(|uuid_part| uuid::Uuid::parse_str(uuid_part).is_ok())
}

/// Save an uploaded NZB's bytes under a freshly-random name and return the
/// loopback URL `plugin-usenet`'s own HTTP fetch will resolve it at later.
/// Loopback, not the instance's public URL: today only `plugin-usenet`'s
/// direct-NNTP path ever fetches an NZB URL itself — `plugin-stremthru`'s newz
/// relay submits the URL to StremThru's own server, which would need it to be
/// externally reachable instead. If a newz-capable StremThru store is ever
/// wired up on this instance, this needs to switch to the public URL.
pub async fn store_nzb_upload(bytes: &[u8], gql_port: u16) -> std::io::Result<String> {
    tokio::fs::create_dir_all(NZB_UPLOAD_DIR).await?;
    let filename = format!("{}.nzb", uuid::Uuid::new_v4());
    let path = std::path::Path::new(NZB_UPLOAD_DIR).join(&filename);
    tokio::fs::write(&path, bytes).await?;
    Ok(format!(
        "http://127.0.0.1:{gql_port}{NZB_UPLOAD_ROUTE_PREFIX}{filename}"
    ))
}

/// If `url` points at one of this crate's own temp uploads, the filename it
/// was stored under (validated — see [`is_valid_upload_filename`]). `None` for
/// any real external NZB URL — a `downloadExplicitNzb`/`previewManualNzb`
/// caller controls this string directly, so it is untrusted input right up to
/// this check.
///
/// Parses `url` properly (scheme + host, not a raw substring match) rather
/// than just checking the path contains [`NZB_UPLOAD_ROUTE_PREFIX`] somewhere:
/// a plain `url.contains(...)` check would also accept
/// `https://evil.example/internal/nzb-uploads/<uuid>.nzb` as "one of ours",
/// which — since this function's whole purpose is deciding what's safe to
/// delete on disk after a fetch succeeds — would let an attacker-controlled
/// external host trigger a local file deletion by shaping their response to
/// pass whatever came next. Requiring `http://127.0.0.1:<any port>/...`
/// specifically (the exact shape [`store_nzb_upload`] produces) closes that.
pub fn uploaded_nzb_filename(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    if parsed.scheme() != "http" || parsed.host_str() != Some("127.0.0.1") {
        return None;
    }
    let tail = parsed.path().strip_prefix(NZB_UPLOAD_ROUTE_PREFIX)?;
    is_valid_upload_filename(tail).then(|| tail.to_string())
}

/// Best-effort delete of an ingested upload's temp file. Re-validates
/// `filename` itself rather than trusting the caller to have already checked
/// it via [`uploaded_nzb_filename`] — the one path-safety check that matters
/// here, so it stays enforced at the point of the actual file I/O rather than
/// only at whichever call site happens to remember it. Never surfaces an
/// error: a cleanup miss just means [`sweep_stale_nzb_uploads`] catches it on
/// the next hourly tick.
pub async fn delete_nzb_upload(filename: &str) {
    if !is_valid_upload_filename(filename) {
        return;
    }
    let path = std::path::Path::new(NZB_UPLOAD_DIR).join(filename);
    if let Err(error) = tokio::fs::remove_file(&path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::debug!(%error, filename, "failed to delete temp NZB upload");
    }
}

/// Delete every file in [`NZB_UPLOAD_DIR`] older than `max_age`. Runs on the
/// queue Scheduler's hourly tick as a backstop for uploads whose ingest never
/// reached the point that deletes them immediately — a bad upload, a crash
/// mid-flow, or a manual scrape the user never followed through on. Returns
/// the count removed, purely for logging.
pub async fn sweep_stale_nzb_uploads(max_age: std::time::Duration) -> usize {
    let Ok(mut entries) = tokio::fs::read_dir(NZB_UPLOAD_DIR).await else {
        return 0;
    };
    let mut removed = 0usize;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let is_stale = entry
            .metadata()
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age > max_age);
        if is_stale && tokio::fs::remove_file(entry.path()).await.is_ok() {
            removed += 1;
        }
    }
    removed
}

/// Which indexer a release came from, written next to its URL at scrape time.
/// The download path is where a grab becomes real, and by then the scrape
/// results are long gone — this is the only handle back to the indexer that
/// earned the grab. Same TTL as the URL: without the URL there is no grab to
/// attribute anyway.
pub fn nzb_indexer_redis_key(info_hash: &str) -> String {
    format!("riven:nzb:indexer:{info_hash}")
}

/// Seasons numbered as calendar years (Formula 1 "Season 2020", daily shows).
/// Indexers don't parse these releases into season/ep attributes, so a
/// `season=2020` filter matches nothing server-side.
fn is_year_season(season: i32) -> bool {
    season >= 1900
}

/// Reduce a display title to a Newznab-friendly `q` term: apostrophes are
/// dropped (releases write "Marvels", not "Marvel's"), all other punctuation
/// becomes a space, and whitespace is collapsed.
fn sanitize_query_title(title: &str) -> String {
    let mut out = String::with_capacity(title.len());
    for c in title.chars() {
        if c == '\'' || c == '\u{2019}' {
            continue;
        }
        if c.is_alphanumeric() {
            out.push(c);
        } else if !out.ends_with(' ') && !out.is_empty() {
            out.push(' ');
        }
    }
    out.trim_end().to_string()
}

/// Build the text (`q=`) search for a scrape request, as a fallback for when
/// an ID-based search returns nothing. Sports and other non-episodic content
/// frequently has no tvdbid/imdbid mapping on indexers and is only reachable
/// by title — e.g. drunkenslug returns zero F1 items for any ID query but
/// finds the season packs via `q=Formula 1 2020`.
///
/// Returns `(search_type, params)` with the same shape as the ID builders.
/// For year-style seasons the year goes into the `q` term itself and the
/// `season`/`ep` filters are omitted entirely: indexers can't tie those
/// releases to season/ep attributes, so the filters only subtract results.
pub fn newznab_text_query(req: &ScrapeRequest<'_>) -> (&'static str, Vec<(&'static str, String)>) {
    let q = sanitize_query_title(req.title);
    match req.item_type {
        MediaItemType::Movie => ("movie", vec![("q", q)]),
        MediaItemType::Show => ("tvsearch", vec![("q", q)]),
        MediaItemType::Season | MediaItemType::Episode => {
            let season = req.season_or_1();
            if is_year_season(season) {
                ("tvsearch", vec![("q", format!("{q} {season}"))])
            } else {
                let mut params = vec![("q", q), ("season", season.to_string())];
                if req.item_type == MediaItemType::Episode {
                    params.push(("ep", req.episode_or_1().to_string()));
                }
                ("tvsearch", params)
            }
        }
    }
}

/// What one indexer says it can be asked, from its `t=caps` document.
///
/// Newznab's `<searching>` block advertises, per search mode, whether the mode
/// exists and which parameters it honours. Sending a parameter an indexer does
/// not advertise is not an error it reports — it silently ignores the
/// constraint and answers the *unconstrained* query, which is how a `tvdbid`
/// search for one show comes back full of another. Asking first is the only way
/// to tell "this indexer has nothing for the show" apart from "this indexer
/// ignored the part of the query that named the show".
#[derive(Debug, Default, Clone)]
pub struct NewznabCaps {
    /// Caps element name (`search`, `tv-search`, `movie-search`, ...) to the
    /// parameters it lists in `supportedParams`. Absent key means the mode is
    /// unavailable.
    modes: std::collections::HashMap<String, std::collections::HashSet<String>>,
}

impl NewznabCaps {
    /// Caps element name for a `t=` search value.
    fn element_for(search_type: &str) -> &'static str {
        match search_type {
            "tvsearch" => "tv-search",
            "movie" | "moviesearch" => "movie-search",
            "music" => "audio-search",
            "book" => "book-search",
            _ => "search",
        }
    }

    pub fn is_empty(&self) -> bool {
        self.modes.is_empty()
    }

    /// Whether the indexer advertises this `t=` mode at all. An indexer that
    /// served no usable caps document reports everything available, so a
    /// caps fetch that fails degrades to asking anyway rather than to silence.
    pub fn supports_search(&self, search_type: &str) -> bool {
        self.is_empty() || self.modes.contains_key(Self::element_for(search_type))
    }

    /// Whether this mode honours `param`. Unknown caps means yes, for the same
    /// reason as above.
    pub fn supports_param(&self, search_type: &str, param: &str) -> bool {
        if self.is_empty() {
            return true;
        }
        match self.modes.get(Self::element_for(search_type)) {
            // An advertised mode that lists no params at all tells us nothing
            // useful; treat it as unconstrained rather than unusable.
            Some(params) => params.is_empty() || params.contains(param),
            None => false,
        }
    }
}

/// Parse the `<searching>` block of a Newznab `t=caps` document.
///
/// Anything unparseable yields empty caps, which every `NewznabCaps` query
/// reads as "no constraints known" — a malformed caps document must never take
/// an indexer out of rotation.
pub fn parse_newznab_caps(body: &str) -> NewznabCaps {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);

    let mut caps = NewznabCaps::default();
    let mut in_searching = false;

    while let Ok(event) = reader.read_event() {
        match event {
            Event::Eof => break,
            Event::Start(e) if e.name().as_ref() == b"searching" => in_searching = true,
            Event::End(e) if e.name().as_ref() == b"searching" => in_searching = false,
            // Modes are usually empty elements, but a self-closing tag written
            // as a start/end pair parses as `Start` — accept both.
            Event::Empty(e) | Event::Start(e) if in_searching => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let mut available = true;
                let mut params = std::collections::HashSet::new();
                for attr in e.attributes().flatten() {
                    let Ok(value) = attr.normalized_value(quick_xml::XmlVersion::Implicit1_0)
                    else {
                        continue;
                    };
                    match attr.key.as_ref() {
                        b"available" => available = !value.eq_ignore_ascii_case("no"),
                        b"supportedParams" => {
                            params.extend(
                                value
                                    .split(',')
                                    .map(|p| p.trim().to_ascii_lowercase())
                                    .filter(|p| !p.is_empty()),
                            );
                        }
                        _ => {}
                    }
                }
                if available {
                    caps.modes.insert(name, params);
                }
            }
            _ => {}
        }
    }
    caps
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
                let Some(item) = current.as_mut() else {
                    continue;
                };
                match local {
                    b"enclosure" => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"url"
                                && let Ok(v) =
                                    attr.normalized_value(quick_xml::XmlVersion::Implicit1_0)
                            {
                                item.nzb_url = v.into_owned();
                            }
                        }
                    }
                    b"link" if item.nzb_url.is_empty() => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"href"
                                && let Ok(v) =
                                    attr.normalized_value(quick_xml::XmlVersion::Implicit1_0)
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
                                    name_val = attr
                                        .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                        .ok()
                                        .map(std::borrow::Cow::into_owned);
                                }
                                b"value" => {
                                    value_val = attr
                                        .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                        .ok()
                                        .map(std::borrow::Cow::into_owned);
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

    const CAPS: &str = r#"<?xml version="1.0"?>
    <caps>
      <searching>
        <search available="yes" supportedParams="q,cat"/>
        <tv-search available="yes" supportedParams="q,rid,tvdbid,season,ep"/>
        <movie-search available="no" supportedParams="q,imdbid"/>
      </searching>
    </caps>"#;

    #[test]
    fn caps_report_advertised_modes_and_params() {
        let caps = parse_newznab_caps(CAPS);
        assert!(caps.supports_search("tvsearch"));
        assert!(caps.supports_param("tvsearch", "tvdbid"));
        assert!(caps.supports_param("tvsearch", "season"));
        // Listed under tv-search's params but not offered by this indexer.
        assert!(!caps.supports_param("tvsearch", "imdbid"));
        assert!(caps.supports_search("search"));
        assert!(!caps.supports_param("search", "tvdbid"));
    }

    /// `available="no"` is the case that matters: the mode is described in the
    /// document but must not be used.
    #[test]
    fn caps_respect_an_unavailable_mode() {
        let caps = parse_newznab_caps(CAPS);
        assert!(!caps.supports_search("movie"));
        assert!(!caps.supports_param("movie", "imdbid"));
    }

    /// An indexer that serves no usable caps must keep being searched exactly
    /// as before, not fall silent.
    #[test]
    fn unparseable_caps_constrain_nothing() {
        for body in ["", "not xml at all", "<caps><searching/></caps>"] {
            let caps = parse_newznab_caps(body);
            assert!(caps.is_empty(), "{body:?}");
            assert!(caps.supports_search("tvsearch"), "{body:?}");
            assert!(caps.supports_param("tvsearch", "tvdbid"), "{body:?}");
            assert!(caps.supports_param("movie", "imdbid"), "{body:?}");
        }
    }

    /// A mode advertised without a `supportedParams` list says nothing about
    /// parameters, so it must not be read as "supports none of them".
    #[test]
    fn caps_without_a_param_list_are_unconstrained() {
        let caps = parse_newznab_caps(
            r#"<caps><searching><tv-search available="yes"/></searching></caps>"#,
        );
        assert!(!caps.is_empty());
        assert!(caps.supports_param("tvsearch", "tvdbid"));
        assert!(!caps.supports_search("movie"));
    }

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

    fn request(
        item_type: MediaItemType,
        title: &'static str,
        season: Option<i32>,
        episode: Option<i32>,
    ) -> ScrapeRequest<'static> {
        ScrapeRequest {
            id: 1,
            item_type,
            imdb_id: None,
            tvdb_id: None,
            title,
            season,
            episode,
        }
    }

    #[test]
    fn text_query_uses_season_and_ep_filters_for_normal_seasons() {
        let req = request(MediaItemType::Episode, "Severance", Some(2), Some(3));
        let (t, params) = newznab_text_query(&req);
        assert_eq!(t, "tvsearch");
        assert_eq!(
            params,
            vec![
                ("q", "Severance".to_string()),
                ("season", "2".to_string()),
                ("ep", "3".to_string()),
            ]
        );
    }

    #[test]
    fn text_query_embeds_year_style_seasons_in_q() {
        let req = request(MediaItemType::Season, "Formula 1", Some(2020), None);
        let (t, params) = newznab_text_query(&req);
        assert_eq!(t, "tvsearch");
        assert_eq!(params, vec![("q", "Formula 1 2020".to_string())]);

        let req = request(MediaItemType::Episode, "Formula 1", Some(2020), Some(6));
        let (_, params) = newznab_text_query(&req);
        assert_eq!(params, vec![("q", "Formula 1 2020".to_string())]);
    }

    #[test]
    fn text_query_sanitizes_punctuation() {
        let req = request(
            MediaItemType::Show,
            "Marvel's Agents of S.H.I.E.L.D.",
            None,
            None,
        );
        let (_, params) = newznab_text_query(&req);
        assert_eq!(
            params,
            vec![("q", "Marvels Agents of S H I E L D".to_string())]
        );
    }

    #[test]
    fn info_hash_is_stable() {
        let a = nzb_info_hash("https://example/x.nzb");
        let b = nzb_info_hash("https://example/x.nzb");
        assert_eq!(a, b);
        assert!(is_nzb_info_hash(&a));
    }

    #[test]
    fn uploaded_nzb_filename_accepts_a_real_upload_url() {
        let filename = "550e8400-e29b-41d4-a716-446655440000.nzb";
        let url = format!("http://127.0.0.1:8082{NZB_UPLOAD_ROUTE_PREFIX}{filename}");
        assert_eq!(uploaded_nzb_filename(&url), Some(filename.to_string()));
    }

    #[test]
    fn uploaded_nzb_filename_rejects_a_real_external_nzb_url() {
        assert_eq!(
            uploaded_nzb_filename("https://indexer.example/get/abc123.nzb"),
            None
        );
    }

    /// The host, not just the path, must be the loopback upload address —
    /// otherwise an external server could shape its own URL/path to be
    /// mistaken for one of this instance's own temp uploads and have a local
    /// file deleted on its behalf after a successful fetch.
    #[test]
    fn uploaded_nzb_filename_rejects_an_external_host_on_the_upload_path() {
        let filename = "550e8400-e29b-41d4-a716-446655440000.nzb";
        for hostile in [
            format!("https://evil.example{NZB_UPLOAD_ROUTE_PREFIX}{filename}"),
            format!("http://evil.example:8080{NZB_UPLOAD_ROUTE_PREFIX}{filename}"),
            // Same path shape, wrong loopback literal (IPv6, or a hostname
            // that merely resolves to 127.0.0.1) — only the exact
            // `127.0.0.1` host string this crate itself writes is trusted.
            format!("http://localhost{NZB_UPLOAD_ROUTE_PREFIX}{filename}"),
            format!("http://[::1]{NZB_UPLOAD_ROUTE_PREFIX}{filename}"),
        ] {
            assert_eq!(uploaded_nzb_filename(&hostile), None, "{hostile:?}");
        }
    }

    /// The specific attack this whole check exists to stop: a caller-supplied
    /// `nzbUrl` (any `downloadExplicitNzb` caller controls this string
    /// directly) engineered so the tail after the prefix is `..` or similar.
    /// A weaker check — e.g. "no `/` in the tail" — would let `..` straight
    /// through, since it contains no `/` either, and joining it onto
    /// `NZB_UPLOAD_DIR` would walk out of that directory entirely. Requiring
    /// the tail to fully parse as `{uuid}.nzb` closes that regardless of what
    /// shape the traversal attempt takes.
    #[test]
    fn uploaded_nzb_filename_rejects_path_traversal() {
        for hostile in [
            format!("http://127.0.0.1{NZB_UPLOAD_ROUTE_PREFIX}.."),
            format!("http://127.0.0.1{NZB_UPLOAD_ROUTE_PREFIX}../../../etc/passwd"),
            format!("http://127.0.0.1{NZB_UPLOAD_ROUTE_PREFIX}not-a-uuid.nzb"),
            format!("http://127.0.0.1{NZB_UPLOAD_ROUTE_PREFIX}"),
            // Same prefix substring, but not actually a single path segment —
            // must not be mistaken for a real upload reference.
            format!(
                "http://evil.example/x?u=127.0.0.1{NZB_UPLOAD_ROUTE_PREFIX}550e8400-e29b-41d4-a716-446655440000.nzb/../../secrets"
            ),
        ] {
            assert_eq!(uploaded_nzb_filename(&hostile), None, "{hostile:?}");
        }
    }

    #[test]
    fn delete_nzb_upload_refuses_a_non_uuid_filename() {
        // No assertion beyond "doesn't panic and doesn't touch the
        // filesystem" is possible without a real NZB_UPLOAD_DIR fixture;
        // is_valid_upload_filename (exercised via uploaded_nzb_filename
        // above) is the actual guarantee this relies on.
        assert!(!is_valid_upload_filename(".."));
        assert!(!is_valid_upload_filename("../../etc/passwd"));
        assert!(!is_valid_upload_filename(
            "550e8400-e29b-41d4-a716-446655440000"
        ));
        assert!(is_valid_upload_filename(
            "550e8400-e29b-41d4-a716-446655440000.nzb"
        ));
    }
}
