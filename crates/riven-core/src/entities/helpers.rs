//! Shared helpers for entity Model impls.

use crate::settings::FilesystemItemMetadata;
use crate::types::ContentRating;

pub fn build_filesystem_metadata(
    genres: Option<&serde_json::Value>,
    network: Option<String>,
    content_rating: Option<ContentRating>,
    language: Option<String>,
    country: Option<String>,
    year: Option<i32>,
    rating: Option<f64>,
    is_anime: bool,
) -> FilesystemItemMetadata {
    FilesystemItemMetadata {
        genres: lowercase_json_strings(genres),
        network,
        content_rating,
        language,
        country,
        year,
        rating,
        is_anime,
    }
}

fn lowercase_json_strings(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_ascii_lowercase)
        .collect()
}

/// Artwork hosts. `media_items.poster_path` holds whatever the indexer plugin
/// stored, and the two plugins disagree: TMDB writes an absolute URL, TVDB
/// writes the value it was given, which may be a bare path.
const TMDB_ARTWORK: &str = "https://image.tmdb.org/t/p";
const TVDB_ARTWORK: &str = "https://artworks.thetvdb.com";

/// What an image is for, which is what decides the size TMDB is asked for.
///
/// The one place these sizes are written down. TMDB resizes to any `wNNN` on
/// request, including upward — asking for `w1920` when the source is 1000px
/// wide returns an 833 KB upscale of a 260 KB image — so anything full-bleed
/// asks for `original` and takes the source as it is. TVDB serves one size per
/// asset and ignores this entirely.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Artwork {
    /// Grid and list posters.
    Poster,
    /// Full-bleed heroes and episode stills. TMDB's own backdrops are 1080p or
    /// better, so the source is what a hero wants.
    Backdrop,
    /// A person's photo on their own page.
    Portrait,
    /// Cast thumbnails.
    Profile,
    /// Title treatments drawn over a backdrop.
    Logo,
}

impl Artwork {
    const fn tmdb_size(self) -> &'static str {
        match self {
            Self::Poster | Self::Logo => "w500",
            Self::Backdrop => "original",
            Self::Portrait => "h632",
            Self::Profile => "w185",
        }
    }
}

/// TVDB serves artwork under these path roots; anything else bare is TMDB's.
const TVDB_ROOTS: [&str; 6] = [
    "/banners/",
    "/episodes/",
    "/series/",
    "/seasons/",
    "/movies/",
    "/people/",
];

/// Absolute URL for a path known to have come from TVDB.
///
/// Prefer this wherever the source is known. [`artwork_url`] has to guess from
/// the path shape because `media_items.poster_path` does not record which
/// indexer wrote it, and that guess is wrong for any TVDB path outside
/// [`TVDB_ROOTS`].
#[must_use]
pub fn tvdb_artwork_url(path: Option<&str>) -> Option<String> {
    let path = path.map(str::trim).filter(|path| !path.is_empty())?;
    if path.starts_with("http://") || path.starts_with("https://") {
        return Some(path.to_owned());
    }
    let separator = if path.starts_with('/') { "" } else { "/" };
    Some(format!("{TVDB_ARTWORK}{separator}{path}"))
}

/// Absolute URL for a stored artwork path of unknown origin.
///
/// Every client used to carry this rule — the web frontend, the Swift app and
/// the notification plugins each prefixed paths themselves, and each had to
/// guess which CDN a bare path belonged to. It is one rule, so it lives once.
/// Values that are already absolute are returned unchanged.
#[must_use]
pub fn artwork_url(path: Option<&str>, kind: Artwork) -> Option<String> {
    let path = path.map(str::trim).filter(|path| !path.is_empty())?;
    if path.starts_with("http://") || path.starts_with("https://") {
        return Some(path.to_owned());
    }
    if TVDB_ROOTS.iter().any(|root| path.starts_with(root)) {
        return Some(format!("{TVDB_ARTWORK}{path}"));
    }
    Some(format!("{TMDB_ARTWORK}/{}{path}", kind.tmdb_size()))
}

#[cfg(test)]
mod tests {
    use super::{Artwork, artwork_url, tvdb_artwork_url};

    #[test]
    fn an_absolute_url_is_left_alone() {
        assert_eq!(
            artwork_url(
                Some("https://image.tmdb.org/t/p/w500/a.jpg"),
                Artwork::Poster
            )
            .as_deref(),
            Some("https://image.tmdb.org/t/p/w500/a.jpg")
        );
    }

    #[test]
    fn a_tvdb_path_resolves_to_the_tvdb_cdn() {
        for path in [
            "/banners/posters/1-1.jpg",
            "/episodes/1/2.jpg",
            "/series/abc/posters/x.jpg",
        ] {
            assert_eq!(
                artwork_url(Some(path), Artwork::Poster),
                Some(format!("https://artworks.thetvdb.com{path}")),
                "{path}"
            );
        }
    }

    #[test]
    fn any_other_bare_path_resolves_to_tmdb() {
        assert_eq!(
            artwork_url(Some("/abc123.jpg"), Artwork::Poster).as_deref(),
            Some("https://image.tmdb.org/t/p/w500/abc123.jpg")
        );
    }

    #[test]
    fn nothing_in_nothing_out() {
        assert_eq!(artwork_url(None, Artwork::Poster), None);
        assert_eq!(artwork_url(Some(""), Artwork::Poster), None);
        assert_eq!(artwork_url(Some("   "), Artwork::Poster), None);
    }

    #[test]
    fn a_hero_takes_the_source_rather_than_an_upscale() {
        // TMDB will happily resize past the source; `original` is the source.
        assert_eq!(
            artwork_url(Some("/b.jpg"), Artwork::Backdrop).as_deref(),
            Some("https://image.tmdb.org/t/p/original/b.jpg")
        );
    }

    #[test]
    fn each_use_asks_for_its_own_size() {
        let url = |kind| artwork_url(Some("/x.jpg"), kind).unwrap();
        assert!(url(Artwork::Poster).contains("/w500/"));
        assert!(url(Artwork::Logo).contains("/w500/"));
        assert!(url(Artwork::Portrait).contains("/h632/"));
        assert!(url(Artwork::Profile).contains("/w185/"));
        assert!(url(Artwork::Backdrop).contains("/original/"));
    }

    #[test]
    fn a_tvdb_path_ignores_the_requested_size() {
        assert_eq!(
            artwork_url(Some("/banners/x.jpg"), Artwork::Backdrop).as_deref(),
            Some("https://artworks.thetvdb.com/banners/x.jpg")
        );
    }

    #[test]
    fn a_known_tvdb_path_never_needs_the_prefix_guess() {
        // The guess only covers TVDB's documented roots; this is why anything
        // with a known source should say so.
        assert_eq!(
            artwork_url(Some("/fallback.jpg"), Artwork::Poster).as_deref(),
            Some("https://image.tmdb.org/t/p/w500/fallback.jpg"),
            "an unrooted path reads as TMDB when the source is unknown"
        );
        assert_eq!(
            tvdb_artwork_url(Some("/fallback.jpg")).as_deref(),
            Some("https://artworks.thetvdb.com/fallback.jpg")
        );
        assert_eq!(
            tvdb_artwork_url(Some("banners/x.jpg")).as_deref(),
            Some("https://artworks.thetvdb.com/banners/x.jpg"),
            "a path without a leading slash still resolves"
        );
        assert_eq!(tvdb_artwork_url(None), None);
    }
}
