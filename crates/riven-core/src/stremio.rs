//! Helpers for plugins that talk to Stremio-style addons (Comet, Torrentio, AIOStreams,
//! StremThru, ...).
//!
//! Stremio identifies content via `tt<imdb>:<season>:<episode>` for series streams and
//! plain `tt<imdb>` for movies. Series streams are addressed at the episode level —
//! Show / Season requests use sentinel values (`:1:1` / `:N:1`) because most Stremio
//! addons don't expose Show- or Season-level endpoints.
//!
//! Plugins with a different upstream convention (e.g. Comet's `:N` season-only
//! identifier) should not use this helper — they have their own format.

use crate::events::ScrapeRequest;
use crate::types::MediaItemType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StremioKind {
    Movie,
    Series,
}

impl StremioKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Movie => "movie",
            Self::Series => "series",
        }
    }
}

/// Stremio scrape config derived from a `ScrapeRequest`.
#[derive(Debug, Clone)]
pub struct StremioScrapeConfig<'a> {
    pub imdb_id: &'a str,
    pub kind: StremioKind,
    /// `(season, episode)` for series; `None` for movies.
    /// Defaults Show → `(1, 1)` and Season → `(season, 1)` so Stremio addons that
    /// only expose episode-level endpoints still receive a valid identifier.
    pub episode_id: Option<(i32, i32)>,
}

impl<'a> StremioScrapeConfig<'a> {
    /// Build from a `ScrapeRequest`. Returns `None` if the request has no IMDB id.
    pub fn from_request(req: &ScrapeRequest<'a>) -> Option<Self> {
        let imdb_id = req.imdb_id?;
        let (kind, episode_id) = match req.item_type {
            MediaItemType::Movie => (StremioKind::Movie, None),
            MediaItemType::Show => (StremioKind::Series, Some((1, 1))),
            MediaItemType::Season => (StremioKind::Series, Some((req.season_or_1(), 1))),
            MediaItemType::Episode => (
                StremioKind::Series,
                Some((req.season_or_1(), req.episode_or_1())),
            ),
        };
        Some(Self {
            imdb_id,
            kind,
            episode_id,
        })
    }

    /// `:S:E` suffix for series, empty string for movies. Used in URL path-style
    /// addons: `/stream/{kind}/{imdb_id}{suffix}.json`.
    pub fn id_suffix(&self) -> String {
        match self.episode_id {
            Some((s, e)) => format!(":{s}:{e}"),
            None => String::new(),
        }
    }

    /// Full identifier — `imdb_id` for movies, `imdb_id:S:E` for series. Used in
    /// addons that take the identifier as a single colon-joined value.
    pub fn full_id(&self) -> String {
        match self.episode_id {
            Some((s, e)) => format!("{}:{s}:{e}", self.imdb_id),
            None => self.imdb_id.to_string(),
        }
    }
}
