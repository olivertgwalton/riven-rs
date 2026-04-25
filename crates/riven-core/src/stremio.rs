//! Stremio addon identifier helpers. Series requests are episode-addressed —
//! Show falls back to `:1:1`, Season to `:N:1`, since most addons don't expose
//! Show- or Season-level endpoints.

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

#[derive(Debug, Clone)]
pub struct StremioScrapeConfig<'a> {
    pub imdb_id: &'a str,
    pub kind: StremioKind,
    pub episode_id: Option<(i32, i32)>,
}

impl<'a> StremioScrapeConfig<'a> {
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

    /// `:S:E` for series, empty for movies — appended to the imdb id in URL paths.
    pub fn id_suffix(&self) -> String {
        match self.episode_id {
            Some((s, e)) => format!(":{s}:{e}"),
            None => String::new(),
        }
    }

    /// `imdb_id` for movies, `imdb_id:S:E` for series — colon-joined single token.
    pub fn full_id(&self) -> String {
        match self.episode_id {
            Some((s, e)) => format!("{}:{s}:{e}", self.imdb_id),
            None => self.imdb_id.to_string(),
        }
    }
}
