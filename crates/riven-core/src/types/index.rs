use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{ContentRating, ShowStatus};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexedMediaItem {
    pub title: Option<String>,
    pub full_title: Option<String>,
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
    pub poster_path: Option<String>,
    pub year: Option<i32>,
    pub genres: Option<Vec<String>>,
    pub country: Option<String>,
    pub language: Option<String>,
    pub network: Option<String>,
    pub content_rating: Option<ContentRating>,
    pub rating: Option<f64>,
    pub is_anime: Option<bool>,
    pub runtime: Option<i32>,
    pub aliases: Option<HashMap<String, Vec<String>>>,
    pub aired_at: Option<chrono::NaiveDate>,
    pub status: Option<ShowStatus>,
    pub seasons: Option<Vec<IndexedSeason>>,
    pub network_timezone: Option<String>,
}

impl IndexedMediaItem {
    /// Merge another `IndexedMediaItem` into this one. Fields from `other` take
    /// precedence when present (non-None), otherwise the existing value is kept.
    pub fn merge(self, other: Self) -> Self {
        Self {
            title: other.title.or(self.title),
            full_title: other.full_title.or(self.full_title),
            imdb_id: other.imdb_id.or(self.imdb_id),
            tvdb_id: other.tvdb_id.or(self.tvdb_id),
            tmdb_id: other.tmdb_id.or(self.tmdb_id),
            poster_path: other.poster_path.or(self.poster_path),
            year: other.year.or(self.year),
            genres: other.genres.or(self.genres),
            country: other.country.or(self.country),
            language: other.language.or(self.language),
            network: other.network.or(self.network),
            content_rating: other.content_rating.or(self.content_rating),
            rating: other.rating.or(self.rating),
            is_anime: merge_anime_flag(self.is_anime, other.is_anime),
            runtime: other.runtime.or(self.runtime),
            aliases: other.aliases.or(self.aliases),
            aired_at: other.aired_at.or(self.aired_at),
            status: other.status.or(self.status),
            seasons: other.seasons.or(self.seasons),
            network_timezone: other.network_timezone.or(self.network_timezone),
        }
    }

    pub fn inferred_is_anime(&self) -> bool {
        if let Some(is_anime) = self.is_anime {
            return is_anime;
        }

        let genres = self
            .genres
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|genre| genre.to_ascii_lowercase())
            .collect::<Vec<_>>();

        let language = self
            .language
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase();

        genres.iter().any(|genre| genre == "anime")
            || (genres.iter().any(|genre| genre == "animation")
                && !matches!(language.as_str(), "en" | "eng"))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexedSeason {
    pub number: i32,
    pub title: Option<String>,
    pub tvdb_id: Option<String>,
    pub episodes: Vec<IndexedEpisode>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexedEpisode {
    pub number: i32,
    pub absolute_number: Option<i32>,
    pub title: Option<String>,
    pub tvdb_id: Option<String>,
    pub aired_at: Option<chrono::NaiveDate>,
    pub aired_at_utc: Option<chrono::DateTime<chrono::Utc>>,
    pub runtime: Option<i32>,
    pub poster_path: Option<String>,
    pub content_rating: Option<ContentRating>,
}

fn merge_anime_flag(current: Option<bool>, incoming: Option<bool>) -> Option<bool> {
    match (current, incoming) {
        (Some(a), Some(b)) => Some(a || b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}
