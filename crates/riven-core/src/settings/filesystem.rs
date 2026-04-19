use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::ContentRating;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemContentType {
    Movie,
    Show,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct FilesystemFilterRules {
    pub content_types: Vec<FilesystemContentType>,
    pub genres: Vec<String>,
    pub networks: Vec<String>,
    pub languages: Vec<String>,
    pub countries: Vec<String>,
    pub content_ratings: Vec<String>,
    pub min_year: Option<i32>,
    pub max_year: Option<i32>,
    pub min_rating: Option<f64>,
    pub max_rating: Option<f64>,
    pub is_anime: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct FilesystemLibraryProfile {
    pub name: String,
    pub library_path: String,
    pub enabled: bool,
    /// When true, items matched by this profile are hidden from the default
    /// `/movies` and `/shows` paths and only appear under this profile's path.
    pub exclusive: bool,
    pub filter_rules: FilesystemFilterRules,
}

impl Default for FilesystemLibraryProfile {
    fn default() -> Self {
        Self {
            name: String::new(),
            library_path: String::new(),
            enabled: true,
            exclusive: false,
            filter_rules: FilesystemFilterRules::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct FilesystemSettings {
    pub mount_path: String,
    pub library_profiles: HashMap<String, FilesystemLibraryProfile>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct LibraryProfileMembership(pub Vec<String>);

impl LibraryProfileMembership {
    pub fn new<I>(keys: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let mut keys: Vec<String> = keys.into_iter().collect();
        keys.sort();
        keys.dedup();
        Self(keys)
    }

    pub fn contains(&self, profile_key: &str) -> bool {
        self.0.iter().any(|key| key == profile_key)
    }

    pub fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!([]))
    }

    pub fn from_json(value: Option<&serde_json::Value>) -> Self {
        value
            .cloned()
            .and_then(|value| serde_json::from_value::<Self>(value).ok())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FilesystemItemMetadata {
    pub genres: Vec<String>,
    pub network: Option<String>,
    pub content_rating: Option<ContentRating>,
    pub language: Option<String>,
    pub country: Option<String>,
    pub year: Option<i32>,
    pub rating: Option<f64>,
    pub is_anime: bool,
}

impl FilesystemSettings {
    pub fn matching_profile_keys(
        &self,
        metadata: &FilesystemItemMetadata,
        content_type: FilesystemContentType,
    ) -> LibraryProfileMembership {
        LibraryProfileMembership::new(
            self.library_profiles
                .iter()
                .filter(|(_, profile)| profile.enabled)
                .filter(|(_, profile)| profile.filter_rules.matches(metadata, content_type))
                .map(|(key, _)| key.clone()),
        )
    }
}

impl FilesystemFilterRules {
    pub fn matches(
        &self,
        metadata: &FilesystemItemMetadata,
        content_type: FilesystemContentType,
    ) -> bool {
        self.allows_content_type(content_type)
            && matches_token_filter(&metadata.genres, &self.genres)
            && matches_text_filter(metadata.network.as_deref(), &self.networks)
            && matches_text_filter(metadata.language.as_deref(), &self.languages)
            && matches_text_filter(metadata.country.as_deref(), &self.countries)
            && matches_content_rating_filter(metadata.content_rating, &self.content_ratings)
            && within_bounds(metadata.year, self.min_year, self.max_year)
            && within_bounds(metadata.rating, self.min_rating, self.max_rating)
            && self
                .is_anime
                .is_none_or(|required| metadata.is_anime == required)
    }

    fn allows_content_type(&self, content_type: FilesystemContentType) -> bool {
        self.content_types.is_empty() || self.content_types.contains(&content_type)
    }
}

fn matches_text_filter(value: Option<&str>, filters: &[String]) -> bool {
    let values = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| vec![value.to_ascii_lowercase()])
        .unwrap_or_default();
    matches_token_filter(&values, filters)
}

fn matches_content_rating_filter(rating: Option<ContentRating>, filters: &[String]) -> bool {
    let values = rating
        .map(content_rating_key)
        .map(|value| vec![value.to_string()])
        .unwrap_or_default();
    matches_token_filter(&values, filters)
}

fn matches_token_filter(values: &[String], filters: &[String]) -> bool {
    let mut inclusions = Vec::new();
    for filter in filters
        .iter()
        .map(|filter| filter.trim().to_ascii_lowercase())
    {
        if filter.is_empty() {
            continue;
        }
        if let Some(exclusion) = filter.strip_prefix('!') {
            if values.iter().any(|value| value == exclusion) {
                return false;
            }
        } else {
            inclusions.push(filter);
        }
    }

    inclusions.is_empty()
        || inclusions
            .iter()
            .any(|filter| values.iter().any(|value| value == filter))
}

fn within_bounds<T>(value: Option<T>, min: Option<T>, max: Option<T>) -> bool
where
    T: Copy + PartialOrd,
{
    min.is_none_or(|min| value.is_some_and(|value| value >= min))
        && max.is_none_or(|max| value.is_some_and(|value| value <= max))
}

fn content_rating_key(rating: ContentRating) -> &'static str {
    match rating {
        ContentRating::G => "g",
        ContentRating::Pg => "pg",
        ContentRating::Pg13 => "pg-13",
        ContentRating::R => "r",
        ContentRating::Nc17 => "nc-17",
        ContentRating::TvY => "tv-y",
        ContentRating::TvY7 => "tv-y7",
        ContentRating::TvG => "tv-g",
        ContentRating::TvPg => "tv-pg",
        ContentRating::Tv14 => "tv-14",
        ContentRating::TvMa => "tv-ma",
    }
}
