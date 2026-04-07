use figment::{
    providers::{Env, Serialized},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

use crate::types::ContentRating;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemContentType {
    Movie,
    Show,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FilesystemFilterRules {
    pub content_types: Vec<FilesystemContentType>,
    pub genres: Vec<String>,
    pub content_ratings: Vec<String>,
    pub is_anime: Option<bool>,
}

impl Default for FilesystemFilterRules {
    fn default() -> Self {
        Self {
            content_types: Vec::new(),
            genres: Vec::new(),
            content_ratings: Vec::new(),
            is_anime: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FilesystemLibraryProfile {
    pub name: String,
    pub library_path: String,
    pub enabled: bool,
    pub filter_rules: FilesystemFilterRules,
}

impl Default for FilesystemLibraryProfile {
    fn default() -> Self {
        Self {
            name: String::new(),
            library_path: String::new(),
            enabled: true,
            filter_rules: FilesystemFilterRules::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FilesystemSettings {
    pub mount_path: String,
    pub library_profiles: HashMap<String, FilesystemLibraryProfile>,
}

impl Default for FilesystemSettings {
    fn default() -> Self {
        Self {
            mount_path: String::new(),
            library_profiles: HashMap::new(),
        }
    }
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilesystemItemMetadata {
    pub genres: Vec<String>,
    pub content_rating: Option<ContentRating>,
    pub language: Option<String>,
    pub country: Option<String>,
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
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>(),
        )
    }
}

impl FilesystemFilterRules {
    pub fn matches(
        &self,
        metadata: &FilesystemItemMetadata,
        content_type: FilesystemContentType,
    ) -> bool {
        if !self.content_types.is_empty() && !self.content_types.contains(&content_type) {
            return false;
        }

        if !matches_token_filter(&metadata.genres, &self.genres) {
            return false;
        }

        let content_rating = metadata
            .content_rating
            .map(content_rating_key)
            .map(|value| vec![value])
            .unwrap_or_default();
        if !matches_token_filter(&content_rating, &self.content_ratings) {
            return false;
        }

        if let Some(required) = self.is_anime {
            if metadata.is_anime != required {
                return false;
            }
        }

        true
    }
}

fn matches_token_filter(values: &[String], filters: &[String]) -> bool {
    let mut inclusions = Vec::new();
    for filter in filters {
        let filter = filter.trim().to_ascii_lowercase();
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

fn content_rating_key(rating: ContentRating) -> String {
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
    .to_string()
}

/// Core application settings, loaded from environment variables.
/// Prefix: RIVEN_SETTING__
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RivenSettings {
    pub database_url: String,
    pub redis_url: String,
    pub vfs_mount_path: String,
    pub filesystem: FilesystemSettings,
    pub unsafe_clear_queues_on_startup: bool,
    pub unsafe_refresh_database_on_startup: bool,
    pub log_directory: String,
    pub gql_port: u16,
    pub dubbed_anime_only: bool,
    /// Minimum average bitrate for movies (Mbps). `None` = disabled.
    pub minimum_average_bitrate_movies: Option<u32>,
    /// Minimum average bitrate for episodes (Mbps). `None` = disabled.
    pub minimum_average_bitrate_episodes: Option<u32>,
    /// Maximum average bitrate for movies (Mbps). `None` = disabled.
    pub maximum_average_bitrate_movies: Option<u32>,
    /// Maximum average bitrate for episodes (Mbps). `None` = disabled.
    pub maximum_average_bitrate_episodes: Option<u32>,

    /// Retry items that have been stuck (failed_attempts > 0) for longer than
    /// this many seconds. 0 = disabled. Default: 86400 (24 h).
    pub retry_interval_secs: u64,
    /// Minutes to wait after a known release/air date before re-indexing.
    pub schedule_offset_minutes: u64,
    /// Fallback delay when an unreleased/ongoing item has no known future air date.
    pub unknown_air_date_offset_days: u64,

    /// Bearer token / API key required on the GraphQL endpoint.
    /// Empty string means no authentication is enforced.
    pub api_key: String,

    /// VFS in-memory chunk cache capacity in MB. 0 = use default (1 024 MB).
    pub vfs_cache_max_size_mb: u64,
}

impl Default for RivenSettings {
    fn default() -> Self {
        Self {
            database_url: "postgresql://localhost/riven".into(),
            redis_url: "redis://localhost:6379".into(),
            vfs_mount_path: String::new(),
            filesystem: FilesystemSettings::default(),
            unsafe_clear_queues_on_startup: false,
            unsafe_refresh_database_on_startup: false,
            log_directory: "./logs".into(),
            gql_port: 8080,
            dubbed_anime_only: false,
            minimum_average_bitrate_movies: None,
            minimum_average_bitrate_episodes: None,
            maximum_average_bitrate_movies: None,
            maximum_average_bitrate_episodes: None,
            retry_interval_secs: 86400,
            schedule_offset_minutes: 30,
            unknown_air_date_offset_days: 7,
            api_key: String::new(),
            vfs_cache_max_size_mb: 0,
        }
    }
}

impl RivenSettings {
    pub fn load() -> anyhow::Result<Self> {
        let mut settings: Self = Figment::new()
            .merge(Serialized::defaults(Self::default()))
            .merge(Env::prefixed("RIVEN_SETTING__").split("__"))
            .extract()?;
        if settings.filesystem.mount_path.is_empty() {
            settings.filesystem.mount_path = settings.vfs_mount_path.clone();
        }
        Ok(settings)
    }

    pub fn effective_vfs_mount_path(&self) -> &str {
        if self.filesystem.mount_path.is_empty() {
            &self.vfs_mount_path
        } else {
            &self.filesystem.mount_path
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_profiles_support_inclusion_and_exclusion_rules() {
        let mut library_profiles = HashMap::new();
        library_profiles.insert(
            "kids".to_string(),
            FilesystemLibraryProfile {
                name: "Kids".to_string(),
                library_path: "/kids".to_string(),
                enabled: true,
                filter_rules: FilesystemFilterRules {
                    content_types: vec![FilesystemContentType::Movie],
                    genres: vec!["animation".to_string(), "!horror".to_string()],
                    content_ratings: vec!["pg".to_string(), "!r".to_string()],
                    is_anime: None,
                },
            },
        );
        let settings = FilesystemSettings {
            mount_path: "/mount".to_string(),
            library_profiles,
        };
        let metadata = FilesystemItemMetadata {
            genres: vec!["animation".to_string(), "family".to_string()],
            content_rating: Some(ContentRating::Pg),
            language: None,
            country: None,
            is_anime: false,
        };

        assert_eq!(
            settings.matching_profile_keys(&metadata, FilesystemContentType::Movie),
            LibraryProfileMembership(vec!["kids".to_string()])
        );
    }
}

/// Per-plugin settings, loaded from environment variables.
/// Prefix: RIVEN_PLUGIN_SETTING__{PLUGIN_PREFIX}__{KEY}
#[derive(Debug, Clone)]
pub struct PluginSettings {
    prefix: String,
    values: HashMap<String, String>,
}

impl PluginSettings {
    pub fn load(prefix: &str) -> Self {
        let env_prefix = format!("RIVEN_PLUGIN_SETTING__{prefix}__");
        let mut values = HashMap::new();

        for (key, value) in std::env::vars() {
            if let Some(suffix) = key.strip_prefix(&env_prefix) {
                values.insert(suffix.to_lowercase(), value);
            }
        }

        Self {
            prefix: prefix.to_string(),
            values,
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.values
            .get(&key.to_lowercase())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
    }

    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or(default).to_string()
    }

    pub fn get_bool(&self, key: &str) -> bool {
        self.get(key)
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    }

    pub fn get_parsed<T>(&self, key: &str) -> Option<T>
    where
        T: FromStr,
    {
        self.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_parsed_or<T>(&self, key: &str, default: T) -> T
    where
        T: FromStr,
    {
        self.get_parsed(key).unwrap_or(default)
    }

    pub fn get_list(&self, key: &str) -> Vec<String> {
        self.get(key)
            .map(|v| {
                serde_json::from_str::<Vec<String>>(v)
                    .unwrap_or_else(|_| v.split(',').map(|s| s.trim().to_string()).collect())
            })
            .unwrap_or_default()
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn has(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Merge DB-stored settings (JSON object of string values) on top of env vars.
    /// DB values win for any key they provide.
    pub fn merge_db_override(&mut self, db_value: &serde_json::Value) {
        if let Some(obj) = db_value.as_object() {
            for (k, v) in obj {
                let val = match v {
                    serde_json::Value::String(s) if !s.is_empty() => s.clone(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Array(_) => v.to_string(),
                    _ => continue,
                };
                self.values.insert(k.to_lowercase(), val);
            }
        }
    }

    /// Serialize the active settings to a JSON object (string values).
    pub fn to_json(&self) -> serde_json::Value {
        let map: serde_json::Map<String, serde_json::Value> = self
            .values
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        serde_json::Value::Object(map)
    }
}
