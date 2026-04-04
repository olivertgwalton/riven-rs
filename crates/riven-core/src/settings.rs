use figment::{
    providers::{Env, Serialized},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

/// Core application settings, loaded from environment variables.
/// Prefix: RIVEN_SETTING__
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RivenSettings {
    pub database_url: String,
    pub redis_url: String,
    pub vfs_mount_path: String,
    pub vfs_debug_logging: bool,
    pub unsafe_clear_queues_on_startup: bool,
    pub unsafe_refresh_database_on_startup: bool,
    pub log_level: String,
    pub log_directory: String,
    pub logging_enabled: bool,
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
            vfs_debug_logging: false,
            unsafe_clear_queues_on_startup: false,
            unsafe_refresh_database_on_startup: false,
            log_level: "info".into(),
            log_directory: "./logs".into(),
            logging_enabled: true,
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
        let settings: Self = Figment::new()
            .merge(Serialized::defaults(Self::default()))
            .merge(Env::prefixed("RIVEN_SETTING__").split("__"))
            .extract()?;
        Ok(settings)
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
