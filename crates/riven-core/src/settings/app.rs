use figment::{
    Figment,
    providers::{Env, Serialized},
};
use serde::{Deserialize, Serialize};

use super::FilesystemSettings;

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
    /// this many seconds. 0 = disabled. Default: 600 (10 m).
    pub retry_interval_secs: u64,
    /// Minutes to wait after a known release/air date before re-indexing.
    pub schedule_offset_minutes: u64,
    /// Fallback delay when an unreleased/ongoing item has no known future air date.
    pub unknown_air_date_offset_days: u64,

    /// Bearer token / API key required on the GraphQL endpoint.
    /// Empty string means no authentication is enforced.
    pub api_key: String,
    /// Shared secret used to verify frontend-signed auth claims.
    pub frontend_auth_signing_secret: String,

    /// VFS in-memory chunk cache capacity in MB. 0 = use default (1 024 MB).
    pub vfs_cache_max_size_mb: u64,

    /// Comma-separated list of allowed CORS origins for the API.
    /// Empty = fall back to permissive CORS (logs a warning on startup).
    pub cors_allowed_origins: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct GeneralSettingsOverride {
    filesystem: Option<FilesystemSettings>,
    dubbed_anime_only: Option<bool>,
    minimum_average_bitrate_movies: Option<u32>,
    minimum_average_bitrate_episodes: Option<u32>,
    maximum_average_bitrate_movies: Option<u32>,
    maximum_average_bitrate_episodes: Option<u32>,
    retry_interval_secs: Option<u64>,
    schedule_offset_minutes: Option<u64>,
    unknown_air_date_offset_days: Option<u64>,
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
            retry_interval_secs: 60 * 10, // 10 minutes
            schedule_offset_minutes: 30,
            unknown_air_date_offset_days: 7,
            api_key: String::new(),
            frontend_auth_signing_secret: String::new(),
            vfs_cache_max_size_mb: 0,
            cors_allowed_origins: String::new(),
        }
    }
}

impl RivenSettings {
    pub fn load() -> anyhow::Result<Self> {
        let mut settings: Self = Figment::new()
            .merge(Serialized::defaults(Self::default()))
            .merge(Env::prefixed("RIVEN_SETTING__").split("__"))
            .extract()?;
        settings.fill_legacy_filesystem_mount_path();
        Ok(settings)
    }

    pub fn effective_vfs_mount_path(&self) -> &str {
        if self.filesystem.mount_path.is_empty() {
            &self.vfs_mount_path
        } else {
            &self.filesystem.mount_path
        }
    }

    pub fn apply_general_db_override(&mut self, value: &serde_json::Value) {
        let Ok(override_settings) =
            serde_json::from_value::<GeneralSettingsOverride>(value.clone())
        else {
            return;
        };

        if let Some(filesystem) = override_settings.filesystem {
            self.filesystem = filesystem;
            self.fill_legacy_filesystem_mount_path();
        }

        set_if_some(
            &mut self.dubbed_anime_only,
            override_settings.dubbed_anime_only,
        );
        set_option_if_some(
            &mut self.minimum_average_bitrate_movies,
            override_settings.minimum_average_bitrate_movies,
        );
        set_option_if_some(
            &mut self.minimum_average_bitrate_episodes,
            override_settings.minimum_average_bitrate_episodes,
        );
        set_option_if_some(
            &mut self.maximum_average_bitrate_movies,
            override_settings.maximum_average_bitrate_movies,
        );
        set_option_if_some(
            &mut self.maximum_average_bitrate_episodes,
            override_settings.maximum_average_bitrate_episodes,
        );
        set_if_some(
            &mut self.retry_interval_secs,
            override_settings.retry_interval_secs,
        );
        set_if_some(
            &mut self.schedule_offset_minutes,
            override_settings.schedule_offset_minutes,
        );
        set_if_some(
            &mut self.unknown_air_date_offset_days,
            override_settings.unknown_air_date_offset_days,
        );
    }

    fn fill_legacy_filesystem_mount_path(&mut self) {
        if self.filesystem.mount_path.is_empty() {
            self.filesystem.mount_path = self.vfs_mount_path.clone();
        }
    }
}

fn set_if_some<T>(slot: &mut T, value: Option<T>) {
    if let Some(value) = value {
        *slot = value;
    }
}

fn set_option_if_some<T>(slot: &mut Option<T>, value: Option<T>) {
    if let Some(value) = value {
        *slot = Some(value);
    }
}
