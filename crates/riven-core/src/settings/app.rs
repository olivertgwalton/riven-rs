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
    /// Enable debug logging for the virtual file system.
    pub vfs_debug_logging: bool,
    pub filesystem: FilesystemSettings,
    /// **UNSAFE.** If true, all Redis data is removed on application startup.
    pub unsafe_wipe_redis_on_startup: bool,
    /// **UNSAFE.** If true, the database is wiped on application startup.
    pub unsafe_wipe_database_on_startup: bool,
    /// Master switch for application logging.
    pub logging_enabled: bool,
    /// Logging level (off, error, warn, info, debug, trace).
    pub log_level: String,
    /// Rolling-file rotation cadence ("hourly" or "daily").
    pub log_rotation: String,
    /// Maximum number of rotated log files to retain on disk.
    pub log_max_files: usize,
    pub log_directory: String,
    pub gql_port: u16,
    pub dubbed_anime_only: bool,
    /// When true, torrents with unknown cache status are included as download candidates.
    /// Defaults to false because attempting unknown torrents degrades performance.
    pub attempt_unknown_downloads: bool,
    /// Minimum average bitrate for movies (Mbps). `None` = disabled.
    pub minimum_average_bitrate_movies: Option<u32>,
    /// Minimum average bitrate for episodes (Mbps). `None` = disabled.
    pub minimum_average_bitrate_episodes: Option<u32>,
    /// Maximum average bitrate for movies (Mbps). `None` = disabled.
    pub maximum_average_bitrate_movies: Option<u32>,
    /// Maximum average bitrate for episodes (Mbps). `None` = disabled.
    pub maximum_average_bitrate_episodes: Option<u32>,

    /// Retry items that have been stuck (failed_attempts > 0) for longer than
    /// this many seconds. 0 = disabled. Default: 600 (10 m). Controls the
    /// global retry-library scan cadence.
    pub retry_interval_secs: u64,
    /// Hard ceiling on consecutive scrape/index failures before an item is
    /// transitioned to `Failed` and excluded from further retry cycles. `0`
    /// disables the cap (retry forever, subject only to the cooldown
    /// back-off).
    pub maximum_scrape_attempts: u32,
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
    attempt_unknown_downloads: Option<bool>,
    minimum_average_bitrate_movies: Option<u32>,
    minimum_average_bitrate_episodes: Option<u32>,
    maximum_average_bitrate_movies: Option<u32>,
    maximum_average_bitrate_episodes: Option<u32>,
    retry_interval_secs: Option<u64>,
    maximum_scrape_attempts: Option<u32>,
    schedule_offset_minutes: Option<u64>,
    unknown_air_date_offset_days: Option<u64>,
    logging_enabled: Option<bool>,
    log_level: Option<String>,
    log_rotation: Option<String>,
    log_max_files: Option<usize>,
    vfs_debug_logging: Option<bool>,
}

impl Default for RivenSettings {
    fn default() -> Self {
        Self {
            database_url: "postgresql://localhost/riven".into(),
            redis_url: "redis://localhost:6379".into(),
            vfs_mount_path: String::new(),
            vfs_debug_logging: false,
            filesystem: FilesystemSettings::default(),
            unsafe_wipe_redis_on_startup: false,
            unsafe_wipe_database_on_startup: false,
            logging_enabled: true,
            log_level: "info".into(),
            log_rotation: "hourly".into(),
            log_max_files: 5,
            log_directory: "./logs".into(),
            gql_port: 8080,
            dubbed_anime_only: false,
            attempt_unknown_downloads: false,
            minimum_average_bitrate_movies: None,
            minimum_average_bitrate_episodes: None,
            maximum_average_bitrate_movies: None,
            maximum_average_bitrate_episodes: None,
            retry_interval_secs: 60 * 10, // 10 minutes
            maximum_scrape_attempts: 0,
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
        set_if_some(
            &mut self.attempt_unknown_downloads,
            override_settings.attempt_unknown_downloads,
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
            &mut self.maximum_scrape_attempts,
            override_settings.maximum_scrape_attempts,
        );
        set_if_some(
            &mut self.schedule_offset_minutes,
            override_settings.schedule_offset_minutes,
        );
        set_if_some(
            &mut self.unknown_air_date_offset_days,
            override_settings.unknown_air_date_offset_days,
        );
        set_if_some(&mut self.logging_enabled, override_settings.logging_enabled);
        set_if_some(&mut self.log_level, override_settings.log_level);
        set_if_some(&mut self.log_rotation, override_settings.log_rotation);
        set_if_some(&mut self.log_max_files, override_settings.log_max_files);
        set_if_some(
            &mut self.vfs_debug_logging,
            override_settings.vfs_debug_logging,
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
