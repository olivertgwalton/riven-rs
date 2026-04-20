use async_graphql::*;
use plugin_logs::{LogControl, LogSettings};
use riven_core::downloader::DownloaderConfig;
use riven_core::plugin::PluginRegistry;
use riven_core::settings::{FilesystemSettings, LibraryProfileMembership};
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::repo;
use riven_queue::JobQueue;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;

use crate::schema::auth::require_settings_access;
use crate::vfs_mount::VfsMountManager;

// ── Helpers ──

fn coerce_json_bool(value: &serde_json::Value) -> Option<bool> {
    match value {
        serde_json::Value::Bool(enabled) => Some(*enabled),
        serde_json::Value::String(value) => match value.as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

pub(super) async fn rematch_filesystem_library_profiles_inner(
    pool: &PgPool,
    filesystem_settings: &FilesystemSettings,
) -> Result<i64> {
    let candidates = repo::list_filesystem_profile_entry_candidates(pool).await?;
    let updates = candidates
        .into_iter()
        .filter_map(|candidate| {
            let next = filesystem_settings.matching_profile_keys(
                &candidate.filesystem_metadata(),
                candidate.filesystem_content_type(),
            );
            let current = LibraryProfileMembership::from_json(candidate.library_profiles.as_ref());
            (next != current).then(|| (candidate.id, next.into_json()))
        })
        .collect::<Vec<_>>();

    Ok(repo::update_library_profiles_batch(pool, &updates).await? as i64)
}

// ── Resolver ──

#[derive(Default)]
pub struct SettingsMutations;

#[Object]
impl SettingsMutations {
    /// Save a custom profile. If `id` is provided the existing profile is
    /// updated; otherwise a new one is created. Built-in profiles cannot be
    /// modified through this mutation — use `setProfileEnabled` instead.
    async fn save_custom_profile(
        &self,
        ctx: &Context<'_>,
        id: Option<i32>,
        name: String,
        settings: serde_json::Value,
        enabled: Option<bool>,
    ) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let validated: riven_rank::RankSettings = serde_json::from_value(settings)
            .map_err(|e| Error::new(format!("invalid rank settings: {e}")))?;
        let canonical = serde_json::to_value(&validated)
            .map_err(|e| Error::new(format!("failed to serialise rank settings: {e}")))?;

        let pool = ctx.data::<PgPool>()?;
        let profile =
            repo::upsert_ranking_profile(pool, id, &name, canonical, enabled.unwrap_or(false))
                .await?;
        Ok(serde_json::to_value(profile)?)
    }

    /// Delete a custom ranking profile by ID. Built-in profiles cannot be deleted.
    async fn delete_custom_profile(&self, ctx: &Context<'_>, id: i32) -> Result<bool> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::delete_ranking_profile(pool, id).await?)
    }

    /// Enable or disable a ranking profile (built-in or custom) by name.
    /// Enabled profiles are used for multi-version scraping and downloading.
    async fn set_profile_enabled(
        &self,
        ctx: &Context<'_>,
        name: String,
        enabled: bool,
    ) -> Result<bool> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::set_profile_enabled(pool, &name, enabled).await?)
    }

    /// Update settings for any profile (built-in or custom) by name.
    /// For built-in profiles these are stored as overrides that get merged on
    /// top of the Rust defaults at load time.
    async fn update_profile_settings(
        &self,
        ctx: &Context<'_>,
        name: String,
        settings: serde_json::Value,
    ) -> Result<bool> {
        require_settings_access(ctx)?;
        let _validated: riven_rank::RankSettings = serde_json::from_value(settings.clone())
            .map_err(|e| Error::new(format!("invalid rank settings: {e}")))?;

        let pool = ctx.data::<PgPool>()?;
        Ok(repo::update_profile_settings(pool, &name, settings).await?)
    }

    /// Update rank settings. Deserialises into [`RankSettings`] (applying
    /// serde defaults for any missing fields), then re-serialises the
    /// canonical form — ensuring the Rust schema is the source of truth.
    async fn update_rank_settings(
        &self,
        ctx: &Context<'_>,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let validated: riven_rank::RankSettings = serde_json::from_value(settings)
            .map_err(|e| Error::new(format!("invalid rank settings: {e}")))?;
        let canonical = serde_json::to_value(&validated)
            .map_err(|e| Error::new(format!("failed to serialise rank settings: {e}")))?;

        let pool = ctx.data::<PgPool>()?;
        repo::set_setting(pool, "rank_settings", canonical.clone()).await?;
        Ok(canonical)
    }

    /// Update all settings. Accepts a JSON object of key/value pairs.
    async fn update_all_settings(
        &self,
        ctx: &Context<'_>,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::set_all_settings(pool, settings).await?)
    }

    /// Mark the instance-wide first-run setup flow as completed.
    async fn complete_initial_setup(&self, ctx: &Context<'_>) -> Result<bool> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        repo::set_setting(
            pool,
            "instance.setup_completed",
            serde_json::Value::Bool(true),
        )
        .await?;
        Ok(true)
    }

    /// Update settings for a specific plugin (stored under "plugin.{name}" key).
    /// Also re-validates the plugin with the new settings in-memory.
    /// Returns an object with the saved settings and the new valid status.
    async fn update_plugin_settings(
        &self,
        ctx: &Context<'_>,
        plugin: String,
        mut settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        let key = format!("plugin.{plugin}");
        let enabled = match settings
            .as_object_mut()
            .and_then(|obj| obj.remove("enabled"))
            .as_ref()
            .and_then(coerce_json_bool)
        {
            Some(enabled) => enabled,
            None => repo::get_plugin_enabled(pool, &plugin).await?,
        };

        repo::set_setting(pool, &key, settings.clone()).await?;
        repo::set_plugin_enabled(pool, &plugin, enabled).await?;

        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let valid = registry
            .revalidate_plugin(&plugin, enabled, &settings)
            .await;

        if plugin == "logs" {
            let log_control = ctx.data::<Arc<LogControl>>()?;
            let log_settings = LogSettings {
                enabled: enabled
                    && settings
                        .get("logging_enabled")
                        .and_then(coerce_json_bool)
                        .unwrap_or(true),
                level: settings
                    .get("log_level")
                    .and_then(|value| value.as_str())
                    .unwrap_or("info")
                    .to_string(),
                rotation: settings
                    .get("log_rotation")
                    .and_then(|value| value.as_str())
                    .unwrap_or("hourly")
                    .to_string(),
                max_files: settings
                    .get("log_max_files")
                    .and_then(|value| value.as_u64())
                    .map(|value| value as usize)
                    .filter(|value| *value > 0)
                    .unwrap_or(5),
                vfs_debug_logging: settings
                    .get("vfs_debug_logging")
                    .and_then(coerce_json_bool)
                    .unwrap_or(false),
            };

            log_control
                .apply(&log_settings)
                .map_err(|error| Error::new(error.to_string()))?;
        }

        let mut response_settings = settings;
        if let Some(obj) = response_settings.as_object_mut() {
            obj.insert("enabled".to_string(), serde_json::Value::Bool(enabled));
        }

        Ok(serde_json::json!({
            "settings": response_settings,
            "enabled": enabled,
            "valid": valid
        }))
    }

    /// Enable or disable a plugin without overwriting its saved settings.
    async fn set_plugin_enabled(
        &self,
        ctx: &Context<'_>,
        plugin: String,
        enabled: bool,
    ) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        let settings_key = format!("plugin.{plugin}");
        let settings = match repo::get_setting(pool, &settings_key).await? {
            Some(value @ serde_json::Value::Object(_)) => value,
            _ => serde_json::json!({}),
        };

        repo::set_plugin_enabled(pool, &plugin, enabled).await?;

        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let valid = registry
            .revalidate_plugin(&plugin, enabled, &settings)
            .await;

        if plugin == "logs" {
            let log_control = ctx.data::<Arc<LogControl>>()?;
            let log_settings = LogSettings {
                enabled,
                level: settings
                    .get("log_level")
                    .and_then(|value| value.as_str())
                    .unwrap_or("info")
                    .to_string(),
                rotation: settings
                    .get("log_rotation")
                    .and_then(|value| value.as_str())
                    .unwrap_or("hourly")
                    .to_string(),
                max_files: settings
                    .get("log_max_files")
                    .and_then(|value| value.as_u64())
                    .map(|value| value as usize)
                    .filter(|value| *value > 0)
                    .unwrap_or(5),
                vfs_debug_logging: settings
                    .get("vfs_debug_logging")
                    .and_then(coerce_json_bool)
                    .unwrap_or(false),
            };

            log_control
                .apply(&log_settings)
                .map_err(|error| Error::new(error.to_string()))?;
        }

        Ok(serde_json::json!({
            "enabled": enabled,
            "valid": valid
        }))
    }

    /// Update general (non-plugin) settings and apply them to the live runtime config.
    async fn update_general_settings(
        &self,
        ctx: &Context<'_>,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;
        repo::set_setting(pool, "general", settings.clone()).await?;

        let cfg = ctx.data::<Arc<RwLock<DownloaderConfig>>>()?;
        let mut cfg = cfg.write().await;
        let mbps = |key: &str| settings.get(key).and_then(|v| v.as_u64()).map(|v| v as u32);
        cfg.minimum_average_bitrate_movies = mbps("minimum_average_bitrate_movies");
        cfg.minimum_average_bitrate_episodes = mbps("minimum_average_bitrate_episodes");
        cfg.maximum_average_bitrate_movies = mbps("maximum_average_bitrate_movies");
        cfg.maximum_average_bitrate_episodes = mbps("maximum_average_bitrate_episodes");

        let queue = ctx.data::<Arc<JobQueue>>()?;
        let mut reindex_cfg = queue.reindex_config.write().await;
        reindex_cfg.schedule_offset_minutes = settings
            .get("schedule_offset_minutes")
            .and_then(|v| v.as_u64())
            .unwrap_or(reindex_cfg.schedule_offset_minutes);
        reindex_cfg.unknown_air_date_offset_days = settings
            .get("unknown_air_date_offset_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(reindex_cfg.unknown_air_date_offset_days);
        queue.retry_interval_secs.store(
            settings
                .get("retry_interval_secs")
                .and_then(|v| v.as_u64())
                .unwrap_or(queue.retry_interval_secs.load(Ordering::SeqCst)),
            Ordering::SeqCst,
        );

        let previous_filesystem = queue.filesystem_settings.read().await.clone();
        let filesystem = settings
            .get("filesystem")
            .and_then(|v| serde_json::from_value::<FilesystemSettings>(v.clone()).ok())
            .unwrap_or_default();
        *queue.filesystem_settings.write().await = filesystem.clone();
        *queue.vfs_layout.write().await = VfsLibraryLayout::new(filesystem.clone());
        let mut rematch_count = 0_i64;
        if previous_filesystem != filesystem {
            rematch_count = rematch_filesystem_library_profiles_inner(pool, &filesystem).await?;
            queue
                .filesystem_settings_revision
                .fetch_add(1, Ordering::SeqCst);
            if previous_filesystem.mount_path != filesystem.mount_path {
                ctx.data::<Arc<VfsMountManager>>()?
                    .set_mount_path(&filesystem.mount_path)
                    .await?;
            }
            tracing::info!(
                rematch_count,
                "updated filesystem settings and rematched library profiles"
            );
        }

        Ok(serde_json::json!({
            "settings": settings,
            "filesystem_profile_rematch_count": rematch_count
        }))
    }

    /// Recompute stored library-profile matches for every existing media entry.
    async fn rematch_filesystem_library_profiles(&self, ctx: &Context<'_>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        let queue = ctx.data::<Arc<JobQueue>>()?;
        let filesystem_settings = queue.filesystem_settings.read().await.clone();
        let updated = rematch_filesystem_library_profiles_inner(pool, &filesystem_settings).await?;

        queue
            .filesystem_settings_revision
            .fetch_add(1, Ordering::SeqCst);

        Ok(updated)
    }
}
