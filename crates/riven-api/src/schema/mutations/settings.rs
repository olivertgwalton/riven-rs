use async_graphql::*;
use riven_core::downloader::DownloaderConfig;
use riven_core::logging::{LogControl, LogSettings};
use riven_core::plugin::PluginRegistry;
use riven_core::settings::{FilesystemSettings, LibraryProfileMembership};
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::repo;
use riven_queue::JobQueue;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;

use crate::schema::auth::require_settings_access;
use crate::schema::queries::settings::{build_general_section, build_plugin_section};
use crate::schema::types::SettingsSection;
use crate::vfs_mount::VfsMountManager;

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

/// Build a [`LogSettings`] from a settings JSON object. The `enabled` flag is
/// supplied by the caller because different mutations source it differently
/// (an explicit toggle vs. the `logging_enabled` field); the remaining fields
/// are read uniformly from `settings`.
fn log_settings_from_json(settings: &serde_json::Value, enabled: bool) -> LogSettings {
    LogSettings {
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
            .and_then(serde_json::Value::as_u64)
            .map(|value| value as usize)
            .filter(|value| *value > 0)
            .unwrap_or(5),
        vfs_debug_logging: settings
            .get("vfs_debug_logging")
            .and_then(coerce_json_bool)
            .unwrap_or(false),
    }
}

pub(super) async fn rematch_filesystem_library_profiles_inner(
    filesystem_settings: &FilesystemSettings,
) -> Result<i64> {
    let candidates = repo::list_filesystem_profile_entry_candidates().await?;
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

    Ok(repo::update_library_profiles_batch(&updates).await? as i64)
}

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

        let profile =
            repo::upsert_ranking_profile(id, &name, canonical, enabled.unwrap_or(false)).await?;
        Ok(serde_json::to_value(profile)?)
    }

    /// Delete a custom ranking profile by ID. Built-in profiles cannot be deleted.
    async fn delete_custom_profile(&self, ctx: &Context<'_>, id: i32) -> Result<bool> {
        require_settings_access(ctx)?;
        Ok(repo::delete_ranking_profile(id).await?)
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
        Ok(repo::set_profile_enabled(&name, enabled).await?)
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

        Ok(repo::update_profile_settings(&name, settings).await?)
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

        repo::set_setting("rank_settings", canonical.clone()).await?;
        Ok(canonical)
    }

    /// Update all settings. Accepts a JSON object of key/value pairs.
    async fn update_all_settings(
        &self,
        ctx: &Context<'_>,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        Ok(repo::set_all_settings(settings).await?)
    }

    /// Mark the instance-wide first-run setup flow as completed.
    async fn complete_initial_setup(&self, ctx: &Context<'_>) -> Result<bool> {
        require_settings_access(ctx)?;
        repo::set_setting("instance.setup_completed", serde_json::Value::Bool(true)).await?;
        Ok(true)
    }

    /// The single write entry point for settings: the "general" section or any
    /// plugin (by name). Persists the section's values, reconciles its side
    /// effects (general → logging/downloader/VFS; plugin → revalidate), and
    /// returns the updated section so the UI gets fresh enabled/valid state.
    async fn update_settings(
        &self,
        ctx: &Context<'_>,
        section: String,
        values: serde_json::Value,
    ) -> Result<SettingsSection> {
        require_settings_access(ctx)?;
        if section == "general" {
            apply_general_settings(ctx, values).await?;
            build_general_section().await
        } else {
            apply_plugin_settings(ctx, &section, values).await?;
            let registry = ctx.data::<Arc<PluginRegistry>>()?;
            build_plugin_section(registry, &section).await
        }
    }

    /// Recompute stored library-profile matches for every existing media entry.
    async fn rematch_filesystem_library_profiles(&self, ctx: &Context<'_>) -> Result<i64> {
        require_settings_access(ctx)?;
        let queue = ctx.data::<Arc<JobQueue>>()?;
        let filesystem_settings = queue.filesystem_settings.read().await.clone();
        let updated = rematch_filesystem_library_profiles_inner(&filesystem_settings).await?;

        queue
            .filesystem_settings_revision
            .fetch_add(1, Ordering::SeqCst);

        Ok(updated)
    }
}

/// Persist and reconcile the general (non-plugin) settings: store them, then
/// apply to the live runtime (logging, downloader config, scheduling, and the
/// filesystem layout — rematching library profiles when the mount changes).
/// The single source of truth for general-settings side effects.
async fn apply_general_settings(ctx: &Context<'_>, settings: serde_json::Value) -> Result<()> {
    repo::set_setting("general", settings.clone()).await?;

    let log_control = ctx.data::<Arc<LogControl>>()?;
    let enabled = settings
        .get("logging_enabled")
        .and_then(coerce_json_bool)
        .unwrap_or(true);
    let log_settings = log_settings_from_json(&settings, enabled);
    log_control
        .apply(&log_settings)
        .map_err(|error| Error::new(error.to_string()))?;

    let cfg = ctx.data::<Arc<RwLock<DownloaderConfig>>>()?;
    let mut cfg = cfg.write().await;
    let mbps = |key: &str| {
        settings
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .map(|v| v as u32)
    };
    cfg.minimum_average_bitrate_movies = mbps("minimum_average_bitrate_movies");
    cfg.minimum_average_bitrate_episodes = mbps("minimum_average_bitrate_episodes");
    cfg.maximum_average_bitrate_movies = mbps("maximum_average_bitrate_movies");
    cfg.maximum_average_bitrate_episodes = mbps("maximum_average_bitrate_episodes");
    if let Some(v) = settings
        .get("attempt_unknown_downloads")
        .and_then(serde_json::Value::as_bool)
    {
        cfg.attempt_unknown_downloads = v;
    }
    drop(cfg);

    let queue = ctx.data::<Arc<JobQueue>>()?;
    let mut reindex_cfg = queue.reindex_config.write().await;
    reindex_cfg.schedule_offset_minutes = settings
        .get("schedule_offset_minutes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(reindex_cfg.schedule_offset_minutes);
    reindex_cfg.unknown_air_date_offset_days = settings
        .get("unknown_air_date_offset_days")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(reindex_cfg.unknown_air_date_offset_days);
    drop(reindex_cfg);
    queue.retry_interval_secs.store(
        settings
            .get("retry_interval_secs")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(queue.retry_interval_secs.load(Ordering::SeqCst)),
        Ordering::SeqCst,
    );
    queue.maximum_scrape_attempts.store(
        settings
            .get("maximum_scrape_attempts")
            .and_then(serde_json::Value::as_u64)
            .map_or(queue.maximum_scrape_attempts.load(Ordering::SeqCst), |v| {
                v as u32
            }),
        Ordering::SeqCst,
    );

    let previous_filesystem = queue.filesystem_settings.read().await.clone();
    let mut filesystem = settings
        .get("filesystem")
        .and_then(|v| serde_json::from_value::<FilesystemSettings>(v.clone()).ok())
        .unwrap_or_default();
    // An empty mount path means "unset" — the mount point is a boot-time concern,
    // so keep the running mount rather than tearing the VFS down by remounting at "".
    if filesystem.mount_path.trim().is_empty() {
        filesystem.mount_path = previous_filesystem.mount_path.clone();
    }
    *queue.filesystem_settings.write().await = filesystem.clone();
    *queue.vfs_layout.write().await = VfsLibraryLayout::new(filesystem.clone());
    if previous_filesystem != filesystem {
        let rematch_count = rematch_filesystem_library_profiles_inner(&filesystem).await?;
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

    Ok(())
}

/// Persist a plugin's settings and revalidate it in-memory with the new values.
/// `enabled` is taken from the values object when present, else left unchanged.
async fn apply_plugin_settings(
    ctx: &Context<'_>,
    plugin: &str,
    mut settings: serde_json::Value,
) -> Result<()> {
    let key = format!("plugin.{plugin}");
    let enabled = match settings
        .as_object_mut()
        .and_then(|obj| obj.remove("enabled"))
        .as_ref()
        .and_then(coerce_json_bool)
    {
        Some(enabled) => enabled,
        None => repo::get_plugin_enabled(plugin).await?,
    };

    repo::set_setting(&key, settings.clone()).await?;
    repo::set_plugin_enabled(plugin, enabled).await?;

    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    registry.revalidate_plugin(plugin, enabled, &settings).await;

    // The logging pseudo-plugin drives the live log subscriber.
    if plugin == "logs" {
        let log_control = ctx.data::<Arc<LogControl>>()?;
        let log_settings = log_settings_from_json(&settings, enabled);
        log_control
            .apply(&log_settings)
            .map_err(|error| Error::new(error.to_string()))?;
    }

    Ok(())
}
