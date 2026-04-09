use super::discovery::{discover_streams, ensure_download_target};
use async_graphql::*;
use plugin_logs::{LogControl, LogSettings};
use riven_core::downloader::DownloaderConfig;
use riven_core::events::RivenEvent;
use riven_core::plugin::PluginRegistry;
use riven_core::settings::{FilesystemSettings, LibraryProfileMembership};
use riven_core::types::*;
use riven_core::vfs_layout::VfsLibraryLayout;
use riven_db::entities::*;
use riven_db::repo;
use riven_queue::orchestrator::LibraryOrchestrator;
use riven_queue::{DownloadJob, IndexJob, JobQueue};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;

// ── Mutation root ──

pub struct MutationRoot;

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

async fn rematch_filesystem_library_profiles_inner(
    pool: &sqlx::PgPool,
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

#[Object]
impl MutationRoot {
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
        let validated: riven_rank::RankSettings = serde_json::from_value(settings)
            .map_err(|e| Error::new(format!("invalid rank settings: {e}")))?;
        let canonical = serde_json::to_value(&validated)
            .map_err(|e| Error::new(format!("failed to serialise rank settings: {e}")))?;

        let pool = ctx.data::<sqlx::PgPool>()?;
        let profile =
            repo::upsert_ranking_profile(pool, id, &name, canonical, enabled.unwrap_or(false))
                .await?;
        Ok(serde_json::to_value(profile)?)
    }

    /// Delete a custom ranking profile by ID. Built-in profiles cannot be deleted.
    async fn delete_custom_profile(&self, ctx: &Context<'_>, id: i32) -> Result<bool> {
        let pool = ctx.data::<sqlx::PgPool>()?;
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
        let pool = ctx.data::<sqlx::PgPool>()?;
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
        // Validate that the JSON is a valid RankSettings shape.
        let _validated: riven_rank::RankSettings = serde_json::from_value(settings.clone())
            .map_err(|e| Error::new(format!("invalid rank settings: {e}")))?;

        let pool = ctx.data::<sqlx::PgPool>()?;
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
        let validated: riven_rank::RankSettings = serde_json::from_value(settings)
            .map_err(|e| Error::new(format!("invalid rank settings: {e}")))?;
        let canonical = serde_json::to_value(&validated)
            .map_err(|e| Error::new(format!("failed to serialise rank settings: {e}")))?;

        let pool = ctx.data::<sqlx::PgPool>()?;
        repo::set_setting(pool, "rank_settings", canonical.clone()).await?;
        Ok(canonical)
    }

    /// Update all settings. Accepts a JSON object of key/value pairs.
    async fn update_all_settings(
        &self,
        ctx: &Context<'_>,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::set_all_settings(pool, settings).await?)
    }

    /// Mark the instance-wide first-run setup flow as completed.
    async fn complete_initial_setup(&self, ctx: &Context<'_>) -> Result<bool> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        repo::set_setting(
            pool,
            "instance.setup_completed",
            serde_json::Value::Bool(true),
        )
        .await?;
        Ok(true)
    }

    /// Delete a specific filesystem entry (a single downloaded version) by its ID.
    /// Returns true if the entry was found and deleted.
    async fn delete_filesystem_entry(&self, ctx: &Context<'_>, id: i64) -> Result<bool> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::delete_filesystem_entry(pool, id).await?)
    }

    /// Reset items to Indexed state and clear failed_attempts.
    async fn reset_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::reset_items_by_ids(pool, ids).await? as i64)
    }

    /// Clear failed_attempts for items so they will be retried.
    async fn retry_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::retry_items_by_ids(pool, ids).await? as i64)
    }

    /// Remove items by ID.
    async fn remove_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let deleted_paths = repo::get_media_entry_paths_for_items(pool, &ids)
            .await
            .unwrap_or_default();
        let external_request_ids = repo::get_external_request_ids_for_items(pool, &ids)
            .await
            .unwrap_or_default();

        let count = repo::delete_items_by_ids(pool, ids.clone()).await? as i64;

        if !ids.is_empty() {
            job_queue
                .notify(RivenEvent::MediaItemsDeleted {
                    item_ids: ids,
                    external_request_ids,
                    deleted_paths,
                })
                .await;
        }

        Ok(count)
    }

    /// Pause items.
    async fn pause_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::pause_items_by_ids(pool, ids).await? as i64)
    }

    /// Unpause items (transitions back to Indexed).
    async fn unpause_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::unpause_items_by_ids(pool, ids).await? as i64)
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
        let pool = ctx.data::<sqlx::PgPool>()?;
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
                enabled: settings
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
                    .unwrap_or(72),
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
        let pool = ctx.data::<sqlx::PgPool>()?;
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
                    .unwrap_or(72),
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
        let pool = ctx.data::<sqlx::PgPool>()?;
        repo::set_setting(pool, "general", settings.clone()).await?;

        let cfg = ctx.data::<std::sync::Arc<RwLock<DownloaderConfig>>>()?;
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
        let pool = ctx.data::<sqlx::PgPool>()?;
        let queue = ctx.data::<Arc<JobQueue>>()?;
        let filesystem_settings = queue.filesystem_settings.read().await.clone();
        let updated = rematch_filesystem_library_profiles_inner(pool, &filesystem_settings).await?;

        queue
            .filesystem_settings_revision
            .fetch_add(1, Ordering::SeqCst);

        Ok(updated)
    }

    /// Trigger a scrape for an existing item.
    /// For shows, optionally provide season_numbers to scrape specific seasons.
    /// If season_numbers is omitted, all requested seasons in Indexed state are scraped.
    async fn scrape_item(
        &self,
        ctx: &Context<'_>,
        id: i64,
        season_numbers: Option<Vec<i32>>,
    ) -> Result<String> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let item = repo::get_media_item(pool, id)
            .await?
            .ok_or_else(|| Error::new("Item not found"))?;

        orchestrator
            .queue_scrape_for_item(&item, season_numbers.as_deref(), true)
            .await;

        Ok("Scrape queued".to_string())
    }

    /// Discover stream candidates without creating or mutating media items.
    async fn discover_streams(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        seasons: Option<Vec<i32>>,
        cached_only: Option<bool>,
    ) -> Result<Vec<super::types::DiscoveredStream>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        discover_streams(
            pool,
            registry.as_ref(),
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            seasons.as_deref(),
            cached_only.unwrap_or(false),
        )
        .await
    }

    /// Create or update the real item only after the user picks a specific discovered stream.
    async fn download_discovered_stream(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        season_number: Option<i32>,
        info_hash: String,
        parsed_data: Option<serde_json::Value>,
        rank: Option<i64>,
    ) -> Result<String> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let target = ensure_download_target(
            pool,
            registry.as_ref(),
            job_queue,
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            season_number,
        )
        .await?;

        let stream = repo::upsert_stream(pool, &info_hash, parsed_data, rank).await?;
        repo::link_stream_to_item(pool, target.id, stream.id).await?;

        job_queue
            .push_download(DownloadJob {
                id: target.id,
                info_hash: info_hash.clone(),
                magnet: format!("magnet:?xt=urn:btih:{info_hash}"),
                preferred_info_hash: Some(info_hash),
            })
            .await;

        Ok("Download queued".to_string())
    }

    /// Create or reuse a non-requested media item, then index/scrape it so streams can be inspected.
    async fn discover_item(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        seasons: Option<Vec<i32>>,
    ) -> Result<MediaItem> {
        if !matches!(item_type, MediaItemType::Movie | MediaItemType::Show) {
            return Err(Error::new(
                "Only Movie and Show types can be discovered directly",
            ));
        }

        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let item = if let Some(existing) = repo::find_existing_media_item(
            pool,
            item_type,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
        )
        .await?
        {
            existing
        } else {
            repo::add_media_item_unrequested(
                pool,
                item_type,
                title,
                imdb_id.clone(),
                tmdb_id,
                tvdb_id,
            )
            .await?
        };

        if item.imdb_id.is_some() {
            orchestrator
                .queue_scrape_for_item(&item, seasons.as_deref(), false)
                .await;
        } else {
            job_queue.push_index(IndexJob::from_item(&item)).await;
        }

        Ok(item)
    }

    /// Download a specific stream already linked to a media item.
    async fn download_selected_stream(
        &self,
        ctx: &Context<'_>,
        item_id: i64,
        stream_id: i64,
    ) -> Result<String> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let stream = repo::get_stream_for_item(pool, item_id, stream_id)
            .await?
            .ok_or_else(|| Error::new("Stream not found for item"))?;

        let info_hash = stream.info_hash.clone();
        job_queue
            .push_download(DownloadJob {
                id: item_id,
                info_hash: info_hash.clone(),
                magnet: format!("magnet:?xt=urn:btih:{info_hash}"),
                preferred_info_hash: Some(info_hash),
            })
            .await;

        Ok("Download queued".to_string())
    }

    /// Add a new media item to track and immediately queue it for indexing.
    /// For shows, `seasons` is an optional list of season numbers to request.
    /// If omitted, all non-special seasons are requested.
    async fn add_item(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        seasons: Option<Vec<i32>>,
    ) -> Result<MediaItem> {
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let orchestrator = LibraryOrchestrator::new(job_queue.as_ref());

        let outcome = match item_type {
            MediaItemType::Movie => orchestrator
                .upsert_requested_movie(&title, imdb_id.as_deref(), tmdb_id.as_deref(), None, None)
                .await
                .map_err(Error::from)?,
            MediaItemType::Show => orchestrator
                .upsert_requested_show(
                    &title,
                    imdb_id.as_deref(),
                    tvdb_id.as_deref(),
                    None,
                    None,
                    seasons.as_deref(),
                )
                .await
                .map_err(Error::from)?,
            _ => {
                return Err(Error::new(
                    "Only Movie and Show types can be added directly",
                ));
            }
        };

        orchestrator
            .enqueue_after_request(&outcome, seasons.as_deref())
            .await;

        Ok(outcome.item)
    }
}
