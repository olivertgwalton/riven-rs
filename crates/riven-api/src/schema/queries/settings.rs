use async_graphql::*;
use riven_core::plugin::{PluginRegistry, SettingField};
use riven_core::settings::RivenSettings;
use riven_db::repo;
use std::sync::Arc;

use crate::schema::auth::require_settings_access;
use crate::schema::types::{InstanceStatus, PluginInfo};

#[derive(Default)]
pub struct CoreSettingsQuery;

#[Object]
impl CoreSettingsQuery {
    /// Get the current rank settings. Returns defaults if not yet configured.
    /// Each `custom_ranks` entry is annotated with a `"default"` field carrying
    /// the built-in score so the UI can display the effective value without a
    /// separate query.
    async fn rank_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        let settings: riven_rank::RankSettings = repo::get_setting(pool, "rank_settings")
            .await?
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let mut json = serde_json::to_value(&settings)
            .map_err(|e| Error::new(format!("failed to serialise rank settings: {e}")))?;
        strip_zero_ranks(&mut json);
        inject_rank_defaults(&mut json);
        Ok(json)
    }

    /// Return all quality profiles as an ordered array of
    /// `{ id, label, description, settings }` objects.
    /// The `settings` field reflects the *effective* settings (base preset merged
    /// with any user overrides stored in the database), so the UI always shows
    /// the values that will actually be used at runtime.
    async fn quality_profiles(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<sqlx::PgPool>()?;

        // Load all DB profile rows so we can look up stored overrides by name.
        let db_profiles = repo::list_ranking_profiles(pool).await.unwrap_or_default();

        let profiles: serde_json::Value = riven_rank::QualityProfile::ALL
            .iter()
            .map(|&p| {
                // Find the matching DB row (if any) for this built-in profile.
                let db_row = db_profiles.iter().find(|r| r.name == p.id());

                let effective_settings = db_row
                    .and_then(|row| {
                        // Only merge if the DB actually has non-empty settings.
                        let is_empty = matches!(&row.settings, serde_json::Value::Object(m) if m.is_empty())
                            || matches!(&row.settings, serde_json::Value::Null);
                        if is_empty {
                            return None;
                        }
                        riven_queue::flows::merge_builtin_profile_settings(p, &row.settings)
                            .ok()
                            .and_then(|s| serde_json::to_value(&s).ok())
                    })
                    .unwrap_or_else(|| {
                        serde_json::to_value(p.base_settings()).unwrap_or(serde_json::Value::Null)
                    });

                let mut settings = effective_settings;
                inject_rank_defaults(&mut settings);
                serde_json::json!({
                    "id":          p.id(),
                    "label":       p.label(),
                    "description": p.description(),
                    "settings":    settings,
                })
            })
            .collect();
        Ok(profiles)
    }

    /// Return the built-in default score for every CustomRank field.
    async fn rank_defaults(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        Ok(riven_rank::RankingModel::default().to_category_map())
    }

    /// Return all ranking profiles (built-in + custom) with their enabled status.
    async fn custom_profiles(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        let profiles = repo::list_ranking_profiles(pool).await?;
        Ok(serde_json::to_value(profiles)?)
    }

    /// Get all stored settings as a JSON object.
    async fn all_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_all_settings(pool).await?)
    }

    /// Return instance-level status flags used by frontend bootstrap flows.
    async fn instance_status(&self, ctx: &Context<'_>) -> Result<InstanceStatus> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let explicit_setup_completed =
            match repo::get_setting(pool, "instance.setup_completed").await? {
                Some(serde_json::Value::Bool(value)) => value,
                _ => false,
            };
        let inferred_setup_completed = if explicit_setup_completed {
            false
        } else {
            let registry = ctx.data::<Arc<PluginRegistry>>()?;
            let enabled_profile_count = repo::get_enabled_profiles(pool).await?.len();
            let valid_enabled_plugin_count = registry
                .all_plugins_info()
                .await
                .into_iter()
                .filter(|p| p.enabled && p.valid)
                .count();
            valid_enabled_plugin_count > 0 && enabled_profile_count > 0
        };
        Ok(InstanceStatus {
            setup_completed: explicit_setup_completed || inferred_setup_completed,
        })
    }

    /// Get info about all registered plugins.
    async fn plugin_info(&self, ctx: &Context<'_>) -> Result<Vec<PluginInfo>> {
        require_settings_access(ctx)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        Ok(registry
            .all_plugins_info()
            .await
            .into_iter()
            .map(|p| PluginInfo {
                name: p.name,
                version: p.version,
                enabled: p.enabled,
                valid: p.valid,
                schema: serde_json::to_value(p.schema).unwrap_or(serde_json::Value::Array(vec![])),
            })
            .collect())
    }

    /// Get effective settings for a specific plugin (env vars merged with any DB overrides).
    async fn plugin_settings(
        &self,
        ctx: &Context<'_>,
        plugin: String,
    ) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        let mut settings = registry
            .get_plugin_settings_json(&plugin)
            .await
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let enabled = registry
            .is_plugin_enabled(&plugin)
            .await
            .unwrap_or(repo::get_plugin_enabled(pool, &plugin).await?);
        if let Some(obj) = settings.as_object_mut() {
            obj.insert("enabled".to_string(), serde_json::Value::Bool(enabled));
        }
        Ok(settings)
    }

    /// Return whether a plugin is effectively enabled.
    async fn plugin_enabled(&self, ctx: &Context<'_>, plugin: String) -> Result<bool> {
        require_settings_access(ctx)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(registry
            .is_plugin_enabled(&plugin)
            .await
            .unwrap_or(repo::get_plugin_enabled(pool, &plugin).await?))
    }

    /// Return the SettingField schema for the general (non-plugin) settings.
    async fn general_settings_schema(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let schema: Vec<SettingField> = vec![
            SettingField::new("dubbed_anime_only", "Dubbed anime only", "boolean")
                .with_description("Only fetch dubbed versions of anime titles."),
            SettingField::new("attempt_unknown_downloads", "Attempt unknown downloads", "boolean")
                .with_description("Attempt to download torrents whose cache status cannot be verified. Enabling this degrades performance but may help if plugins cannot confirm cache status for your items."),
            SettingField::new("retry_interval_secs", "Retry interval (seconds)", "number")
                .with_default("600")
                .with_description("How often to retry stuck items. 0 = disabled. Default: 600 (10 m)."),
            SettingField::new("schedule_offset_minutes", "Re-index offset (minutes)", "number")
                .with_default("30")
                .with_description("How long after a known release/air date to wait before re-indexing an unreleased or ongoing item."),
            SettingField::new("unknown_air_date_offset_days", "Fallback re-index delay (days)", "number")
                .with_default("7")
                .with_description("Fallback delay used when an unreleased or ongoing item has no known future air date."),
            SettingField::new("minimum_average_bitrate_movies", "Min bitrate — movies (Mbps)", "number")
                .with_placeholder("Disabled")
                .with_description("Reject movie streams below this average bitrate. Leave blank to disable."),
            SettingField::new("minimum_average_bitrate_episodes", "Min bitrate — episodes (Mbps)", "number")
                .with_placeholder("Disabled")
                .with_description("Reject episode streams below this average bitrate. Leave blank to disable."),
            SettingField::new("maximum_average_bitrate_movies", "Max bitrate — movies (Mbps)", "number")
                .with_placeholder("Disabled")
                .with_description("Reject movie streams above this average bitrate (e.g. 50 to avoid large REMUXes). Leave blank to disable."),
            SettingField::new("maximum_average_bitrate_episodes", "Max bitrate — episodes (Mbps)", "number")
                .with_placeholder("Disabled")
                .with_description("Reject episode streams above this average bitrate. Leave blank to disable."),
            SettingField::new("filesystem", "Filesystem", "object")
                .with_description("Virtual filesystem mount settings and filtered library profile aliases.")
                .with_fields(vec![
                    SettingField::new("mount_path", "Mount path", "string")
                        .with_placeholder("/mount")
                        .with_description("Where the virtual filesystem should be mounted."),
                    SettingField::new("library_profiles", "Library profiles", "dictionary")
                        .with_description("Named filtered views that expose matching items under additional virtual paths.")
                        .with_key_placeholder("profile_key")
                        .with_add_label("Add profile")
                        .with_item_fields(vec![
                            SettingField::new("name", "Name", "string")
                                .required()
                                .with_description("Display name for this profile."),
                            SettingField::new("library_path", "Library path", "string")
                                .required()
                                .with_placeholder("/anime")
                                .with_description("Virtual path prefix to expose for this profile."),
                            SettingField::new("enabled", "Enabled", "boolean")
                                .with_description("Disable a profile without deleting its rules."),
                            SettingField::new("exclusive", "Exclusive", "boolean")
                                .with_description("Hide matched items from the default /movies and /shows paths so they only appear under this profile's path."),
                            SettingField::new("filter_rules", "Filter rules", "object")
                                .with_description("Only items matching all configured rules will appear in this profile. Positive values inside token lists use OR matching; prefix a value with ! to exclude it.")
                                .with_fields(vec![
                                    SettingField::new("content_types", "Content types", "string_array")
                                        .with_options(&["movie", "show"])
                                        .with_description("Restrict the profile to movies, shows, or both."),
                                    SettingField::new("genres", "Genres", "string_array")
                                        .with_description("Genre filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("networks", "Networks", "string_array")
                                        .with_description("Network filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("languages", "Languages", "string_array")
                                        .with_description("Language filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("countries", "Countries", "string_array")
                                        .with_description("Country filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("content_ratings", "Content ratings", "string_array")
                                        .with_description("Content rating filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("min_year", "Min year", "number")
                                        .with_description("Minimum release year for matching items."),
                                    SettingField::new("max_year", "Max year", "number")
                                        .with_description("Maximum release year for matching items."),
                                    SettingField::new("min_rating", "Min rating", "number")
                                        .with_description("Minimum numeric rating for matching items."),
                                    SettingField::new("max_rating", "Max rating", "number")
                                        .with_description("Maximum numeric rating for matching items."),
                                    SettingField::new("is_anime", "Anime filter", "nullable_boolean")
                                        .with_description("Only anime, only non-anime, or leave unset for any item."),
                                ]),
                        ]),
                ]),
        ];
        Ok(serde_json::to_value(schema).unwrap_or(serde_json::Value::Array(vec![])))
    }

    /// Get general (non-plugin) settings. Returns defaults merged with DB values.
    async fn general_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        let defaults = RivenSettings::default();
        let mut result = serde_json::json!({
            "dubbed_anime_only": defaults.dubbed_anime_only,
            "attempt_unknown_downloads": defaults.attempt_unknown_downloads,
            "minimum_average_bitrate_movies": defaults.minimum_average_bitrate_movies,
            "minimum_average_bitrate_episodes": defaults.minimum_average_bitrate_episodes,
            "maximum_average_bitrate_movies": defaults.maximum_average_bitrate_movies,
            "maximum_average_bitrate_episodes": defaults.maximum_average_bitrate_episodes,
            "retry_interval_secs": defaults.retry_interval_secs,
            "schedule_offset_minutes": defaults.schedule_offset_minutes,
            "unknown_air_date_offset_days": defaults.unknown_air_date_offset_days,
            "filesystem": defaults.filesystem,
        });
        if let Some(stored) = repo::get_setting(pool, "general").await?
            && let (Some(obj), Some(stored_obj)) = (result.as_object_mut(), stored.as_object())
        {
            for (k, v) in stored_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
        Ok(result)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Remove `"rank": 0` from every CustomRank entry — old DB data used 0 as the
/// "unset" sentinel before `rank` became `Option<i64>`.
fn strip_zero_ranks(json: &mut serde_json::Value) {
    let Some(custom_ranks) = json.get_mut("custom_ranks").and_then(|v| v.as_object_mut()) else {
        return;
    };
    for cat in custom_ranks.values_mut() {
        if let Some(fields) = cat.as_object_mut() {
            for entry in fields.values_mut() {
                if let Some(obj) = entry.as_object_mut()
                    && obj.get("rank").and_then(|v| v.as_i64()) == Some(0)
                {
                    obj.remove("rank");
                }
            }
        }
    }
}

/// Inject `"default": N` into every `CustomRank` entry so the UI always has
/// the built-in score available without a separate query.
fn inject_rank_defaults(json: &mut serde_json::Value) {
    let defaults = riven_rank::defaults::RankingModel::default().to_category_map();
    let (Some(custom_ranks), Some(def_obj)) = (
        json.get_mut("custom_ranks").and_then(|v| v.as_object_mut()),
        defaults.as_object(),
    ) else {
        return;
    };
    for (cat, cat_defaults) in def_obj {
        if let (Some(rank_cat), Some(cat_obj)) = (
            custom_ranks.get_mut(cat).and_then(|v| v.as_object_mut()),
            cat_defaults.as_object(),
        ) {
            for (field, default_score) in cat_obj {
                if let Some(entry) = rank_cat.get_mut(field).and_then(|v| v.as_object_mut()) {
                    entry.insert("default".to_string(), default_score.clone());
                }
            }
        }
    }
}
