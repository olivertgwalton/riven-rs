use async_graphql::*;
use riven_core::plugin::{Display, FieldType, PluginRegistry, SettingField};
use riven_core::settings::RivenSettings;
use riven_db::repo;
use std::sync::Arc;

use crate::schema::auth::require_settings_access;
use crate::schema::types::{InstanceStatus, SettingsSection, SetupGroup};

#[derive(Default)]
pub struct CoreSettingsQuery;

#[Object]
impl CoreSettingsQuery {
    /// Return all quality profiles as an ordered array of
    /// `{ id, label, description, settings }` objects.
    /// The `settings` field reflects the *effective* settings (base preset merged
    /// with any user overrides stored in the database), so the UI always shows
    /// the values that will actually be used at runtime.
    async fn quality_profiles(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;

        let db_profiles = repo::list_ranking_profiles().await.unwrap_or_default();

        let profiles: serde_json::Value = riven_rank::QualityProfile::ALL
            .iter()
            .map(|&p| {
                let db_row = db_profiles.iter().find(|r| r.name == p.id());

                let effective_settings = db_row
                    .and_then(|row| {
                        let is_empty = matches!(&row.settings, serde_json::Value::Object(m) if m.is_empty())
                            || matches!(&row.settings, serde_json::Value::Null);
                        if is_empty {
                            return None;
                        }
                        riven_queue::discovery::merge_builtin_profile_settings(p, &row.settings)
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

    async fn rank_settings_schema(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        Ok(serde_json::to_value(build_rank_settings_schema())
            .unwrap_or(serde_json::Value::Array(vec![])))
    }

    async fn default_rank_profile(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let db_profiles = repo::list_ranking_profiles().await.unwrap_or_default();

        // Prefer an enabled built-in preset (matched by name to a QualityProfile).
        if let Some(preset) = riven_rank::QualityProfile::ALL
            .iter()
            .find(|p| db_profiles.iter().any(|r| r.enabled && r.name == p.id()))
        {
            let row = db_profiles.iter().find(|r| r.name == preset.id());
            let mut settings = row
                .and_then(|row| {
                    let is_empty = matches!(&row.settings, serde_json::Value::Object(m) if m.is_empty())
                        || matches!(&row.settings, serde_json::Value::Null);
                    if is_empty {
                        return None;
                    }
                    riven_queue::discovery::merge_builtin_profile_settings(*preset, &row.settings)
                        .ok()
                        .and_then(|s| serde_json::to_value(&s).ok())
                })
                .unwrap_or_else(|| {
                    serde_json::to_value(preset.base_settings()).unwrap_or(serde_json::Value::Null)
                });
            inject_rank_defaults(&mut settings);
            return Ok(serde_json::json!({ "name": preset.id(), "settings": settings }));
        }

        // Then an enabled custom (non-built-in) profile.
        if let Some(row) = db_profiles.iter().find(|r| r.enabled && !r.is_builtin) {
            let mut settings = row.settings.clone();
            inject_rank_defaults(&mut settings);
            return Ok(serde_json::json!({ "name": row.name, "settings": settings }));
        }

        // Otherwise the global rank settings, with no active profile.
        let settings: riven_rank::RankSettings = repo::get_setting("rank_settings")
            .await?
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let mut json = serde_json::to_value(&settings)
            .map_err(|e| Error::new(format!("failed to serialise rank settings: {e}")))?;
        strip_zero_ranks(&mut json);
        inject_rank_defaults(&mut json);
        Ok(serde_json::json!({ "name": serde_json::Value::Null, "settings": json }))
    }

    /// Return all ranking profiles (built-in + custom) with their enabled status.
    async fn custom_profiles(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let profiles = repo::list_ranking_profiles().await?;
        Ok(serde_json::to_value(profiles)?)
    }

    /// Get all stored settings as a JSON object.
    async fn all_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        Ok(repo::get_all_settings().await?)
    }

    /// Return instance-level status flags used by frontend bootstrap flows.
    /// Owns the setup-readiness rule so the UI never has to recompute it.
    async fn instance_status(&self, ctx: &Context<'_>) -> Result<InstanceStatus> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        let db_setup_completed = matches!(
            repo::get_setting("instance.setup_completed").await?,
            Some(serde_json::Value::Bool(true))
        );
        let setup_completed =
            env_bool("RIVEN_SETTING__SETUP_COMPLETED").unwrap_or(db_setup_completed);

        let enabled_profile_count = repo::get_enabled_profiles().await?.len() as i32;
        let enabled_valid_plugin_count = registry
            .all_plugins_info()
            .await
            .into_iter()
            .filter(|p| p.enabled && p.valid)
            .count() as i32;

        let mut blockers = Vec::new();
        if enabled_valid_plugin_count == 0 {
            blockers.push(
                "Enable and configure at least one plugin (a media server and a content \
                       source)."
                    .to_string(),
            );
        }
        if enabled_profile_count == 0 {
            blockers.push("Enable at least one quality profile.".to_string());
        }
        let ready_to_complete = blockers.is_empty();

        Ok(InstanceStatus {
            setup_completed,
            ready_to_complete,
            enabled_valid_plugin_count,
            enabled_profile_count,
            blockers,
        })
    }

    /// Ordered setup sections that plugins are grouped under (by `PluginInfo.category`).
    /// This is the single source of truth for setup-step grouping, labels, and order.
    async fn setup_groups(&self, ctx: &Context<'_>) -> Result<Vec<SetupGroup>> {
        require_settings_access(ctx)?;
        Ok(vec![
            SetupGroup {
                id: "media".to_string(),
                title: "Media Servers".to_string(),
                description: "Pick the server Riven should update after downloads finish."
                    .to_string(),
            },
            SetupGroup {
                id: "sources".to_string(),
                title: "Content Sources".to_string(),
                description: "Pick the sources Riven should scrape from.".to_string(),
            },
            SetupGroup {
                id: "services".to_string(),
                title: "Metadata & Requests".to_string(),
                description: "Connect metadata, lists, calendars, and request services."
                    .to_string(),
            },
        ])
    }

    /// Every configurable settings surface — the instance-wide "general"
    /// section plus one section per plugin — each with the schema to render it
    /// and its typed values. This is the single read the settings/setup UIs use.
    async fn settings_sections(&self, ctx: &Context<'_>) -> Result<Vec<SettingsSection>> {
        require_settings_access(ctx)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        let mut sections = vec![build_general_section().await?];
        for p in registry.all_plugins_info().await {
            sections.push(plugin_section_from(registry, &p).await);
        }
        Ok(sections)
    }
}

/// The SettingField schema describing the general (non-plugin) settings.
/// Single source of truth, shared by `settingsSections` and the writer.
fn general_settings_schema_fields() -> Vec<SettingField> {
    vec![
            SettingField::new("dubbed_anime_only", "Dubbed anime only", FieldType::Boolean)
                .with_section("Content")
                .with_description("Only fetch dubbed versions of anime titles."),
            SettingField::new("attempt_unknown_downloads", "Attempt unknown downloads", FieldType::Boolean)
                .with_section("Content")
                .with_description("Try to download torrents even when cache status can't be confirmed. May help in some cases but slows things down."),
            SettingField::new("retry_interval_secs", "Retry interval (seconds)", FieldType::Number)
                .with_section("Scheduling")
                .with_default("600")
                .with_description("How often (in seconds) to retry items that are stuck. 0 disables retries."),
            SettingField::new("maximum_scrape_attempts", "Max scrape attempts", FieldType::Number)
                .with_section("Scheduling")
                .with_default("0")
                .with_description("Mark an item as failed after this many scrape attempts in a row. 0 = keep retrying forever."),
            SettingField::new("schedule_offset_minutes", "Re-index offset (minutes)", FieldType::Number)
                .with_section("Scheduling")
                .with_default("30")
                .with_description("How long to wait after a release or air date before checking for it (in minutes)."),
            SettingField::new("unknown_air_date_offset_days", "Fallback re-index delay (days)", FieldType::Number)
                .with_section("Scheduling")
                .with_default("7")
                .with_description("How many days to wait before rechecking an item with no known release date."),
            SettingField::new("minimum_average_bitrate_movies", "Min bitrate — movies (Mbps)", FieldType::Number)
                .with_section("Bitrate Limits")
                .with_placeholder("Disabled")
                .with_description("Reject movie streams below this average bitrate. Leave blank to disable."),
            SettingField::new("minimum_average_bitrate_episodes", "Min bitrate — episodes (Mbps)", FieldType::Number)
                .with_section("Bitrate Limits")
                .with_placeholder("Disabled")
                .with_description("Reject episode streams below this average bitrate. Leave blank to disable."),
            SettingField::new("maximum_average_bitrate_movies", "Max bitrate — movies (Mbps)", FieldType::Number)
                .with_section("Bitrate Limits")
                .with_placeholder("Disabled")
                .with_description("Skip movies above this bitrate (e.g. 50 to avoid large REMUXes). Leave blank to disable."),
            SettingField::new("maximum_average_bitrate_episodes", "Max bitrate — episodes (Mbps)", FieldType::Number)
                .with_section("Bitrate Limits")
                .with_placeholder("Disabled")
                .with_description("Reject episode streams above this average bitrate. Leave blank to disable."),
            SettingField::new("logging_enabled", "Application logging", FieldType::Boolean)
                .with_section("Logging")
                .with_description("Turn application logging on or off."),
            SettingField::new("log_level", "Logging verbosity", FieldType::Select)
                .with_section("Logging")
                .with_default("info")
                .with_options(&["error", "warn", "info", "debug", "trace"])
                .with_description("Choose how verbose the application logs should be."),
            SettingField::new("log_rotation", "Log rotation", FieldType::Select)
                .with_section("Logging")
                .with_default("hourly")
                .with_options(&["hourly", "daily"])
                .with_description("Rotate log files on this schedule. Takes effect after restart."),
            SettingField::new("log_max_files", "Retained log files", FieldType::Number)
                .with_section("Logging")
                .with_default("5")
                .with_description("Maximum number of rotated log files to keep on disk. Takes effect after restart."),
            SettingField::new("vfs_debug_logging", "VFS debug logging", FieldType::Boolean)
                .with_section("Logging")
                .with_description("Log detailed virtual filesystem activity. Enable when troubleshooting file access issues."),
            SettingField::new("filesystem", "Filesystem", FieldType::Object)
                .with_section("Filesystem")
                .with_description("Where to mount Riven's virtual filesystem and any custom library views.")
                .with_fields(vec![
                    SettingField::new("mount_path", "Mount path", FieldType::Text)
                        .with_placeholder("/mount")
                        .with_description("Where the virtual filesystem should be mounted."),
                    SettingField::new("library_profiles", "Library profiles", FieldType::Dictionary)
                        .with_description("Custom library folders that show a filtered subset of your content.")
                        .with_key_placeholder("profile_key")
                        .with_add_label("Add profile")
                        .with_item_fields(vec![
                            SettingField::new("name", "Name", FieldType::Text)
                                .required()
                                .with_description("Display name for this profile."),
                            SettingField::new("library_path", "Library path", FieldType::Text)
                                .required()
                                .with_placeholder("/anime")
                                .with_description("Virtual path prefix to expose for this profile."),
                            SettingField::new("enabled", "Enabled", FieldType::Boolean)
                                .with_description("Disable a profile without deleting its rules."),
                            SettingField::new("exclusive", "Exclusive", FieldType::Boolean)
                                .with_description("Hide these items from the main library — only show them under this profile."),
                            SettingField::new("filter_rules", "Filter rules", FieldType::Object)
                                .with_description("Only items matching these filters will appear in this profile. Prefix a value with ! to exclude it.")
                                .with_fields(vec![
                                    SettingField::new("content_types", "Content types", FieldType::StringArray)
                                        .with_options(&["movie", "show"])
                                        .with_description("Restrict the profile to movies, shows, or both."),
                                    SettingField::new("genres", "Genres", FieldType::StringArray)
                                        .with_description("Filter by genre. Prefix with ! to exclude."),
                                    SettingField::new("networks", "Networks", FieldType::StringArray)
                                        .with_description("Filter by network. Prefix with ! to exclude."),
                                    SettingField::new("languages", "Languages", FieldType::StringArray)
                                        .with_description("Filter by language. Prefix with ! to exclude."),
                                    SettingField::new("countries", "Countries", FieldType::StringArray)
                                        .with_description("Filter by country. Prefix with ! to exclude."),
                                    SettingField::new("content_ratings", "Content ratings", FieldType::StringArray)
                                        .with_description("Filter by content rating. Prefix with ! to exclude."),
                                    SettingField::new("min_year", "Min year", FieldType::Number)
                                        .with_description("Minimum release year for matching items."),
                                    SettingField::new("max_year", "Max year", FieldType::Number)
                                        .with_description("Maximum release year for matching items."),
                                    SettingField::new("min_rating", "Min rating", FieldType::Number)
                                        .with_description("Minimum numeric rating for matching items."),
                                    SettingField::new("max_rating", "Max rating", FieldType::Number)
                                        .with_description("Maximum numeric rating for matching items."),
                                    SettingField::new("is_anime", "Anime filter", FieldType::NullableBoolean)
                                        .with_bool_labels("Anime only", "Non-anime only")
                                        .with_description("Only anime, only non-anime, or leave unset for any item."),
                                ]),
                        ]),
                ]),
    ]
}

/// Effective general settings: defaults merged with stored DB overrides.
/// Single source of truth, shared by `settingsSections` and the writer.
async fn general_settings_values() -> Result<serde_json::Value> {
    let defaults = RivenSettings::default();
    let mut result = serde_json::json!({
        "dubbed_anime_only": defaults.dubbed_anime_only,
        "attempt_unknown_downloads": defaults.attempt_unknown_downloads,
        "minimum_average_bitrate_movies": defaults.minimum_average_bitrate_movies,
        "minimum_average_bitrate_episodes": defaults.minimum_average_bitrate_episodes,
        "maximum_average_bitrate_movies": defaults.maximum_average_bitrate_movies,
        "maximum_average_bitrate_episodes": defaults.maximum_average_bitrate_episodes,
        "retry_interval_secs": defaults.retry_interval_secs,
        "maximum_scrape_attempts": defaults.maximum_scrape_attempts,
        "schedule_offset_minutes": defaults.schedule_offset_minutes,
        "unknown_air_date_offset_days": defaults.unknown_air_date_offset_days,
        "logging_enabled": defaults.logging_enabled,
        "log_level": defaults.log_level,
        "log_rotation": defaults.log_rotation,
        "log_max_files": defaults.log_max_files,
        "vfs_debug_logging": defaults.vfs_debug_logging,
        "filesystem": defaults.filesystem,
    });
    if let Some(stored) = repo::get_setting("general").await?
        && let (Some(obj), Some(stored_obj)) = (result.as_object_mut(), stored.as_object())
    {
        for (k, v) in stored_obj {
            obj.insert(k.clone(), v.clone());
        }
    }
    Ok(result)
}

/// Build the instance-wide "general" settings section.
pub(crate) async fn build_general_section() -> Result<SettingsSection> {
    let schema = general_settings_schema_fields();
    Ok(SettingsSection {
        id: "general".to_string(),
        title: "General".to_string(),
        kind: "general".to_string(),
        schema: serde_json::to_value(&schema).unwrap_or(serde_json::Value::Array(vec![])),
        values: general_settings_values().await?,
        category: None,
        enabled: None,
        valid: None,
        configured: None,
        missing_required_fields: Vec::new(),
        version: None,
    })
}

/// Build a plugin's settings section from its registry info: typed values
/// (coerced from the flat string map via the schema), plus enable/validity.
pub(crate) async fn plugin_section_from(
    registry: &PluginRegistry,
    p: &riven_core::plugin::PluginInfo,
) -> SettingsSection {
    let raw = registry
        .get_plugin_settings_json(&p.name)
        .await
        .unwrap_or(serde_json::Value::Object(Default::default()));
    let mut values = coerce_settings(&p.schema, &raw);
    if let Some(obj) = values.as_object_mut() {
        obj.insert("enabled".to_string(), serde_json::Value::Bool(p.enabled));
    }
    let missing_required_fields: Vec<String> = p
        .schema
        .iter()
        .filter(|f| f.required)
        .filter(|f| !setting_value_present(&raw, &f.key))
        .map(|f| f.key.to_string())
        .collect();
    let configured = missing_required_fields.is_empty();
    SettingsSection {
        id: p.name.clone(),
        title: p.name.clone(),
        kind: "plugin".to_string(),
        schema: serde_json::to_value(&p.schema).unwrap_or(serde_json::Value::Array(vec![])),
        values,
        category: Some(p.category.clone()),
        enabled: Some(p.enabled),
        valid: Some(p.valid),
        configured: Some(configured),
        missing_required_fields,
        version: Some(p.version.clone()),
    }
}

/// Build a single plugin's section by name (used by the writer after a save).
pub(crate) async fn build_plugin_section(
    registry: &PluginRegistry,
    name: &str,
) -> Result<SettingsSection> {
    let info = registry.all_plugins_info().await;
    let p = info
        .iter()
        .find(|p| p.name == name)
        .ok_or_else(|| Error::new(format!("unknown plugin: {name}")))?;
    Ok(plugin_section_from(registry, p).await)
}

/// Coerce a flat string-map of plugin settings into typed JSON per the schema,
/// so the frontend renders/edits typed values with no client-side adaptation.
fn coerce_settings(schema: &[SettingField], raw: &serde_json::Value) -> serde_json::Value {
    let obj = raw.as_object().cloned().unwrap_or_default();
    let mut out = serde_json::Map::new();
    for field in schema {
        let key = field.key.as_ref();
        if let Some(value) = obj.get(key) {
            out.insert(key.to_string(), coerce_value(field.field_type, value));
        }
    }
    serde_json::Value::Object(out)
}

fn coerce_value(field_type: FieldType, value: &serde_json::Value) -> serde_json::Value {
    let as_str = value.as_str();
    match field_type {
        FieldType::Boolean => serde_json::Value::Bool(value.as_bool().unwrap_or_else(|| {
            matches!(
                as_str.map(str::to_ascii_lowercase).as_deref(),
                Some("true" | "1" | "yes" | "on")
            )
        })),
        FieldType::NullableBoolean => match as_str {
            Some("true") => serde_json::Value::Bool(true),
            Some("false") => serde_json::Value::Bool(false),
            _ if value.is_boolean() => value.clone(),
            _ => serde_json::Value::Null,
        },
        FieldType::Number => match as_str {
            Some("") => serde_json::Value::Null,
            Some(s) => s
                .parse::<i64>()
                .map(Into::into)
                .or_else(|_| s.parse::<f64>().map(|n| serde_json::json!(n)))
                .unwrap_or(serde_json::Value::Null),
            None if value.is_number() => value.clone(),
            None => serde_json::Value::Null,
        },
        FieldType::StringArray => match as_str {
            Some(s) => serde_json::from_str::<Vec<String>>(s)
                .map(|v| serde_json::json!(v))
                .unwrap_or_else(|_| {
                    serde_json::json!(
                        s.split(',')
                            .map(|x| x.trim().to_string())
                            .filter(|x| !x.is_empty())
                            .collect::<Vec<_>>()
                    )
                }),
            None if value.is_array() => value.clone(),
            None => serde_json::json!([]),
        },
        FieldType::Object | FieldType::Dictionary => match as_str {
            Some(s) => serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!({})),
            None if value.is_object() => value.clone(),
            None => serde_json::json!({}),
        },
        // text / password / url / textarea / select / custom_rank stay as-is
        _ => value.clone(),
    }
}

/// Whether a required setting key has a usable value in the effective settings
/// (present, non-null, non-empty string, non-empty array).
fn setting_value_present(settings: &serde_json::Value, key: &str) -> bool {
    match settings.get(key) {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::String(s)) => !s.trim().is_empty(),
        Some(serde_json::Value::Array(a)) => !a.is_empty(),
        Some(_) => true,
    }
}

/// Strict boolean env var: `true` => Some(true), `false` => Some(false),
/// anything else (or unset) => None (caller falls back).
fn env_bool(key: &str) -> Option<bool> {
    match std::env::var(key)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// Label for a resolution key: strip the leading `r` before the digits so
/// `r2160p` → "2160p"; everything else (e.g. `unknown`) falls back to humanize.
fn resolution_label(key: &str) -> String {
    match key.strip_prefix('r') {
        Some(rest) if rest.starts_with(|c: char| c.is_ascii_digit()) => rest.to_string(),
        _ => humanize(key),
    }
}

/// Turn a snake_case key into a human label, e.g. `dolby_vision` → "Dolby Vision".
fn humanize(key: &str) -> String {
    key.split(['_', ' '])
        .filter(|s| !s.is_empty())
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build the SettingField schema for `RankSettings`. The resolution / options /
/// languages descriptors are static; `custom_ranks` is generated from the ranking
/// model defaults so it stays in sync with the scoring model automatically.
fn build_rank_settings_schema() -> Vec<SettingField> {
    // `resolutions` is a nested object on RankSettings (rank.resolutions.*), and
    // `resolution_ranks` carries an extra r1440p tier. Labels strip the leading
    // `r` so they read "2160p", not "R2160p".
    let resolution_toggles = ["r2160p", "r1080p", "r720p", "r480p", "r360p", "unknown"];
    let resolution_rank_tiers = [
        "r2160p", "r1440p", "r1080p", "r720p", "r480p", "r360p", "unknown",
    ];
    let mut fields: Vec<SettingField> = Vec::new();

    fields.push(
        SettingField::new("resolutions", "Resolutions", FieldType::Object)
            .with_section("Resolutions")
            .with_display(Display::Grid)
            .with_description("Which resolutions Riven will accept.")
            .with_fields(
                resolution_toggles
                    .iter()
                    .map(|r| {
                        SettingField::new(r.to_string(), resolution_label(r), FieldType::Boolean)
                    })
                    .collect(),
            ),
    );
    fields.push(
        SettingField::new(
            "resolution_ranks",
            "Resolution tie-breakers",
            FieldType::Object,
        )
        .with_section("Resolutions")
        .with_display(Display::Grid)
        .with_description(
            "Score applied per resolution to break ties between otherwise equal streams.",
        )
        .with_fields(
            resolution_rank_tiers
                .iter()
                .map(|r| SettingField::new(r.to_string(), resolution_label(r), FieldType::Number))
                .collect(),
        ),
    );

    for (key, label, desc) in [
        (
            "require",
            "Required patterns",
            "Only accept streams whose title matches these regex patterns.",
        ),
        (
            "exclude",
            "Excluded patterns",
            "Reject streams whose title matches these regex patterns.",
        ),
        (
            "preferred",
            "Preferred patterns",
            "Boost streams whose title matches these regex patterns.",
        ),
    ] {
        fields.push(
            SettingField::new(key, label, FieldType::StringArray)
                .with_section("Filters")
                .with_description(desc),
        );
    }

    fields.push(
        SettingField::new("options", "Options", FieldType::Object)
            .with_section("Options")
            .with_display(Display::Grid)
            .with_fields(vec![
                SettingField::new(
                    "title_similarity",
                    "Title similarity threshold",
                    FieldType::Number,
                )
                .with_description(
                    "Minimum fuzzy title-match score (0–1) required to accept a stream.",
                ),
                SettingField::new(
                    "remove_ranks_under",
                    "Remove ranks under",
                    FieldType::Number,
                )
                .with_description("Discard any stream whose total rank falls below this value."),
                SettingField::new("remove_all_trash", "Remove all trash", FieldType::Boolean)
                    .with_description("Reject releases flagged as trash (cam, telesync, etc.)."),
                SettingField::new(
                    "remove_unknown_languages",
                    "Remove unknown languages",
                    FieldType::Boolean,
                ),
                SettingField::new(
                    "allow_english_in_languages",
                    "Always allow English",
                    FieldType::Boolean,
                ),
                SettingField::new(
                    "remove_adult_content",
                    "Remove adult content",
                    FieldType::Boolean,
                ),
                SettingField::new(
                    "enable_fetch_speed_mode",
                    "Fetch speed mode",
                    FieldType::Boolean,
                )
                .with_description("Stop scoring once a good-enough stream is found."),
            ]),
    );

    fields.push(
        SettingField::new("languages", "Languages", FieldType::Object)
            .with_section("Languages")
            .with_fields(vec![
                SettingField::new("required", "Required", FieldType::StringArray),
                SettingField::new("allowed", "Allowed", FieldType::StringArray),
                SettingField::new("exclude", "Excluded", FieldType::StringArray),
                SettingField::new("preferred", "Preferred", FieldType::StringArray),
            ]),
    );

    let defaults = riven_rank::RankingModel::default().to_category_map();
    if let Some(categories) = defaults.as_object() {
        let category_fields: Vec<SettingField> = categories
            .iter()
            .filter_map(|(category, entries)| {
                let entry_obj = entries.as_object()?;
                let rank_fields = entry_obj
                    .keys()
                    .map(|key| SettingField::new(key.clone(), humanize(key), FieldType::CustomRank))
                    .collect();
                Some(
                    SettingField::new(category.clone(), humanize(category), FieldType::Object)
                        .with_display(Display::Grid)
                        .with_fields(rank_fields),
                )
            })
            .collect();
        fields.push(
            SettingField::new("custom_ranks", "Custom ranks", FieldType::Object)
                .with_section("Custom ranks")
                .with_display(Display::Tabs)
                .with_fields(category_fields),
        );
    }

    fields
}

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
                    && obj.get("rank").and_then(serde_json::Value::as_i64) == Some(0)
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
