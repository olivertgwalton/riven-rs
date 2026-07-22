use super::*;

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
