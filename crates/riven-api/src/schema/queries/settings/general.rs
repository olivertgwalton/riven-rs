use super::*;

/// Derived, display-only key in the general section. Never persisted — see
/// `general_settings_values` and `apply_general_settings`.
pub(crate) const STREMIO_MANIFEST_URL_KEY: &str = "stremio_manifest_url";
/// User-editable public origin the Stremio manifest URL is built from.
pub(crate) const STREMIO_BASE_URL_KEY: &str = "stremio_base_url";

/// The SettingField schema describing the general (non-plugin) settings.
/// Single source of truth, shared by `settingsSections` and the writer.
fn general_settings_schema_fields(
    options: riven_db::repo::FilesystemLibraryFilterOptions,
) -> Vec<SettingField> {
    let riven_db::repo::FilesystemLibraryFilterOptions {
        genres,
        networks,
        languages,
        countries,
        content_ratings,
    } = options;
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
            SettingField::new(STREMIO_BASE_URL_KEY, "Public URL", FieldType::Url)
                .with_section("Stremio")
                .with_placeholder("https://riven.example.com")
                .with_description("The public URL Riven is reachable at. Stremio needs an absolute address, so the manifest URL below can only be built once this is set."),
            SettingField::new(STREMIO_MANIFEST_URL_KEY, "Manifest URL", FieldType::Url)
                .with_section("Stremio")
                .read_only()
                .with_description("Paste this into Stremio's Addons page to make your library appear in the stream picker. Rotating the API key invalidates it."),
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
                                .with_description("Only items matching these filters will appear in this profile.")
                                .with_fields(vec![
                                    SettingField::new("content_types", "Content types", FieldType::StringArray)
                                        .with_options(&["movie", "show"])
                                        .with_description("Restrict the profile to movies, shows, or both."),
                                    SettingField::new("genres", "Genres", FieldType::FilterArray)
                                        .with_dynamic_options(genres)
                                        .allow_custom_options()
                                        .with_description("Choose genres to include or exclude."),
                                    SettingField::new("networks", "Networks", FieldType::FilterArray)
                                        .with_dynamic_options(networks)
                                        .allow_custom_options()
                                        .with_description("Choose networks to include or exclude."),
                                    SettingField::new("languages", "Languages", FieldType::FilterArray)
                                        .with_dynamic_options(languages)
                                        .allow_custom_options()
                                        .with_description("Choose languages to include or exclude."),
                                    SettingField::new("countries", "Countries", FieldType::FilterArray)
                                        .with_dynamic_options(countries)
                                        .allow_custom_options()
                                        .with_description("Choose countries to include or exclude."),
                                    SettingField::new("content_ratings", "Content ratings", FieldType::FilterArray)
                                        .with_dynamic_options(content_ratings)
                                        .allow_custom_options()
                                        .with_description("Choose content ratings to include or exclude."),
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

/// Effective general settings: defaults merged with stored DB overrides, then
/// backend-derived values layered on top.
/// Single source of truth, shared by `settingsSections` and the writer.
async fn general_settings_values(addon_token: Option<&str>) -> Result<serde_json::Value> {
    let defaults = RivenSettings::default();
    let mut result = serde_json::json!({
        "dubbed_anime_only": defaults.dubbed_anime_only,
        "attempt_unknown_downloads": defaults.attempt_unknown_downloads,
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
        STREMIO_BASE_URL_KEY: "",
    });
    if let Some(stored) = repo::get_setting("general").await?
        && let (Some(obj), Some(stored_obj)) = (result.as_object_mut(), stored.as_object())
    {
        for (k, v) in stored_obj {
            obj.insert(k.clone(), v.clone());
        }
    }

    // Derived last, so a value a client previously round-tripped into the
    // stored blob can never shadow the real one.
    if let Some(obj) = result.as_object_mut() {
        let base_url = obj
            .get(STREMIO_BASE_URL_KEY)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let manifest_url =
            riven_core::stremio::manifest_url(base_url, addon_token).unwrap_or_default();
        obj.insert(
            STREMIO_MANIFEST_URL_KEY.to_string(),
            serde_json::Value::String(manifest_url),
        );
    }
    Ok(result)
}

/// Build the instance-wide "general" settings section. `addon_token` is the
/// Stremio addon token for this instance (`None` when the API is unauthenticated).
pub(crate) async fn build_general_section(addon_token: Option<&str>) -> Result<SettingsSection> {
    let options = match repo::list_filesystem_library_filter_options().await {
        Ok(options) => options,
        Err(error) => {
            tracing::warn!(%error, "could not load filesystem library filter options");
            Default::default()
        }
    };
    let schema = general_settings_schema_fields(options);
    Ok(SettingsSection {
        id: "general".to_string(),
        title: "General".to_string(),
        kind: "general".to_string(),
        schema: serde_json::to_value(&schema).unwrap_or(serde_json::Value::Array(vec![])),
        values: general_settings_values(addon_token).await?,
        category: None,
        enabled: None,
        valid: None,
        configured: None,
        missing_required_fields: Vec::new(),
        version: None,
    })
}
