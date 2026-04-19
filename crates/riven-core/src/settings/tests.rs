use std::collections::HashMap;

use super::*;
use crate::types::ContentRating;

fn plugin_settings(values: &[(&str, &str)]) -> PluginSettings {
    PluginSettings::from_pairs("TEST", values)
}

#[test]
fn matching_profiles_allow_any_positive_token_and_respect_exclusions() {
    let mut library_profiles = HashMap::new();
    library_profiles.insert(
        "kids".to_string(),
        FilesystemLibraryProfile {
            name: "Kids".to_string(),
            library_path: "/kids".to_string(),
            enabled: true,
            exclusive: false,
            filter_rules: FilesystemFilterRules {
                content_types: vec![FilesystemContentType::Movie],
                genres: vec![
                    "animation".to_string(),
                    "family".to_string(),
                    "!horror".to_string(),
                ],
                networks: vec![],
                content_ratings: vec!["pg".to_string(), "tv-pg".to_string(), "!r".to_string()],
                languages: vec![],
                countries: vec![],
                min_year: None,
                max_year: None,
                min_rating: None,
                max_rating: None,
                is_anime: None,
            },
        },
    );
    let settings = FilesystemSettings {
        mount_path: "/mount".to_string(),
        library_profiles,
    };
    let metadata = FilesystemItemMetadata {
        genres: vec!["animation".to_string(), "family".to_string()],
        network: None,
        content_rating: Some(ContentRating::Pg),
        language: None,
        country: None,
        year: None,
        rating: None,
        is_anime: false,
    };

    assert_eq!(
        settings.matching_profile_keys(&metadata, FilesystemContentType::Movie),
        LibraryProfileMembership(vec!["kids".to_string()])
    );
}

#[test]
fn matching_profiles_reject_when_no_positive_token_matches() {
    let settings = FilesystemSettings {
        mount_path: "/mount".to_string(),
        library_profiles: HashMap::from([(
            "nonkids".to_string(),
            FilesystemLibraryProfile {
                name: "Non-kids".to_string(),
                library_path: "/nonkids".to_string(),
                enabled: true,
                exclusive: false,
                filter_rules: FilesystemFilterRules {
                    content_types: vec![FilesystemContentType::Movie, FilesystemContentType::Show],
                    genres: vec!["family".to_string(), "children".to_string()],
                    networks: vec![],
                    content_ratings: vec!["tv-14".to_string(), "r".to_string()],
                    languages: vec![],
                    countries: vec![],
                    min_year: None,
                    max_year: None,
                    min_rating: None,
                    max_rating: None,
                    is_anime: Some(false),
                },
            },
        )]),
    };

    let metadata = FilesystemItemMetadata {
        genres: vec!["drama".to_string()],
        network: None,
        content_rating: Some(ContentRating::Pg),
        language: None,
        country: None,
        year: None,
        rating: None,
        is_anime: false,
    };

    assert_eq!(
        settings.matching_profile_keys(&metadata, FilesystemContentType::Movie),
        LibraryProfileMembership::default()
    );
}

#[test]
fn matching_profiles_support_language_country_year_and_rating_filters() {
    let settings = FilesystemSettings {
        mount_path: "/mount".to_string(),
        library_profiles: HashMap::from([(
            "curated".to_string(),
            FilesystemLibraryProfile {
                name: "Curated".to_string(),
                library_path: "/curated".to_string(),
                enabled: true,
                exclusive: false,
                filter_rules: FilesystemFilterRules {
                    content_types: vec![FilesystemContentType::Movie],
                    genres: vec![],
                    networks: vec!["netflix".to_string(), "!fox".to_string()],
                    languages: vec!["en".to_string(), "!jp".to_string()],
                    countries: vec!["us".to_string()],
                    content_ratings: vec![],
                    min_year: Some(2000),
                    max_year: Some(2020),
                    min_rating: Some(7.0),
                    max_rating: Some(8.5),
                    is_anime: Some(false),
                },
            },
        )]),
    };

    let metadata = FilesystemItemMetadata {
        genres: vec!["thriller".to_string()],
        network: Some("Netflix".to_string()),
        content_rating: None,
        language: Some("EN".to_string()),
        country: Some("us".to_string()),
        year: Some(2010),
        rating: Some(7.8),
        is_anime: false,
    };

    assert_eq!(
        settings.matching_profile_keys(&metadata, FilesystemContentType::Movie),
        LibraryProfileMembership(vec!["curated".to_string()])
    );
}

#[test]
fn apply_general_db_override_updates_supported_fields() {
    let mut settings = RivenSettings {
        vfs_mount_path: "/vfs".to_string(),
        minimum_average_bitrate_movies: Some(10),
        retry_interval_secs: 60,
        ..RivenSettings::default()
    };

    settings.apply_general_db_override(&serde_json::json!({
        "filesystem": {
            "mount_path": "",
            "library_profiles": {
                "kids": {
                    "name": "Kids",
                    "library_path": "/kids",
                    "enabled": true,
                    "filter_rules": {}
                }
            }
        },
        "dubbed_anime_only": true,
        "minimum_average_bitrate_movies": 15,
        "maximum_average_bitrate_episodes": 20,
        "retry_interval_secs": 3600,
        "schedule_offset_minutes": 45,
        "unknown_air_date_offset_days": 3
    }));

    assert_eq!(settings.filesystem.mount_path, "/vfs");
    assert!(settings.filesystem.library_profiles.contains_key("kids"));
    assert!(settings.dubbed_anime_only);
    assert_eq!(settings.minimum_average_bitrate_movies, Some(15));
    assert_eq!(settings.maximum_average_bitrate_episodes, Some(20));
    assert_eq!(settings.retry_interval_secs, 3600);
    assert_eq!(settings.schedule_offset_minutes, 45);
    assert_eq!(settings.unknown_air_date_offset_days, 3);
}

#[test]
fn apply_general_db_override_ignores_invalid_payloads() {
    let original = RivenSettings::default();
    let mut settings = original.clone();

    settings.apply_general_db_override(&serde_json::json!("invalid"));

    assert_eq!(settings.database_url, original.database_url);
    assert_eq!(settings.filesystem, original.filesystem);
    assert_eq!(settings.retry_interval_secs, original.retry_interval_secs);
}

#[test]
fn plugin_settings_getters_normalize_keys_and_trim_values() {
    let settings = plugin_settings(&[
        ("api_key", "  secret-token  "),
        ("empty_value", "   "),
        ("feature_enabled", "YeS"),
        ("timeout_secs", "45"),
    ]);

    assert_eq!(settings.get("API_KEY"), Some("secret-token"));
    assert_eq!(settings.get("empty_value"), None);
    assert!(settings.get_bool("feature_enabled"));
    assert_eq!(settings.get_parsed::<u32>("timeout_secs"), Some(45));
    assert_eq!(settings.get_or("missing", "fallback"), "fallback");
    assert_eq!(settings.get_parsed_or("missing", 12_u32), 12);
    assert_eq!(settings.prefix(), "TEST");
    assert!(settings.has("api_key"));
}

#[test]
fn plugin_settings_get_list_supports_json_and_csv() {
    let settings = plugin_settings(&[
        ("json_values", r#"["one","two"]"#),
        ("csv_values", "alpha, beta ,gamma"),
    ]);

    assert_eq!(
        settings.get_list("json_values"),
        vec!["one".to_string(), "two".to_string()]
    );
    assert_eq!(
        settings.get_list("csv_values"),
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
    );
}

#[test]
fn plugin_settings_merge_db_override_overrides_and_serializes_supported_values() {
    let mut settings = plugin_settings(&[("api_key", "env-value"), ("keep", "original")]);

    settings.merge_db_override(&serde_json::json!({
        "api_key": "db-value",
        "enabled": true,
        "retries": 3,
        "providers": ["a", "b"],
        "ignored_empty": "",
        "ignored_null": null
    }));

    assert_eq!(settings.get("api_key"), Some("db-value"));
    assert!(settings.get_bool("enabled"));
    assert_eq!(settings.get_parsed::<u32>("retries"), Some(3));
    assert_eq!(
        settings.get_list("providers"),
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(settings.get("keep"), Some("original"));
    assert_eq!(
        settings.to_json(),
        serde_json::json!({
            "api_key": "db-value",
            "enabled": "true",
            "keep": "original",
            "providers": "[\"a\",\"b\"]",
            "retries": "3"
        })
    );
}

#[test]
fn plugin_settings_reports_empty_only_when_no_values_exist() {
    let empty = plugin_settings(&[]);
    let whitespace_only = plugin_settings(&[("blank", "   ")]);

    assert!(empty.is_empty());
    assert!(!whitespace_only.has("blank"));
    assert!(!whitespace_only.is_empty());
    assert!(!whitespace_only.has_effective_values());
}

#[test]
fn plugin_settings_reports_effective_values_when_any_value_is_usable() {
    let settings = plugin_settings(&[("blank", "   "), ("api_key", " secret ")]);

    assert!(settings.has_effective_values());
}
