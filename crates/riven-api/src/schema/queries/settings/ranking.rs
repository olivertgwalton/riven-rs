use super::*;

pub(super) fn effective_profile_settings(
    profile: riven_rank::QualityProfile,
    stored: Option<&serde_json::Value>,
) -> serde_json::Value {
    stored
        .filter(|value| {
            !value.is_null() && !value.as_object().is_some_and(serde_json::Map::is_empty)
        })
        .and_then(|value| {
            riven_queue::discovery::merge_builtin_profile_settings(profile, value).ok()
        })
        .and_then(|settings| serde_json::to_value(settings).ok())
        .unwrap_or_else(|| {
            serde_json::to_value(profile.base_settings()).unwrap_or(serde_json::Value::Null)
        })
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
pub(super) fn build_rank_settings_schema() -> Vec<SettingField> {
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
pub(super) fn strip_zero_ranks(json: &mut serde_json::Value) {
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
pub(super) fn inject_rank_defaults(json: &mut serde_json::Value) {
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
