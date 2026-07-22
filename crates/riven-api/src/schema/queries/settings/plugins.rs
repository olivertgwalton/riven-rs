use super::*;

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
