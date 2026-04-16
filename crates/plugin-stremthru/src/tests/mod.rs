use super::*;

fn settings(values: &[(&str, &str)]) -> PluginSettings {
    let mut settings = PluginSettings::load("STREMTHRU_TEST");
    let value = serde_json::Value::Object(
        values
            .iter()
            .map(|(key, value)| {
                (
                    (*key).to_string(),
                    serde_json::Value::String((*value).to_string()),
                )
            })
            .collect(),
    );
    settings.merge_db_override(&value);
    settings
}

#[test]
fn configured_stores_preserve_declared_store_order() {
    let stores = get_configured_stores(&settings(&[
        ("torboxapikey", "torbox-token"),
        ("realdebridapikey", "rd-token"),
        ("unknownapikey", "ignored"),
    ]));

    assert_eq!(
        stores,
        vec![
            ("realdebrid", "rd-token".to_string()),
            ("torbox", "torbox-token".to_string())
        ]
    );
}

#[test]
fn configured_stores_ignore_blank_values() {
    let stores = get_configured_stores(&settings(&[("realdebridapikey", "  ")]));

    assert!(stores.is_empty());
}

#[test]
fn store_score_key_is_namespaced_by_store() {
    assert_eq!(
        store_score_key("realdebrid"),
        "plugin:stremthru:store-score:realdebrid"
    );
}
