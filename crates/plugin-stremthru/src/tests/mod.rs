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

#[test]
fn newz_dispatch_stores_excludes_debrid_stores() {
    // Debrid stores reject every `/v0/store/newz` call with a 400 — a
    // configured debrid API key must never end up in this list, even
    // alongside a configured `stremthruauth`.
    let stores = get_newz_dispatch_stores(&settings(&[
        ("realdebridapikey", "rd-token"),
        ("alldebridapikey", "ad-token"),
        ("stremthruauth", "st-token"),
    ]));

    assert_eq!(stores, vec![("stremthru", "st-token".to_string())]);
}

#[test]
fn newz_dispatch_stores_empty_without_stremthruauth() {
    // No stremthruauth means no newz-capable store at all — must not fall
    // back to trying debrid stores, which would just spend a wasted request
    // and ding their health score for an API they don't support.
    let stores = get_newz_dispatch_stores(&settings(&[
        ("realdebridapikey", "rd-token"),
        ("torboxapikey", "torbox-token"),
    ]));

    assert!(stores.is_empty());
}

#[test]
fn link_stores_include_both_debrid_and_stremthru() {
    // Unlike get_newz_dispatch_stores, this list backs stream-link
    // generation/refresh, which is always pinned/filtered to one specific
    // store by the caller — so it's fine (and necessary) for it to include
    // every possible originating store, newz-capable or not.
    let stores = get_link_stores(&settings(&[
        ("realdebridapikey", "rd-token"),
        ("stremthruauth", "st-token"),
    ]));

    assert_eq!(
        stores,
        vec![
            ("realdebrid", "rd-token".to_string()),
            ("stremthru", "st-token".to_string()),
        ]
    );
}
