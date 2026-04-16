use super::*;

#[test]
fn settings_schema_has_expected_select_options() {
    let schema = LogsPlugin.settings_schema();

    let level = schema
        .iter()
        .find(|field| field.key == "log_level")
        .expect("log_level field");
    assert_eq!(level.default_value, Some("info"));
    assert_eq!(
        level.options.as_ref().expect("level options"),
        &vec!["error", "warn", "info", "debug", "trace"]
    );

    let rotation = schema
        .iter()
        .find(|field| field.key == "log_rotation")
        .expect("log_rotation field");
    assert_eq!(rotation.default_value, Some("hourly"));
    assert_eq!(
        rotation.options.as_ref().expect("rotation options"),
        &vec!["hourly", "daily"]
    );
}
