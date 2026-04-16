use super::*;

#[test]
fn response_deserializes_external_id_field_names() {
    let response: ListrrResponse = serde_json::from_value(serde_json::json!({
        "totalPages": 2,
        "items": [
            {
                "imDbId": "tt123",
                "tvDbId": 456,
                "tmDbId": 789
            }
        ]
    }))
    .expect("listrr response should deserialize");

    assert_eq!(response.total_pages, Some(2));
    assert_eq!(response.items[0].imdb_id, Some("tt123".to_string()));
    assert_eq!(response.items[0].tvdb_id, Some(456));
    assert_eq!(response.items[0].tmdb_id, Some(789));
}

#[test]
fn plugin_schema_marks_api_key_required() {
    let schema = ListrrPlugin.settings_schema();

    let api_key = schema
        .iter()
        .find(|field| field.key == "apikey")
        .expect("apikey field");
    assert!(api_key.required);
    assert_eq!(api_key.field_type, "password");
}
