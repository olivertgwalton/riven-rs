use super::*;

#[test]
fn request_response_deserializes_media_and_requested_seasons() {
    let response: SeerrRequestResponse = serde_json::from_value(serde_json::json!({
        "results": [
            {
                "id": 99,
                "type": "tv",
                "media": { "tvdbId": 1234 },
                "requestedBy": { "email": "user@example.test" },
                "seasons": [
                    { "seasonNumber": 1 },
                    { "seasonNumber": 2 }
                ]
            }
        ]
    }))
    .expect("seerr response should deserialize");

    let request = &response.results[0];
    assert_eq!(request.id, 99);
    assert_eq!(request.media_type.as_deref(), Some("tv"));
    assert_eq!(
        request.media.as_ref().and_then(|media| media.tvdb_id),
        Some(1234)
    );
    assert_eq!(
        request
            .requested_by
            .as_ref()
            .and_then(|user| user.email.as_deref()),
        Some("user@example.test")
    );
    assert_eq!(
        request
            .seasons
            .as_ref()
            .expect("seasons")
            .iter()
            .filter_map(|season| season.season_number)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn plugin_schema_declares_default_url_and_filter() {
    let schema = SeerrPlugin.settings_schema();

    let url = schema
        .iter()
        .find(|field| field.key == "url")
        .expect("url field");
    let filter = schema
        .iter()
        .find(|field| field.key == "filter")
        .expect("filter field");
    assert_eq!(url.default_value, Some(DEFAULT_URL));
    assert_eq!(filter.default_value, Some(DEFAULT_FILTER));
}
