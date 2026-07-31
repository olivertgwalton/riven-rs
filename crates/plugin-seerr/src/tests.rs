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
fn multiple_partial_season_requests_for_same_show_are_merged_not_dropped() {
    let response: SeerrRequestResponse = serde_json::from_value(serde_json::json!({
        "results": [
            {
                "id": 1,
                "type": "tv",
                "media": { "id": 500, "tvdbId": 1234 },
                "seasons": [{ "seasonNumber": 1 }, { "seasonNumber": 2 }, { "seasonNumber": 3 }]
            },
            {
                "id": 2,
                "type": "tv",
                "media": { "id": 500, "tvdbId": 1234 },
                "seasons": [{ "seasonNumber": 4 }]
            }
        ]
    }))
    .expect("seerr response should deserialize");

    let mut content = ContentCollection::default();
    for request in &response.results {
        let media = request.media.as_ref().expect("media");
        let seasons: Vec<i32> = request
            .seasons
            .as_ref()
            .into_iter()
            .flatten()
            .filter_map(|s| s.season_number)
            .collect();
        let external_request_id = media
            .id
            .map(|id| id.to_string())
            .unwrap_or_else(|| request.id.to_string());
        content.insert_show(ExternalIds {
            tvdb_id: media.tvdb_id.map(|id| id.to_string()),
            external_request_id: Some(external_request_id),
            requested_seasons: Some(seasons),
            ..Default::default()
        });
    }

    assert_eq!(content.show_count(), 1);
    let show = &content.into_response().shows[0];
    assert_eq!(show.external_request_id.as_deref(), Some("500"));
    assert_eq!(
        show.requested_seasons.as_deref(),
        Some([1, 2, 3, 4].as_slice())
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
    assert_eq!(url.default_value.as_deref(), Some(DEFAULT_URL));
    assert_eq!(filter.default_value.as_deref(), Some(DEFAULT_FILTER));
}
