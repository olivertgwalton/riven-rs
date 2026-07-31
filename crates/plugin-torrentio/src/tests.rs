use super::*;

#[test]
fn deferred_statuses_cover_torrentio_overload_responses() {
    assert!(is_deferred_status(StatusCode::TOO_MANY_REQUESTS));
    assert!(is_deferred_status(StatusCode::BAD_GATEWAY));
    assert!(is_deferred_status(StatusCode::SERVICE_UNAVAILABLE));
    assert!(is_deferred_status(StatusCode::GATEWAY_TIMEOUT));
    assert!(!is_deferred_status(StatusCode::NOT_FOUND));
}

#[test]
fn response_mapping_uses_first_title_line_before_peer_count() {
    let resp: TorrentioResponse = serde_json::from_value(serde_json::json!({
        "streams": [
            {
                "infoHash": "ABCDEF",
                "title": "Movie.File.2024.1080p 👤 22\nsecond line"
            },
            {
                "infoHash": "123456",
                "title": "   "
            },
            {
                "title": "missing hash"
            }
        ]
    }))
    .expect("torrentio response should deserialize");

    let results = scrape_results_from_response(resp);

    assert_eq!(results.len(), 1);
    let entry = results
        .get("abcdef")
        .expect("lowercased info hash should be present");
    assert_eq!(entry.title, "Movie.File.2024.1080p");
    assert_eq!(entry.file_size_bytes, None);
}
