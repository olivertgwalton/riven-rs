use super::*;

#[test]
fn scrape_url_defaults_series_season_and_episode() {
    assert_eq!(
        scrape_url("sort=quality", MediaItemType::Episode, "tt123", None, None),
        "http://torrentio.strem.fun/sort=quality/stream/series/tt123:1:1.json"
    );
    assert_eq!(
        scrape_url(
            "sort=quality",
            MediaItemType::Movie,
            "tt123",
            Some(2),
            Some(3)
        ),
        "http://torrentio.strem.fun/sort=quality/stream/movie/tt123.json"
    );
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

    assert_eq!(
        results,
        HashMap::from([("abcdef".to_string(), "Movie.File.2024.1080p".to_string())])
    );
}
