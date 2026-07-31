use super::*;

#[test]
fn response_mapping_prefers_filename_and_normalizes_hash() {
    let resp: CometResponse = serde_json::from_value(serde_json::json!({
        "streams": [
            {
                "infoHash": "ABCDEF",
                "description": "🎬 ignored",
                "behaviorHints": { "filename": "Movie.File.2024.1080p.mkv" }
            },
            {
                "infoHash": "123456",
                "description": "📦 Fallback.Title.2024\nsecond line"
            },
            {
                "description": "📦 missing hash"
            }
        ]
    }))
    .expect("comet response should deserialize");

    let results = scrape_results_from_response(resp);

    assert_eq!(
        results.get("abcdef").map(|e| e.title.as_str()),
        Some("Movie.File.2024.1080p.mkv")
    );
    assert_eq!(
        results.get("123456").map(|e| e.title.as_str()),
        Some("Fallback.Title.2024")
    );
    assert_eq!(results.len(), 2);
}
