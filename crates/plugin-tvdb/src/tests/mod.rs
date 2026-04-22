use super::*;

#[test]
fn extract_english_name_prefers_english_translation() {
    let series = TvdbSeries {
        name: Some("Original".to_string()),
        image: None,
        year: None,
        first_aired: None,
        original_language: None,
        original_country: None,
        country: None,
        original_network: None,
        genres: None,
        status: None,
        aliases: None,
        remote_ids: None,
        content_ratings: None,
        airs_time: None,
        translations: Some(TvdbTranslations {
            name_translations: Some(vec![
                TvdbTranslation {
                    language: "fra".to_string(),
                    name: "Nom".to_string(),
                },
                TvdbTranslation {
                    language: "eng".to_string(),
                    name: "English Name".to_string(),
                },
            ]),
        }),
    };

    assert_eq!(
        extract_english_name(&series),
        Some("English Name".to_string())
    );
}

#[test]
fn parse_content_rating_maps_tvdb_certifications() {
    assert_eq!(parse_content_rating("TV-14"), Some(ContentRating::Tv14));
    assert_eq!(parse_content_rating("PG"), Some(ContentRating::Pg));
    assert_eq!(parse_content_rating("Unrated"), None);
}

#[test]
fn runtime_deserializer_accepts_integer_float_and_missing_values() {
    let integer: TvdbEpisode =
        serde_json::from_value(serde_json::json!({ "runtime": 42 })).expect("integer runtime");
    let float: TvdbEpisode =
        serde_json::from_value(serde_json::json!({ "runtime": 22.5 })).expect("float runtime");
    let missing: TvdbEpisode =
        serde_json::from_value(serde_json::json!({})).expect("missing runtime");

    assert_eq!(integer.runtime, Some(42));
    assert_eq!(float.runtime, Some(22));
    assert_eq!(missing.runtime, None);
}
