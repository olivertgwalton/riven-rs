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
    assert_eq!(ContentRating::parse("TV-14"), Some(ContentRating::Tv14));
    assert_eq!(ContentRating::parse("PG"), Some(ContentRating::Pg));
    assert_eq!(ContentRating::parse("Unrated"), None);
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

#[test]
fn scene_map_applies_thexem_numbering() {
    let resp: XemResponse = serde_json::from_str(
        r#"{"result":"success","data":[
            {"scene":{"season":8,"episode":5,"absolute":97},"tvdb":{"season":7,"episode":5,"absolute":97}}
        ]}"#,
    )
    .unwrap();
    let map = resp
        .data
        .into_iter()
        .map(|m| {
            (
                (m.tvdb.season, m.tvdb.episode),
                (m.scene.season, m.scene.episode),
            )
        })
        .collect();
    let mut indexed = IndexedMediaItem {
        seasons: Some(vec![IndexedSeason {
            number: 7,
            episodes: vec![
                IndexedEpisode {
                    number: 5,
                    ..Default::default()
                },
                IndexedEpisode {
                    number: 6,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }]),
        ..Default::default()
    };
    apply_scene_map(&mut indexed, &map);
    let eps = &indexed.seasons.unwrap()[0].episodes;
    assert_eq!(eps[0].scene, Some((8, 5)));
    assert_eq!(eps[1].scene, None);

    let none: XemResponse = serde_json::from_str(
        r#"{"result":"failure","data":[],"message":"no show with the tvdb_id 1 found"}"#,
    )
    .unwrap();
    assert!(none.data.is_empty());
}
