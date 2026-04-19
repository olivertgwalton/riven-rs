use super::rotten_tomatoes::{RTAlgoliaHit, RTRatings, best_match, encode_query_value};
use super::util::required_media_type;

#[test]
fn media_type_parsing_is_case_insensitive() {
    assert_eq!(required_media_type(Some("Movie")).unwrap(), "movie");
    assert_eq!(required_media_type(Some(" TV ")).unwrap(), "tv");
    assert!(required_media_type(Some("episode")).is_err());
    assert!(required_media_type(None).is_err());
}

#[test]
fn rt_query_encoding_preserves_algolia_params_shape() {
    let encoded = encode_query_value("isEmsSearchable=1 AND type:\"movie\"");
    assert_eq!(encoded, "isEmsSearchable%3D1%20AND%20type%3A%22movie%22");
}

#[test]
fn rotten_tomatoes_matching_prefers_close_title_and_year() {
    let hits = vec![
        RTAlgoliaHit {
            title: "Unrelated Movie".to_owned(),
            titles: None,
            release_year: 2024,
            vanity: "unrelated_movie".to_owned(),
            aka: None,
            rotten_tomatoes: Some(RTRatings {
                audience_score: 80,
                certified_fresh: false,
                critics_score: 80,
            }),
        },
        RTAlgoliaHit {
            title: "The Matrix".to_owned(),
            titles: None,
            release_year: 1999,
            vanity: "matrix".to_owned(),
            aka: Some(vec!["Matrix".to_owned()]),
            rotten_tomatoes: Some(RTRatings {
                audience_score: 85,
                certified_fresh: true,
                critics_score: 88,
            }),
        },
    ];

    let best = best_match(&hits, "Matrix", Some(1999)).unwrap();
    assert_eq!(best.vanity, "matrix");
}

#[test]
fn rt_scores_use_expected_badges() {
    let hit = RTAlgoliaHit {
        title: "The Matrix".to_owned(),
        titles: None,
        release_year: 1999,
        vanity: "matrix".to_owned(),
        aka: None,
        rotten_tomatoes: Some(RTRatings {
            audience_score: 59,
            certified_fresh: true,
            critics_score: 88,
        }),
    };

    let scores = hit.scores("movie").unwrap();
    assert_eq!(scores[0].name, "rt_tomatometer_certified_fresh");
    assert_eq!(scores[1].name, "rt_popcornmeter_stale");
}
