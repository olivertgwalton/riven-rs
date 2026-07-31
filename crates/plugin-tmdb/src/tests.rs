use super::*;

#[test]
fn content_rating_prefers_first_valid_us_certification() {
    let release_dates = TmdbReleaseDates {
        results: vec![
            TmdbReleaseDateCountry {
                iso_3166_1: "GB".to_string(),
                release_dates: vec![TmdbReleaseDateResult {
                    certification: "15".to_string(),
                }],
            },
            TmdbReleaseDateCountry {
                iso_3166_1: "US".to_string(),
                release_dates: vec![
                    TmdbReleaseDateResult {
                        certification: " ".to_string(),
                    },
                    TmdbReleaseDateResult {
                        certification: "PG-13".to_string(),
                    },
                ],
            },
        ],
    };

    assert_eq!(
        parse_content_rating_from_release_dates(&release_dates),
        Some(ContentRating::Pg13)
    );
}

#[test]
fn content_rating_falls_back_to_non_us_valid_certification() {
    let release_dates = TmdbReleaseDates {
        results: vec![TmdbReleaseDateCountry {
            iso_3166_1: "CA".to_string(),
            release_dates: vec![TmdbReleaseDateResult {
                certification: " TV-MA ".to_string(),
            }],
        }],
    };

    assert_eq!(
        parse_content_rating_from_release_dates(&release_dates),
        Some(ContentRating::TvMa)
    );
}

#[test]
fn content_rating_rejects_unknown_values() {
    assert_eq!(parse_content_rating("BBFC-15"), None);
}
