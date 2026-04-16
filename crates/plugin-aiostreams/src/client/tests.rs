use super::*;

fn request(
    item_type: MediaItemType,
    season: Option<i32>,
    episode: Option<i32>,
) -> ScrapeRequest<'static> {
    ScrapeRequest {
        id: 1,
        item_type,
        imdb_id: Some("tt0944947"),
        title: "Game of Thrones",
        season,
        episode,
    }
}

#[test]
fn request_identifier_matches_aiostreams_search_ids() {
    assert_eq!(
        request_identifier(&request(MediaItemType::Movie, None, None)),
        Some("tt0944947".to_string())
    );
    assert_eq!(
        request_identifier(&request(MediaItemType::Show, None, None)),
        Some("tt0944947:1:1".to_string())
    );
    assert_eq!(
        request_identifier(&request(MediaItemType::Season, Some(3), None)),
        Some("tt0944947:3".to_string())
    );
    assert_eq!(
        request_identifier(&request(MediaItemType::Episode, Some(3), Some(9))),
        Some("tt0944947:3:9".to_string())
    );
}

#[test]
fn request_identifier_returns_none_without_imdb_id() {
    let mut req = request(MediaItemType::Movie, None, None);
    req.imdb_id = None;

    assert_eq!(request_identifier(&req), None);
}

#[test]
fn first_description_line_strips_leading_symbols() {
    assert_eq!(
        first_description_line("🎬 Movie.File.2024\nsecond line".to_string()),
        Some("Movie.File.2024".to_string())
    );
    assert_eq!(first_description_line("!!!\nsecond line".to_string()), None);
}
