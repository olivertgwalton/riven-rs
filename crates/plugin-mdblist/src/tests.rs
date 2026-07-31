use super::*;

#[test]
fn normalize_list_name_accepts_slugs_and_common_urls() {
    assert_eq!(
        normalize_list_name("owner/list-name"),
        Some("owner/list-name".to_string())
    );
    assert_eq!(
        normalize_list_name("https://mdblist.com/lists/owner/list-name/"),
        Some("owner/list-name".to_string())
    );
    assert_eq!(
        normalize_list_name("https://mdblist.com/owner/list-name"),
        Some("owner/list-name".to_string())
    );
    assert_eq!(
        normalize_list_name("https://mdblist.com/single"),
        Some("single".to_string())
    );
    assert_eq!(normalize_list_name("   "), None);
}

#[test]
fn list_items_resolve_ids_from_either_nested_or_top_level() {
    let body = r#"{
        "movies": [
            {"id": 1, "ids": {"imdb": "tt-movie", "tmdb": 100, "tvdb": null}}
        ],
        "shows": [
            {"id": 2, "ids": {"imdb": "tt-nested", "tmdb": 200, "tvdb": 4000}},
            {"id": 3, "imdb_id": "tt-toplevel", "tvdb_id": 5000}
        ]
    }"#;

    let resp: MdblistListItemsResponse = serde_json::from_str(body).unwrap();
    let movies = resp.movies.unwrap();
    let shows = resp.shows.unwrap();

    assert_eq!(movies[0].imdb_id().as_deref(), Some("tt-movie"));
    assert_eq!(movies[0].tmdb_id(), Some(100));

    assert_eq!(shows[0].imdb_id().as_deref(), Some("tt-nested"));
    assert_eq!(shows[0].tvdb_id(), Some(4000));

    assert_eq!(shows[1].imdb_id().as_deref(), Some("tt-toplevel"));
    assert_eq!(shows[1].tvdb_id(), Some(5000));
}
