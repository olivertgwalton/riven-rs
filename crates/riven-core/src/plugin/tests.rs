use super::{ContentCollection, SettingField};
use crate::events::HookResponse;
use crate::types::ExternalIds;

#[test]
fn setting_field_builder_populates_optional_metadata() {
    let field = SettingField::new("quality", "Quality", "select")
        .required()
        .with_default("1080p")
        .with_placeholder("Choose quality")
        .with_description("Preferred quality")
        .with_options(&["720p", "1080p"])
        .with_fields(vec![SettingField::new("nested", "Nested", "text")])
        .with_item_fields(vec![SettingField::new("item", "Item", "text")])
        .with_key_placeholder("provider")
        .with_add_label("Add provider");

    assert!(field.required);
    assert_eq!(field.default_value, Some("1080p"));
    assert_eq!(field.placeholder, Some("Choose quality"));
    assert_eq!(field.description, Some("Preferred quality"));
    assert_eq!(field.options, Some(vec!["720p", "1080p"]));
    assert_eq!(field.fields.as_ref().map(Vec::len), Some(1));
    assert_eq!(field.item_fields.as_ref().map(Vec::len), Some(1));
    assert_eq!(field.key_placeholder, Some("provider"));
    assert_eq!(field.add_label, Some("Add provider"));
}

#[test]
fn content_collection_deduplicates_movies_and_shows_by_preferred_keys() {
    let mut collection = ContentCollection::default();

    collection.insert_movie(ExternalIds {
        imdb_id: Some("tt001".to_string()),
        tmdb_id: Some("10".to_string()),
        ..ExternalIds::default()
    });
    collection.insert_movie(ExternalIds {
        imdb_id: Some("tt001".to_string()),
        tmdb_id: Some("99".to_string()),
        ..ExternalIds::default()
    });
    collection.insert_show(ExternalIds {
        imdb_id: None,
        tvdb_id: Some("tv-1".to_string()),
        ..ExternalIds::default()
    });
    collection.insert_show(ExternalIds {
        imdb_id: None,
        tvdb_id: Some("tv-1".to_string()),
        ..ExternalIds::default()
    });

    assert_eq!(collection.movie_count(), 1);
    assert_eq!(collection.show_count(), 1);

    let response = collection.into_response();
    assert_eq!(response.movies.len(), 1);
    assert_eq!(response.shows.len(), 1);
    assert_eq!(response.movies[0].imdb_id.as_deref(), Some("tt001"));
    assert_eq!(response.shows[0].tvdb_id.as_deref(), Some("tv-1"));
}

#[test]
fn content_collection_can_be_converted_to_hook_response() {
    let mut collection = ContentCollection::default();
    collection.insert_movie(ExternalIds {
        imdb_id: Some("tt123".to_string()),
        ..ExternalIds::default()
    });

    let response = collection.into_hook_response();

    match response {
        HookResponse::ContentService(payload) => {
            assert_eq!(payload.movies.len(), 1);
            assert_eq!(payload.movies[0].imdb_id.as_deref(), Some("tt123"));
            assert!(payload.shows.is_empty());
        }
        other => panic!("expected content-service response, got {other:?}"),
    }
}
