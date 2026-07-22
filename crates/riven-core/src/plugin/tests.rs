use std::borrow::Cow;

use super::{ContentCollection, FieldType, SettingField};
use crate::events::HookResponse;
use crate::types::ExternalIds;

#[test]
fn setting_field_builder_populates_optional_metadata() {
    let field = SettingField::new("quality", "Quality", FieldType::Select)
        .required()
        .with_default("1080p")
        .with_placeholder("Choose quality")
        .with_description("Preferred quality")
        .with_options(&["720p", "1080p"])
        .with_fields(vec![SettingField::new("nested", "Nested", FieldType::Text)])
        .with_item_fields(vec![SettingField::new("item", "Item", FieldType::Text)])
        .with_key_placeholder("provider")
        .with_add_label("Add provider");

    assert!(field.required);
    assert_eq!(field.default_value.as_deref(), Some("1080p"));
    assert_eq!(field.placeholder.as_deref(), Some("Choose quality"));
    assert_eq!(field.description.as_deref(), Some("Preferred quality"));
    assert_eq!(
        field.options,
        Some(vec![Cow::Borrowed("720p"), Cow::Borrowed("1080p")])
    );
    assert_eq!(field.fields.as_ref().map(Vec::len), Some(1));
    assert_eq!(field.item_fields.as_ref().map(Vec::len), Some(1));
    assert_eq!(field.key_placeholder.as_deref(), Some("provider"));
    assert_eq!(field.add_label.as_deref(), Some("Add provider"));
}

#[test]
fn setting_field_builder_accepts_dynamic_options() {
    let field = SettingField::new("genre", "Genre", FieldType::FilterArray)
        .with_dynamic_options(vec!["Science Fiction".to_string(), "TV Movie".to_string()])
        .allow_custom_options();

    assert_eq!(
        field.options,
        Some(vec![
            Cow::Owned("Science Fiction".to_string()),
            Cow::Owned("TV Movie".to_string()),
        ])
    );
    assert_eq!(field.allow_custom_options, Some(true));
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
    // Colliding inserts merge rather than drop: the first-seen scalar field wins...
    assert_eq!(response.movies[0].tmdb_id.as_deref(), Some("10"));
    assert_eq!(response.shows[0].tvdb_id.as_deref(), Some("tv-1"));
}

#[test]
fn content_collection_merges_seasons_and_fills_missing_fields_on_collision() {
    let mut collection = ContentCollection::default();

    // e.g. two separate partial-season requests for the same show.
    collection.insert_show(ExternalIds {
        tvdb_id: Some("tv-1".to_string()),
        external_request_id: Some("req-1".to_string()),
        requested_seasons: Some(vec![1, 2, 3]),
        ..ExternalIds::default()
    });
    collection.insert_show(ExternalIds {
        tvdb_id: Some("tv-1".to_string()),
        requested_by: Some("later@example.test".to_string()),
        requested_seasons: Some(vec![3, 4]),
        ..ExternalIds::default()
    });

    assert_eq!(collection.show_count(), 1);
    let response = collection.into_response();
    let show = &response.shows[0];
    // ...seasons are unioned rather than one request's seasons being lost...
    assert_eq!(
        show.requested_seasons.as_deref(),
        Some([1, 2, 3, 4].as_slice())
    );
    // ...and a field missing from the first insert is filled in from the second.
    assert_eq!(show.external_request_id.as_deref(), Some("req-1"));
    assert_eq!(show.requested_by.as_deref(), Some("later@example.test"));
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
