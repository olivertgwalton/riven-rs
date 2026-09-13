use super::*;

fn ids(imdb: Option<&str>, tmdb: Option<i64>, tvdb: Option<i64>) -> TraktIds {
    TraktIds {
        imdb: imdb.map(str::to_string),
        tmdb,
        tvdb,
    }
}

fn direct(ids: TraktIds) -> TraktItem {
    TraktItem {
        ids: Some(ids),
        movie: None,
        show: None,
    }
}

#[test]
fn ids_to_external_rejects_empty_id_sets() {
    assert!(ids_to_external(&ids(None, None, None)).is_none());

    let external =
        ids_to_external(&ids(Some("tt123"), Some(456), Some(789))).expect("ids should convert");
    assert_eq!(external.imdb_id.as_deref(), Some("tt123"));
    assert_eq!(external.tmdb_id.as_deref(), Some("456"));
    assert_eq!(external.tvdb_id.as_deref(), Some("789"));
}

#[test]
fn collect_wrapped_deduplicates_by_content_collection_keys() {
    let mut content = ContentCollection::default();
    let items = vec![
        TraktItem {
            ids: None,
            movie: Some(Box::new(direct(ids(Some("tt123"), Some(456), None)))),
            show: None,
        },
        TraktItem {
            ids: None,
            movie: Some(Box::new(direct(ids(Some("tt123"), Some(999), None)))),
            show: None,
        },
        TraktItem {
            ids: None,
            movie: None,
            show: Some(Box::new(direct(ids(None, None, None)))),
        },
    ];

    collect(items, &mut content, true);

    assert_eq!(content.movie_count(), 1);
    assert_eq!(content.show_count(), 0);
}

#[test]
fn collect_direct_inserts_shows_when_requested() {
    let mut content = ContentCollection::default();
    let items = vec![direct(ids(None, None, Some(42)))];

    collect(items, &mut content, false);

    assert_eq!(content.movie_count(), 0);
    assert_eq!(content.show_count(), 1);
}
