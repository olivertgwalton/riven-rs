use super::*;
use chrono::{TimeZone, Utc};
use riven_core::types::{MediaItemState, ShowStatus};

fn media_item(item_type: MediaItemType, title: &str) -> MediaItem {
    MediaItem {
        id: 42,
        title: title.to_string(),
        full_title: None,
        imdb_id: None,
        tvdb_id: None,
        tmdb_id: None,
        poster_path: None,
        created_at: Utc.timestamp_opt(0, 0).single().expect("valid timestamp"),
        updated_at: None,
        indexed_at: None,
        scraped_at: None,
        scraped_times: 0,
        aliases: None,
        network: None,
        country: None,
        language: None,
        is_anime: false,
        aired_at: None,
        year: None,
        genres: None,
        rating: None,
        content_rating: None,
        state: MediaItemState::Unreleased,
        failed_attempts: 0,
        item_type,
        is_requested: false,
        show_status: None::<ShowStatus>,
        season_number: None,
        is_special: None,
        parent_id: None,
        episode_number: None,
        absolute_number: None,
        runtime: None,
        item_request_id: None,
        active_stream_id: None,
    }
}

#[test]
fn summaries_include_movie_year_and_episode_context() {
    let mut movie = media_item(MediaItemType::Movie, "Dune");
    movie.year = Some(2021);
    assert_eq!(build_summary(&movie), "Dune (2021)");

    let mut episode = media_item(MediaItemType::Episode, "The Beginning");
    episode.full_title = Some("Example Show".to_string());
    episode.season_number = Some(2);
    episode.episode_number = Some(3);
    assert_eq!(
        build_summary(&episode),
        "Example Show - S02E03: The Beginning"
    );
}

#[test]
fn ical_text_is_escaped_and_folded() {
    assert_eq!(
        escape_text("Title, with; chars\\and\nline\r"),
        "Title\\, with\\; chars\\\\and\\nline"
    );

    let folded = fold_line(&format!("SUMMARY:{}", "a".repeat(80)));
    assert!(folded.contains("\r\n "));
    assert!(folded.ends_with("\r\n"));
}

#[test]
fn build_ical_skips_items_without_air_date() {
    let missing = media_item(MediaItemType::Movie, "No Date");
    let mut dated = media_item(MediaItemType::Movie, "With Date");
    dated.aired_at = chrono::NaiveDate::from_ymd_opt(2026, 4, 16);

    let ical = build_ical(&[missing, dated]);

    assert!(ical.contains("BEGIN:VCALENDAR\r\n"));
    assert!(ical.contains("SUMMARY:With Date\r\n"));
    assert!(!ical.contains("No Date"));
}
