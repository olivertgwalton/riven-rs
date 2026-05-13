use chrono::{NaiveDate, Utc};
use riven_core::types::{FileSystemEntryType, MediaItemState, MediaItemType};
use riven_db::entities::{FileSystemEntry, MediaItem};
use serde_json::json;

fn sample_movie(title: &str, year: Option<i32>, tmdb_id: Option<&str>) -> MediaItem {
    MediaItem {
        id: 1,
        title: title.into(),
        full_title: None,
        imdb_id: None,
        tvdb_id: None,
        tmdb_id: tmdb_id.map(str::to_string),
        poster_path: None,
        created_at: Utc::now(),
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
        aired_at_utc: None,
        network_timezone: None,
        year,
        genres: None,
        rating: None,
        content_rating: None,
        state: MediaItemState::Indexed,
        failed_attempts: 0,
        last_scrape_attempt_at: None,
        item_type: MediaItemType::Movie,
        is_requested: true,
        show_status: None,
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

fn sample_entry(path: &str, original_filename: Option<&str>) -> FileSystemEntry {
    FileSystemEntry {
        id: 1,
        file_size: 0,
        created_at: Utc::now(),
        updated_at: None,
        media_item_id: 1,
        entry_type: FileSystemEntryType::Media,
        path: path.into(),
        original_filename: original_filename.map(str::to_string),
        download_url: None,
        stream_url: None,
        plugin: None,
        provider: None,
        provider_download_id: None,
        source_provider: None,
        source_id: None,
        library_profiles: None,
        media_metadata: None,
        language: None,
        parent_original_filename: None,
        subtitle_content: None,
        file_hash: None,
        video_file_size: None,
        opensubtitles_id: None,
        stream_id: None,
        resolution: None,
        ranking_profile_name: None,
    }
}

#[test]
fn pretty_name_formats_movie_with_tmdb() {
    let m = sample_movie("Inception", Some(2010), Some("27205"));
    assert_eq!(m.pretty_name(), "Inception (2010) {tmdb-27205}");
}

#[test]
fn pretty_name_omits_missing_year_and_id() {
    let m = sample_movie("Untitled", None, None);
    assert_eq!(m.pretty_name(), "Untitled");
}

#[test]
fn pretty_name_show_uses_tvdb_id() {
    let mut m = sample_movie("Breaking Bad", Some(2008), None);
    m.item_type = MediaItemType::Show;
    m.tvdb_id = Some("81189".into());
    assert_eq!(m.pretty_name(), "Breaking Bad (2008) {tvdb-81189}");
}

#[test]
fn filesystem_metadata_lowercases_genres_and_drops_non_strings() {
    let mut m = sample_movie("Movie", Some(2020), None);
    m.genres = Some(json!(["Action", "THRILLER", 42, "Drama"]));
    assert_eq!(
        m.filesystem_metadata().genres,
        vec!["action", "thriller", "drama"]
    );
}

#[test]
fn filesystem_metadata_handles_missing_or_non_array_genres() {
    let mut m = sample_movie("Movie", Some(2020), None);
    assert!(m.filesystem_metadata().genres.is_empty());
    m.genres = Some(json!("not an array"));
    assert!(m.filesystem_metadata().genres.is_empty());
}

#[test]
fn base_directory_picks_movies_or_shows() {
    assert_eq!(
        sample_entry("/movies/a.mkv", None).base_directory(),
        "movies"
    );
    assert_eq!(
        sample_entry("/shows/s01e01.mkv", None).base_directory(),
        "shows"
    );
    assert_eq!(
        sample_entry("/anything/else", None).base_directory(),
        "shows"
    );
}

#[test]
fn vfs_filename_uses_original_extension_or_default() {
    let e = sample_entry("/movies/a", Some("movie.file.mp4"));
    assert_eq!(e.vfs_filename("Movie (2020)"), "Movie (2020).mp4");
    let e = sample_entry("/movies/a", None);
    assert_eq!(e.vfs_filename("Movie (2020)"), "Movie (2020).mkv");
}

#[test]
fn filesystem_metadata_derives_year_from_aired_at() {
    let mut m = sample_movie("Movie", None, None);
    m.aired_at = Some(NaiveDate::from_ymd_opt(2015, 6, 1).unwrap());
    assert_eq!(m.filesystem_metadata().year, Some(2015));
}
