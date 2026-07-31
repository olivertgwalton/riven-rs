use super::*;

fn payload() -> NotificationPayload {
    NotificationPayload {
        event: "riven.media-item.download.success".to_string(),
        title: "Movie".to_string(),
        full_title: "Movie".to_string(),
        item_type: MediaItemType::Movie,
        year: Some(2024),
        imdb_id: Some("tt123".to_string()),
        tmdb_id: Some("456".to_string()),
        tvdb_id: None,
        poster_path: Some("https://image.test/poster.jpg".to_string()),
        downloader: "stremthru".to_string(),
        provider: Some("realdebrid".to_string()),
        duration_seconds: 3661.2,
        timestamp: "2026-04-16T12:00:00Z".to_string(),
        is_anime: false,
        rating: Some(8.25),
        overview: Some("Short overview".to_string()),
        tvdb_slug: None,
    }
}

#[test]
fn notification_url_parser_supports_discord_and_json_aliases() {
    match parse_notification_url("discord://id/token") {
        Some(NotificationService::Discord {
            webhook_id,
            webhook_token,
        }) => {
            assert_eq!(webhook_id, "id");
            assert_eq!(webhook_token, "token");
        }
        _ => panic!("expected discord URL"),
    }

    match parse_notification_url("jsons://hooks.example/path") {
        Some(NotificationService::Json { url }) => {
            assert_eq!(url, "https://hooks.example/path");
        }
        _ => panic!("expected json URL"),
    }

    assert!(parse_notification_url("ftp://example.test").is_none());
}

#[test]
fn duration_formatter_uses_human_units() {
    assert_eq!(format_duration(12.4), "12.4s");
    assert_eq!(format_duration(125.0), "2m 5s");
    assert_eq!(format_duration(3661.0), "1h 1m 1s");
    assert_eq!(format_duration(-1.0), "-1.0s");
}

#[test]
fn simple_embed_contains_core_download_fields() {
    let body = build_simple_embed(&payload());
    let embed = &body["embeds"][0];

    assert_eq!(embed["title"], "Downloaded: Movie");
    assert_eq!(embed["thumbnail"]["url"], "https://image.test/poster.jpg");
    assert!(
        embed["fields"]
            .as_array()
            .expect("fields array")
            .iter()
            .any(|field| field["name"] == "Provider" && field["value"] == "realdebrid")
    );
}
