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

#[test]
fn ntfy_url_parser_defaults_to_the_public_server() {
    match parse_notification_url("ntfy://mytopic") {
        Some(NotificationService::Ntfy {
            base_url,
            topic,
            auth: NtfyAuth::None,
            priority: None,
            tags: None,
        }) => {
            assert_eq!(base_url, "https://ntfy.sh");
            assert_eq!(topic, "mytopic");
        }
        _ => panic!("expected a public-server ntfy URL"),
    }

    // `ntfys://` is still just the public server when no host is given.
    match parse_notification_url("ntfys://mytopic") {
        Some(NotificationService::Ntfy { base_url, .. }) => {
            assert_eq!(base_url, "https://ntfy.sh");
        }
        _ => panic!("expected a public-server ntfy URL"),
    }
}

#[test]
fn ntfy_url_parser_supports_self_hosted_servers() {
    match parse_notification_url("ntfy://myhost.local/mytopic") {
        Some(NotificationService::Ntfy {
            base_url, topic, ..
        }) => {
            assert_eq!(base_url, "http://myhost.local");
            assert_eq!(topic, "mytopic");
        }
        _ => panic!("expected a self-hosted ntfy URL"),
    }

    match parse_notification_url("ntfys://myhost.local:8080/mytopic") {
        Some(NotificationService::Ntfy { base_url, .. }) => {
            assert_eq!(base_url, "https://myhost.local:8080");
        }
        _ => panic!("expected a secure self-hosted ntfy URL"),
    }

    // The public server has no plaintext endpoint, so an explicit host of
    // ntfy.sh is forced to https even under the plain `ntfy://` scheme.
    match parse_notification_url("ntfy://ntfy.sh/mytopic") {
        Some(NotificationService::Ntfy { base_url, .. }) => {
            assert_eq!(base_url, "https://ntfy.sh");
        }
        _ => panic!("expected ntfy.sh to be forced to https"),
    }
}

#[test]
fn ntfy_url_parser_supports_basic_and_token_auth() {
    match parse_notification_url("ntfy://user:pass@myhost/mytopic") {
        Some(NotificationService::Ntfy {
            auth: NtfyAuth::Basic { user, password },
            ..
        }) => {
            assert_eq!(user, "user");
            assert_eq!(password, "pass");
        }
        _ => panic!("expected basic auth"),
    }

    match parse_notification_url("ntfy://tk_abc123@ntfy.sh/mytopic") {
        Some(NotificationService::Ntfy {
            auth: NtfyAuth::Token(token),
            ..
        }) => {
            assert_eq!(token, "tk_abc123");
        }
        _ => panic!("expected token auth"),
    }
}

#[test]
fn ntfy_url_parser_reads_priority_and_tags() {
    match parse_notification_url("ntfy://mytopic?priority=high&tags=warning,skull") {
        Some(NotificationService::Ntfy { priority, tags, .. }) => {
            assert_eq!(priority.as_deref(), Some("high"));
            assert_eq!(tags.as_deref(), Some("warning,skull"));
        }
        _ => panic!("expected priority and tags to be parsed"),
    }

    // An unrecognized priority is dropped rather than passed through blindly.
    match parse_notification_url("ntfy://mytopic?priority=bogus") {
        Some(NotificationService::Ntfy { priority, .. }) => assert_eq!(priority, None),
        _ => panic!("expected an ntfy URL"),
    }
}

#[test]
fn ntfy_url_parser_rejects_a_missing_topic() {
    assert!(parse_notification_url("ntfy://").is_none());
    assert!(parse_notification_url("ntfy://myhost/").is_none());
}

#[test]
fn ntfy_body_condenses_the_core_fields_and_attaches_the_poster() {
    let body = build_ntfy_body(&payload());

    assert_eq!(body["title"], "Downloaded: Movie");
    assert_eq!(
        body["message"],
        "Movie • 2024 • via stremthru • realdebrid • in 1h 1m 1s"
    );
    assert_eq!(body["attach"], "https://image.test/poster.jpg");
}
