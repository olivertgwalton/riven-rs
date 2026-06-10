use super::*;
use crate::models::{StremthruFile, StremthruTorz};

#[test]
fn download_result_prefers_file_path_and_clamps_negative_sizes() {
    let torz = StremthruTorz {
        id: "torz-1".to_string(),
        status: "downloaded".to_string(),
        files: vec![
            StremthruFile {
                name: "file.mkv".to_string(),
                path: "Season 01/file.mkv".to_string(),
                size: 1024,
                link: "https://example.test/file.mkv".to_string(),
            },
            StremthruFile {
                name: "broken.mkv".to_string(),
                path: String::new(),
                size: -1,
                link: String::new(),
            },
        ],
    };

    let result = download_result_from_torz("realdebrid", "ABCDEF", torz);

    assert_eq!(result.provider, Some("realdebrid".to_string()));
    assert_eq!(result.plugin_name, "stremthru");
    assert_eq!(result.files[0].filename, "Season 01/file.mkv");
    assert_eq!(result.files[0].file_size, 1024);
    assert_eq!(
        result.files[0].download_url,
        Some("https://example.test/file.mkv".to_string())
    );
    assert_eq!(result.files[1].filename, "broken.mkv");
    assert_eq!(result.files[1].file_size, 0);
    assert_eq!(result.files[1].download_url, None);
}

#[test]
fn empty_link_error_describes_store_error_payloads() {
    assert_eq!(
        describe_empty_link_response(r#"{"error":{"code":"BAD_LINK","message":"No link"}}"#),
        "store returned no link data: BAD_LINK - No link"
    );
    assert_eq!(
        describe_empty_link_response("not json"),
        "store returned no link data; body=not json"
    );
}

#[test]
fn cache_check_key_includes_store_and_hash() {
    assert_eq!(
        cache_check_key("torbox", "abcdef"),
        "plugin:stremthru:cache-check:torbox:abcdef"
    );
}

#[test]
fn rate_limit_cooldown_uses_quota_refill_interval() {
    // TorBox: HTTP 429 {"error":{"code":"TOO_MANY_REQUESTS","message":"60 per 1 hour"}}
    // 60 per hour → one slot frees up about every 60 seconds.
    let cooldown = rate_limit_cooldown(
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"code":"TOO_MANY_REQUESTS","message":"60 per 1 hour","errors":[]}}"#,
    );
    assert_eq!(cooldown, Some(Duration::from_secs(60)));
}

#[test]
fn rate_limit_cooldown_defaults_when_quota_is_unparseable() {
    let cooldown = rate_limit_cooldown(reqwest::StatusCode::TOO_MANY_REQUESTS, "not json");
    assert_eq!(
        cooldown,
        Some(Duration::from_secs(DEFAULT_STORE_COOLDOWN_SECS))
    );
}

#[test]
fn rate_limit_cooldown_matches_proxied_error_code_on_other_statuses() {
    // Some stores surface quota errors through StremThru with a non-429
    // status; the error code still identifies them.
    let cooldown = rate_limit_cooldown(
        reqwest::StatusCode::BAD_REQUEST,
        r#"{"error":{"code":"TOO_MANY_REQUESTS","message":"10 per 1 minute","errors":[]}}"#,
    );
    assert_eq!(cooldown, Some(Duration::from_secs(6)));
}

#[test]
fn rate_limit_cooldown_ignores_ordinary_rejections() {
    let cooldown = rate_limit_cooldown(
        reqwest::StatusCode::BAD_REQUEST,
        r#"{"error":{"code":"BAD_REQUEST","message":"Debrid-Link Error Code: notAddTorrent","errors":[]}}"#,
    );
    assert_eq!(cooldown, None);
}

#[test]
fn classifies_already_queued_as_in_progress() {
    // TorBox: HTTP 400 {"error":{"code":"UNKNOWN","message":"Download already queued."}}
    let outcome = classify_add_torrent_rejection(
        reqwest::StatusCode::BAD_REQUEST,
        r#"{"error":{"code":"UNKNOWN","message":"Download already queued.","errors":[]}}"#,
    );
    assert!(matches!(outcome, AddTorrentOutcome::AlreadyQueued));
}

#[test]
fn classifies_store_error_codes_as_rejected() {
    // Debrid-Link: HTTP 400 {"error":{"code":"BAD_REQUEST","message":"Debrid-Link Error Code: notAddTorrent"}}
    let outcome = classify_add_torrent_rejection(
        reqwest::StatusCode::BAD_REQUEST,
        r#"{"error":{"code":"BAD_REQUEST","message":"Debrid-Link Error Code: notAddTorrent","errors":[]}}"#,
    );
    assert!(
        matches!(outcome, AddTorrentOutcome::Rejected { reason } if reason.contains("notAddTorrent"))
    );
}

#[test]
fn parses_quota_messages_into_refill_intervals() {
    assert_eq!(
        parse_quota_interval("60 per 1 hour"),
        Some(Duration::from_secs(60))
    );
    assert_eq!(
        parse_quota_interval("10 per 1 minute"),
        Some(Duration::from_secs(6))
    );
    assert_eq!(
        parse_quota_interval("2 per 1 second"),
        Some(Duration::from_secs(1))
    );
    assert_eq!(parse_quota_interval("garbage"), None);
    assert_eq!(parse_quota_interval(""), None);
    assert_eq!(parse_quota_interval("0 per 1 hour"), None);
}

#[test]
fn add_torrent_accepts_cached_status_for_torbox_instant_downloads() {
    // TorBox items in the seeded pool may return "cached" on the initial ADD
    // response even though files are accessible (DownloadFinished/DownloadPresent
    // flags aren't set until the background fetch completes).
    let torz_cached = StremthruTorz {
        id: "torz-2".to_string(),
        status: "cached".to_string(),
        files: vec![StremthruFile {
            name: "movie.mkv".to_string(),
            path: String::new(),
            size: 2048,
            link: "https://cdn.torbox.app/movie.mkv".to_string(),
        }],
    };
    let result = download_result_from_torz("torbox", "ABCDEF", torz_cached);
    assert_eq!(result.provider, Some("torbox".to_string()));
    assert_eq!(result.files[0].file_size, 2048);
    assert_eq!(
        result.files[0].download_url,
        Some("https://cdn.torbox.app/movie.mkv".to_string())
    );
}
