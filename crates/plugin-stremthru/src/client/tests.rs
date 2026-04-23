use super::*;
use crate::models::{StremthruTorz, StremthruTorzFile};

#[test]
fn download_result_prefers_file_path_and_clamps_negative_sizes() {
    let torz = StremthruTorz {
        id: "torz-1".to_string(),
        status: "downloaded".to_string(),
        files: vec![
            StremthruTorzFile {
                name: "file.mkv".to_string(),
                path: "Season 01/file.mkv".to_string(),
                size: 1024,
                link: "https://example.test/file.mkv".to_string(),
            },
            StremthruTorzFile {
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
fn add_torrent_accepts_cached_status_for_torbox_instant_downloads() {
    // TorBox items in the seeded pool may return "cached" on the initial ADD
    // response even though files are accessible (DownloadFinished/DownloadPresent
    // flags aren't set until the background fetch completes).
    let torz_cached = StremthruTorz {
        id: "torz-2".to_string(),
        status: "cached".to_string(),
        files: vec![StremthruTorzFile {
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
