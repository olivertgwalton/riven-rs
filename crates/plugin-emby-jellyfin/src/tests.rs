use super::*;

fn play_state(is_paused: Option<bool>, play_method: Option<&str>) -> MediaServerPlayState {
    MediaServerPlayState {
        position_ticks: None,
        is_paused,
        play_method: play_method.map(str::to_string),
    }
}

#[test]
fn rewrite_media_path_handles_slashes_once() {
    assert_eq!(
        rewrite_media_path("/library/", "/shows/Show/S01E01.mkv"),
        "/library/shows/Show/S01E01.mkv"
    );
    assert_eq!(
        rewrite_media_path("/library", "movies/Movie.mkv"),
        "/library/movies/Movie.mkv"
    );
}

#[test]
fn media_server_request_uses_header_auth_without_query_token() {
    let client = reqwest::Client::new();
    let request = media_server_request(
        &client,
        reqwest::Method::POST,
        "https://emby.example.test/Library/Media/Updated",
        "secret-token",
    )
    .build()
    .expect("request should build");

    assert_eq!(
        request
            .headers()
            .get(MEDIA_SERVER_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("secret-token")
    );
    assert_eq!(request.url().query(), None);
}

#[test]
fn media_server_playback_state_uses_pause_flag() {
    assert_eq!(
        map_media_server_playback_state(&play_state(Some(true), None)),
        PlaybackState::Paused
    );
    assert_eq!(
        map_media_server_playback_state(&play_state(Some(false), None)),
        PlaybackState::Playing
    );
    assert_eq!(
        map_media_server_playback_state(&play_state(None, None)),
        PlaybackState::Unknown
    );
}

#[test]
fn media_server_playback_method_maps_emby_jellyfin_names() {
    assert_eq!(
        map_media_server_playback_method(&play_state(None, Some("DirectPlay"))),
        PlaybackMethod::DirectPlay
    );
    assert_eq!(
        map_media_server_playback_method(&play_state(None, Some("DirectStream"))),
        PlaybackMethod::DirectStream
    );
    assert_eq!(
        map_media_server_playback_method(&play_state(None, Some("Transcoding"))),
        PlaybackMethod::Transcode
    );
    assert_eq!(
        map_media_server_playback_method(&play_state(None, Some("Other"))),
        PlaybackMethod::Unknown
    );
}
