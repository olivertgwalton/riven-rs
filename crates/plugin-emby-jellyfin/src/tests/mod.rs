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
