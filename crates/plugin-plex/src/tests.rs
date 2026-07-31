use super::*;

#[test]
fn playback_state_mapping_handles_known_plex_states() {
    assert_eq!(map_playback_state(Some("playing")), PlaybackState::Playing);
    assert_eq!(map_playback_state(Some("paused")), PlaybackState::Paused);
    assert_eq!(
        map_playback_state(Some("buffering")),
        PlaybackState::Buffering
    );
    assert_eq!(map_playback_state(Some("stopped")), PlaybackState::Idle);
    assert_eq!(map_playback_state(Some("idle")), PlaybackState::Unknown);
    assert_eq!(map_playback_state(None), PlaybackState::Unknown);
}

#[test]
fn playback_method_is_transcode_only_when_session_reports_transcoding() {
    assert_eq!(map_playback_method(true), PlaybackMethod::Transcode);
    assert_eq!(map_playback_method(false), PlaybackMethod::DirectPlay);
}

#[test]
fn plex_refresh_url_encoding_preserves_path_separators() {
    assert_eq!(
        urlencoding::encode("/mount/Movies/A Film (2024)"),
        "/mount/Movies/A%20Film%20%282024%29"
    );
}
