use super::*;
use riven_core::types::{PlaybackMethod, PlaybackState};

fn session(server: &str, user: Option<&str>, title: &str) -> ActivePlaybackSession {
    ActivePlaybackSession {
        server: server.to_string(),
        user_name: user.map(str::to_string),
        parent_title: None,
        item_title: title.to_string(),
        item_type: None,
        season_number: None,
        episode_number: None,
        playback_state: PlaybackState::Playing,
        playback_method: PlaybackMethod::DirectPlay,
        position_seconds: None,
        duration_seconds: None,
        device_name: None,
        client_name: None,
        image_url: None,
    }
}

#[test]
fn playback_session_sort_order_is_stable() {
    let mut sessions = [
        session("plex", Some("zoe"), "B"),
        session("emby", Some("amy"), "C"),
        session("plex", Some("amy"), "A"),
    ];

    sessions.sort_by(|a, b| {
        a.server
            .cmp(&b.server)
            .then_with(|| a.user_name.cmp(&b.user_name))
            .then_with(|| a.item_title.cmp(&b.item_title))
    });

    assert_eq!(
        sessions
            .iter()
            .map(|session| format!(
                "{}:{:?}:{}",
                session.server, session.user_name, session.item_title
            ))
            .collect::<Vec<_>>(),
        vec![
            "emby:Some(\"amy\"):C".to_string(),
            "plex:Some(\"amy\"):A".to_string(),
            "plex:Some(\"zoe\"):B".to_string(),
        ]
    );
}
