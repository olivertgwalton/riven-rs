use riven_core::types::{ItemRequestState, MediaItemState};
use riven_db::repo::requests::artist_request_state;

use MediaItemState::*;

#[test]
fn artist_request_waits_until_albums_exist() {
    assert_eq!(artist_request_state(&[], true), ItemRequestState::Requested);
}

#[test]
fn chosen_albums_all_in_complete_the_request() {
    assert_eq!(
        artist_request_state(&[Completed, Completed], false),
        ItemRequestState::Completed
    );
}

/// A followed artist may release another album, as a continuing show may air
/// another season.
#[test]
fn followed_discography_stays_ongoing_when_all_in() {
    assert_eq!(
        artist_request_state(&[Completed, Completed], true),
        ItemRequestState::Ongoing
    );
}

#[test]
fn announced_albums_leave_the_request_unreleased() {
    assert_eq!(
        artist_request_state(&[Unreleased], false),
        ItemRequestState::Unreleased
    );
}

#[test]
fn a_failed_album_fails_the_request() {
    assert_eq!(
        artist_request_state(&[Completed, Failed], false),
        ItemRequestState::Failed
    );
}

#[test]
fn albums_in_progress_keep_the_request_ongoing() {
    assert_eq!(
        artist_request_state(&[Completed, Scraped], false),
        ItemRequestState::Ongoing
    );
}
