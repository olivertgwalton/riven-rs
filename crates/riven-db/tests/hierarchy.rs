use chrono::{TimeZone, Utc};
use riven_core::types::MediaItemState;
use riven_db::repo::hierarchy::episode_state_for_air_date;

fn at(y: i32, m: u32, d: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
}

#[test]
fn unknown_air_date_is_unreleased_not_indexed() {
    let now = at(2026, 8, 11);
    assert_eq!(
        episode_state_for_air_date(None, now),
        MediaItemState::Unreleased,
        "a TVDB 'TBA' stub with no air date must wait for real metadata, not be scraped"
    );
}

#[test]
fn future_air_date_is_unreleased() {
    let now = at(2026, 8, 11);
    assert_eq!(
        episode_state_for_air_date(Some(at(2026, 8, 12)), now),
        MediaItemState::Unreleased
    );
}

#[test]
fn past_air_date_is_indexed() {
    let now = at(2026, 8, 11);
    assert_eq!(
        episode_state_for_air_date(Some(at(2026, 8, 10)), now),
        MediaItemState::Indexed
    );
}

#[test]
fn air_date_exactly_now_is_indexed() {
    let now = at(2026, 8, 11);
    assert_eq!(
        episode_state_for_air_date(Some(now), now),
        MediaItemState::Indexed
    );
}
