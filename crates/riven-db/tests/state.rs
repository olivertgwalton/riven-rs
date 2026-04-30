use riven_core::types::{MediaItemState, MediaItemType, ShowStatus};
use riven_db::repo::state::{aggregate_states, leaf_state};

use MediaItemState::*;

fn agg(
    parent_type: MediaItemType,
    parent_state: MediaItemState,
    show_status: Option<ShowStatus>,
    children: &[MediaItemState],
) -> Option<MediaItemState> {
    aggregate_states(parent_type, parent_state, show_status, children)
}

#[test]
fn empty_children_returns_none() {
    assert_eq!(agg(MediaItemType::Season, Indexed, None, &[]), None);
}

#[test]
fn parent_paused_is_sticky() {
    assert_eq!(
        agg(MediaItemType::Season, Paused, None, &[Completed, Indexed]),
        Some(Paused)
    );
}

#[test]
fn parent_failed_is_sticky() {
    assert_eq!(
        agg(MediaItemType::Season, Failed, None, &[Completed]),
        Some(Failed)
    );
}

#[test]
fn all_paused_propagates() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Paused, Paused]),
        Some(Paused)
    );
}

#[test]
fn all_failed_propagates() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Failed, Failed]),
        Some(Failed)
    );
}

#[test]
fn all_unreleased_propagates() {
    assert_eq!(
        agg(
            MediaItemType::Season,
            Indexed,
            None,
            &[Unreleased, Unreleased]
        ),
        Some(Unreleased)
    );
}

#[test]
fn all_completed_season_is_completed() {
    assert_eq!(
        agg(
            MediaItemType::Season,
            Indexed,
            None,
            &[Completed, Completed]
        ),
        Some(Completed)
    );
}

#[test]
fn all_completed_continuing_show_is_ongoing() {
    assert_eq!(
        agg(
            MediaItemType::Show,
            Indexed,
            Some(ShowStatus::Continuing),
            &[Completed, Completed]
        ),
        Some(Ongoing)
    );
}

#[test]
fn all_completed_ended_show_is_completed() {
    assert_eq!(
        agg(
            MediaItemType::Show,
            Indexed,
            Some(ShowStatus::Ended),
            &[Completed, Completed]
        ),
        Some(Completed)
    );
}

#[test]
fn ongoing_child_makes_parent_ongoing() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Ongoing, Indexed]),
        Some(Ongoing)
    );
}

#[test]
fn unreleased_mixed_makes_parent_ongoing() {
    // Mixed unreleased + indexed isn't all-unreleased, so it falls into
    // the ongoing/unreleased "any" check.
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Unreleased, Indexed]),
        Some(Ongoing)
    );
}

#[test]
fn continuing_show_with_indexed_children_is_ongoing() {
    assert_eq!(
        agg(
            MediaItemType::Show,
            Indexed,
            Some(ShowStatus::Continuing),
            &[Indexed, Indexed]
        ),
        Some(Ongoing)
    );
}

#[test]
fn partially_completed_when_some_complete_some_not() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Completed, Indexed]),
        Some(PartiallyCompleted)
    );
}

#[test]
fn scraped_when_a_child_is_scraped_and_others_indexed() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Scraped, Indexed]),
        Some(Scraped)
    );
}

#[test]
fn all_indexed_falls_through_to_leaf_rules() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Indexed, Indexed]),
        None
    );
}

fn leaf(
    item_type: MediaItemType,
    state: MediaItemState,
    is_unreleased: bool,
    failed_attempts: i32,
    has_media_entry: bool,
    has_non_blacklisted_stream: bool,
    max_attempts: i32,
) -> MediaItemState {
    leaf_state(
        item_type,
        state,
        is_unreleased,
        failed_attempts,
        has_media_entry,
        has_non_blacklisted_stream,
        max_attempts,
    )
}

#[test]
fn leaf_unreleased_takes_precedence() {
    assert_eq!(
        leaf(MediaItemType::Episode, Indexed, true, 0, true, true, 0),
        Unreleased
    );
}

#[test]
fn leaf_paused_is_sticky() {
    assert_eq!(
        leaf(MediaItemType::Movie, Paused, false, 0, true, false, 0),
        Paused
    );
}

#[test]
fn leaf_failed_is_sticky() {
    assert_eq!(
        leaf(MediaItemType::Movie, Failed, false, 0, true, false, 0),
        Failed
    );
}

#[test]
fn leaf_attempts_ceiling_fails() {
    assert_eq!(
        leaf(MediaItemType::Movie, Indexed, false, 5, false, false, 5),
        Failed
    );
}

#[test]
fn leaf_attempts_ceiling_disabled_when_zero() {
    assert_eq!(
        leaf(MediaItemType::Movie, Indexed, false, 999, false, false, 0),
        Indexed
    );
}

#[test]
fn leaf_completed_when_media_entry_exists() {
    assert_eq!(
        leaf(MediaItemType::Movie, Indexed, false, 0, true, false, 0),
        Completed
    );
}

#[test]
fn leaf_scraped_when_only_streams() {
    assert_eq!(
        leaf(MediaItemType::Movie, Indexed, false, 0, false, true, 0),
        Scraped
    );
}

#[test]
fn leaf_indexed_when_no_facts() {
    assert_eq!(
        leaf(MediaItemType::Movie, Indexed, false, 0, false, false, 0),
        Indexed
    );
}
