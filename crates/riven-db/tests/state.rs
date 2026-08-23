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
fn ongoing_child_with_pending_sibling_is_partially_completed() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Ongoing, Indexed]),
        Some(PartiallyCompleted)
    );
}

#[test]
fn ongoing_child_without_pending_makes_parent_ongoing() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Ongoing, Unreleased]),
        Some(Ongoing)
    );
}

#[test]
fn unreleased_mixed_with_pending_falls_through_to_leaf() {
    assert_eq!(
        agg(MediaItemType::Season, Indexed, None, &[Unreleased, Indexed]),
        None
    );
}

#[test]
fn completed_plus_unreleased_is_ongoing() {
    assert_eq!(
        agg(
            MediaItemType::Season,
            Indexed,
            None,
            &[Completed, Unreleased]
        ),
        Some(Ongoing)
    );
}

#[test]
fn aired_pending_episode_outranks_ongoing() {
    assert_eq!(
        agg(
            MediaItemType::Season,
            Indexed,
            None,
            &[Completed, Indexed, Unreleased]
        ),
        Some(PartiallyCompleted)
    );
}

#[test]
fn continuing_show_with_indexed_children_falls_through() {
    assert_eq!(
        agg(
            MediaItemType::Show,
            Indexed,
            Some(ShowStatus::Continuing),
            &[Indexed, Indexed]
        ),
        None
    );
}

#[test]
fn continuing_show_with_partially_completed_season_is_partially_completed() {
    assert_eq!(
        agg(
            MediaItemType::Show,
            Indexed,
            Some(ShowStatus::Continuing),
            &[Completed, PartiallyCompleted]
        ),
        Some(PartiallyCompleted)
    );
}

#[test]
fn continuing_show_with_ongoing_season_stays_ongoing() {
    assert_eq!(
        agg(
            MediaItemType::Show,
            Indexed,
            Some(ShowStatus::Continuing),
            &[Completed, Ongoing]
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
        true,
        failed_attempts,
        has_media_entry,
        false,
        has_non_blacklisted_stream,
        max_attempts,
    )
}

/// Same, but for the profile-coverage cases: the item has a file and an
/// enabled profile still has none.
fn leaf_missing_profile(
    item_type: MediaItemType,
    state: MediaItemState,
    failed_attempts: i32,
) -> MediaItemState {
    leaf_state(
        item_type,
        state,
        false,
        true,
        failed_attempts,
        true,
        true,
        true,
        0,
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

/// `Failed` is sticky only in the absence of a real media entry — resetting
/// `failed_attempts` alone must not revive the item; that is what
/// `resetItems`/`Reset` is for.
#[test]
fn leaf_failed_is_sticky_without_a_media_entry() {
    assert_eq!(
        leaf(MediaItemType::Movie, Failed, false, 0, false, false, 0),
        Failed
    );
}

/// A season-pack download matches files by season+episode number regardless
/// of the target episode's own state, so a media entry can land on an item
/// that was marked `Failed` long before. Without this, the item stays stuck
/// showing `Failed` forever despite having a real downloaded file.
#[test]
fn leaf_media_entry_overrides_failed() {
    assert_eq!(
        leaf(MediaItemType::Movie, Failed, false, 0, true, false, 0),
        Completed
    );
    assert_eq!(
        leaf(MediaItemType::Episode, Failed, false, 999, true, false, 5),
        Completed
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

/// With more than one profile enabled, "has a file" and "has the files that
/// were asked for" are different questions. Only the second means done, and
/// `PartiallyCompleted` is already retryable, so the existing retry loop
/// picks the item up and chases just the missing profile.
#[test]
fn leaf_with_a_missing_enabled_profile_is_partially_completed() {
    assert_eq!(
        leaf_missing_profile(MediaItemType::Episode, Completed, 0),
        PartiallyCompleted
    );
    assert_eq!(
        leaf_missing_profile(MediaItemType::Movie, Completed, 0),
        PartiallyCompleted
    );
}

/// The chase is best-effort and must terminate: an item whose missing profile
/// simply does not exist anywhere would otherwise be re-scraped forever, since
/// `maximum_scrape_attempts` cannot apply to something already downloaded
/// (`Failed` is the wrong answer for a file that plays).
#[test]
fn leaf_settles_for_what_it_has_once_the_upgrade_budget_is_spent() {
    assert_eq!(
        leaf_missing_profile(MediaItemType::Episode, PartiallyCompleted, 2),
        PartiallyCompleted
    );
    assert_eq!(
        leaf_missing_profile(MediaItemType::Episode, PartiallyCompleted, 3),
        Completed
    );
    assert_eq!(
        leaf_missing_profile(MediaItemType::Episode, PartiallyCompleted, 99),
        Completed
    );
}

/// Every enabled profile satisfied is still plain `Completed` — the common
/// case must not change.
#[test]
fn leaf_with_full_profile_coverage_is_completed() {
    assert_eq!(
        leaf(MediaItemType::Episode, Completed, false, 0, true, true, 0),
        Completed
    );
}

/// A missing profile must not resurrect an item that has no file at all: the
/// flag is only consulted on the has-a-media-entry branch.
#[test]
fn leaf_without_a_file_ignores_profile_coverage() {
    assert_eq!(
        leaf_state(
            MediaItemType::Episode,
            Indexed,
            false,
            true,
            0,
            false,
            true,
            true,
            0
        ),
        Scraped
    );
}

/// An episode metadata has no date for at all has not been scheduled, let alone
/// released: `TBA` rows must not enter the scrape loop.
#[test]
fn leaf_missing_air_date_is_unreleased() {
    assert_eq!(
        leaf_state(
            MediaItemType::Episode,
            Indexed,
            false,
            false,
            17,
            false,
            false,
            false,
            0,
        ),
        Unreleased
    );
}

/// ...but a file on disk outranks the missing date: metadata gaps are common
/// for old episodes that plainly did air.
#[test]
fn leaf_missing_air_date_yields_to_a_real_file() {
    assert_eq!(
        leaf_state(
            MediaItemType::Episode,
            Completed,
            false,
            false,
            0,
            true,
            false,
            false,
            0,
        ),
        Completed
    );
}

/// A show or season without a date derives its state from its children; only
/// leaves read a missing date as unreleased.
#[test]
fn leaf_missing_air_date_does_not_touch_containers() {
    assert_eq!(
        leaf_state(
            MediaItemType::Show,
            Indexed,
            false,
            false,
            0,
            false,
            false,
            false,
            0,
        ),
        Indexed
    );
}
