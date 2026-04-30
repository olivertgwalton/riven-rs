use std::collections::{HashSet, VecDeque};

use anyhow::Result;
use riven_core::types::{MediaItemState, MediaItemType, ShowStatus};
use sqlx::PgPool;

/// Read the per-item retry ceiling from the `general` settings blob. `0`
/// disables the ceiling.
async fn read_max_attempts(pool: &PgPool) -> Result<i32> {
    let value: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'general'")
            .fetch_optional(pool)
            .await?;
    Ok(value
        .as_ref()
        .and_then(|v| v.get("maximum_scrape_attempts"))
        .and_then(|v| v.as_i64())
        .map(|n| n as i32)
        .unwrap_or(0))
}

/// Parent-from-children rollup. `None` means the rollup didn't decide and the
/// caller should fall through to the leaf rules. Pure so it can be unit-tested.
fn aggregate_states(
    parent_type: MediaItemType,
    parent_state: MediaItemState,
    show_status: Option<ShowStatus>,
    child_states: &[MediaItemState],
) -> Option<MediaItemState> {
    if child_states.is_empty() {
        return None;
    }

    // Sticky states on the parent itself short-circuit the rollup.
    if matches!(
        parent_state,
        MediaItemState::Paused | MediaItemState::Failed
    ) {
        return Some(parent_state);
    }

    let all_eq = |target: MediaItemState| child_states.iter().all(|s| *s == target);

    if all_eq(MediaItemState::Paused) {
        return Some(MediaItemState::Paused);
    }
    if all_eq(MediaItemState::Failed) {
        return Some(MediaItemState::Failed);
    }
    if all_eq(MediaItemState::Unreleased) {
        return Some(MediaItemState::Unreleased);
    }

    // All completed → ongoing for a continuing show, else completed.
    if all_eq(MediaItemState::Completed) {
        if parent_type == MediaItemType::Show && show_status == Some(ShowStatus::Continuing) {
            return Some(MediaItemState::Ongoing);
        }
        return Some(MediaItemState::Completed);
    }

    let any_ongoing_or_unreleased = child_states
        .iter()
        .any(|s| matches!(s, MediaItemState::Ongoing | MediaItemState::Unreleased));
    if any_ongoing_or_unreleased
        || (parent_type == MediaItemType::Show && show_status == Some(ShowStatus::Continuing))
    {
        return Some(MediaItemState::Ongoing);
    }

    if child_states.iter().any(|s| {
        matches!(
            s,
            MediaItemState::Completed | MediaItemState::PartiallyCompleted
        )
    }) {
        return Some(MediaItemState::PartiallyCompleted);
    }

    if child_states.contains(&MediaItemState::Scraped) {
        return Some(MediaItemState::Scraped);
    }

    None
}

/// Item facts a recompute needs. Consolidated in a single SELECT so a leaf
/// recompute is one round-trip; shows/seasons need a second query for child
/// states.
struct ItemFacts {
    item_type: MediaItemType,
    state: MediaItemState,
    show_status: Option<ShowStatus>,
    parent_id: Option<i64>,
    is_unreleased: bool,
    failed_attempts: i32,
    has_media_entry: bool,
    has_non_blacklisted_stream: bool,
}

async fn load_item_facts(pool: &PgPool, item_id: i64) -> Result<Option<ItemFacts>> {
    let row = sqlx::query!(
        r#"SELECT
              m.item_type    AS "item_type: MediaItemType",
              m.state        AS "state: MediaItemState",
              m.show_status  AS "show_status: ShowStatus",
              m.parent_id,
              m.failed_attempts,
              (m.aired_at IS NOT NULL AND m.aired_at > CURRENT_DATE) AS "is_unreleased!",
              EXISTS(
                  SELECT 1 FROM filesystem_entries fe
                  WHERE fe.media_item_id = m.id AND fe.entry_type = 'media'
              ) AS "has_media_entry!",
              EXISTS(
                  SELECT 1 FROM media_item_streams ms
                  WHERE ms.media_item_id = m.id
                    AND ms.stream_id NOT IN (
                        SELECT stream_id FROM media_item_blacklisted_streams
                        WHERE media_item_id = m.id
                    )
              ) AS "has_non_blacklisted_stream!"
           FROM media_items m WHERE m.id = $1"#,
        item_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| ItemFacts {
        item_type: r.item_type,
        state: r.state,
        show_status: r.show_status,
        parent_id: r.parent_id,
        is_unreleased: r.is_unreleased,
        failed_attempts: r.failed_attempts,
        has_media_entry: r.has_media_entry,
        has_non_blacklisted_stream: r.has_non_blacklisted_stream,
    }))
}

async fn load_child_states(
    pool: &PgPool,
    parent_id: i64,
    parent_type: MediaItemType,
) -> Result<Vec<MediaItemState>> {
    match parent_type {
        MediaItemType::Show => Ok(sqlx::query_scalar!(
            r#"SELECT state AS "state: MediaItemState"
               FROM media_items
               WHERE parent_id = $1
                 AND item_type = 'season'
                 AND is_requested = true
                 AND is_special = false"#,
            parent_id
        )
        .fetch_all(pool)
        .await?),
        MediaItemType::Season => Ok(sqlx::query_scalar!(
            r#"SELECT state AS "state: MediaItemState"
               FROM media_items
               WHERE parent_id = $1
                 AND item_type = 'episode'"#,
            parent_id
        )
        .fetch_all(pool)
        .await?),
        _ => Ok(Vec::new()),
    }
}

/// Apply the leaf rules to an already-loaded fact bundle.
fn leaf_state(facts: &ItemFacts, max_attempts: i32) -> MediaItemState {
    if facts.is_unreleased {
        return MediaItemState::Unreleased;
    }
    if matches!(facts.state, MediaItemState::Paused | MediaItemState::Failed) {
        return facts.state;
    }
    if max_attempts > 0 && facts.failed_attempts >= max_attempts {
        return MediaItemState::Failed;
    }
    if matches!(
        facts.item_type,
        MediaItemType::Movie | MediaItemType::Episode
    ) && facts.has_media_entry
    {
        return MediaItemState::Completed;
    }
    if facts.has_non_blacklisted_stream {
        return MediaItemState::Scraped;
    }
    MediaItemState::Indexed
}

/// Recompute one item; if the state changed, return its `parent_id` so the
/// caller can cascade.
async fn recompute_one(pool: &PgPool, item_id: i64, max_attempts: i32) -> Result<Option<i64>> {
    let Some(facts) = load_item_facts(pool, item_id).await? else {
        return Ok(None);
    };

    let new_state = match facts.item_type {
        MediaItemType::Show | MediaItemType::Season => {
            let children = load_child_states(pool, item_id, facts.item_type).await?;
            aggregate_states(facts.item_type, facts.state, facts.show_status, &children)
                .unwrap_or_else(|| leaf_state(&facts, max_attempts))
        }
        MediaItemType::Movie | MediaItemType::Episode => leaf_state(&facts, max_attempts),
    };

    if new_state == facts.state {
        return Ok(None);
    }

    sqlx::query!(
        "UPDATE media_items SET state = $1, updated_at = NOW() WHERE id = $2",
        new_state as MediaItemState,
        item_id,
    )
    .execute(pool)
    .await?;

    Ok(facts.parent_id)
}

/// Recompute the given items, cascading to parents on state change. Safe to
/// call with arbitrary input order: every parent enqueued by a state-change
/// cascade is re-evaluated regardless of whether it appeared earlier in the
/// queue, so a parent processed before its child gets a second pass against
/// the now-updated child state. Termination is guaranteed by the tree shape
/// of `media_items.parent_id` plus the determinism of [`leaf_state`] /
/// [`aggregate_states`].
pub async fn recompute(pool: &PgPool, item_ids: &[i64]) -> Result<()> {
    if item_ids.is_empty() {
        return Ok(());
    }
    let max_attempts = read_max_attempts(pool).await?;

    let mut initial = HashSet::new();
    let mut queue: VecDeque<i64> = VecDeque::new();
    for &id in item_ids {
        if initial.insert(id) {
            queue.push_back(id);
        }
    }

    while let Some(id) = queue.pop_front() {
        if let Some(parent_id) = recompute_one(pool, id, max_attempts).await? {
            queue.push_back(parent_id);
        }
    }
    Ok(())
}

/// User-driven exit from `Paused`. Flips paused rows to a non-sticky
/// placeholder (`Indexed`), then derives the real post-pause state from the
/// current facts via [`recompute`].
pub async fn unpause_items(pool: &PgPool, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query!(
        "UPDATE media_items SET state = 'indexed', updated_at = NOW() \
         WHERE id = ANY($1) AND state = 'paused'",
        ids
    )
    .execute(pool)
    .await?;
    recompute(pool, ids).await
}

/// Re-derive state for the given ids. Application writes already trigger a
/// recompute via the repo layer; this exists for admin tools and one-off
/// backfills after data fix-ups.
pub async fn force_recompute(pool: &PgPool, ids: &[i64]) -> Result<()> {
    recompute(pool, ids).await
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn facts(
        item_type: MediaItemType,
        state: MediaItemState,
        is_unreleased: bool,
        failed_attempts: i32,
        has_media_entry: bool,
        has_non_blacklisted_stream: bool,
    ) -> ItemFacts {
        ItemFacts {
            item_type,
            state,
            show_status: None,
            parent_id: None,
            is_unreleased,
            failed_attempts,
            has_media_entry,
            has_non_blacklisted_stream,
        }
    }

    #[test]
    fn leaf_unreleased_takes_precedence() {
        let f = facts(MediaItemType::Episode, Indexed, true, 0, true, true);
        assert_eq!(leaf_state(&f, 0), Unreleased);
    }

    #[test]
    fn leaf_paused_is_sticky() {
        let f = facts(MediaItemType::Movie, Paused, false, 0, true, false);
        assert_eq!(leaf_state(&f, 0), Paused);
    }

    #[test]
    fn leaf_failed_is_sticky() {
        let f = facts(MediaItemType::Movie, Failed, false, 0, true, false);
        assert_eq!(leaf_state(&f, 0), Failed);
    }

    #[test]
    fn leaf_attempts_ceiling_fails() {
        let f = facts(MediaItemType::Movie, Indexed, false, 5, false, false);
        assert_eq!(leaf_state(&f, 5), Failed);
    }

    #[test]
    fn leaf_attempts_ceiling_disabled_when_zero() {
        let f = facts(MediaItemType::Movie, Indexed, false, 999, false, false);
        assert_eq!(leaf_state(&f, 0), Indexed);
    }

    #[test]
    fn leaf_completed_when_media_entry_exists() {
        let f = facts(MediaItemType::Movie, Indexed, false, 0, true, false);
        assert_eq!(leaf_state(&f, 0), Completed);
    }

    #[test]
    fn leaf_scraped_when_only_streams() {
        let f = facts(MediaItemType::Movie, Indexed, false, 0, false, true);
        assert_eq!(leaf_state(&f, 0), Scraped);
    }

    #[test]
    fn leaf_indexed_when_no_facts() {
        let f = facts(MediaItemType::Movie, Indexed, false, 0, false, false);
        assert_eq!(leaf_state(&f, 0), Indexed);
    }
}
