use std::collections::{HashSet, VecDeque};

use anyhow::Result;
use chrono::Utc;
use riven_core::entities::{media_items, settings};
use riven_core::types::{MediaItemState, MediaItemType, ShowStatus};
use sea_orm::ActiveValue::{Set, Unchanged};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ColumnTrait, DbBackend, EntityTrait, FromQueryResult,
    QueryFilter, QuerySelect, Statement,
};

use crate::orm;

/// Read the per-item retry ceiling from the `general` settings blob. `0`
/// disables the ceiling.
async fn read_max_attempts() -> Result<i32> {
    let value: Option<serde_json::Value> = settings::Entity::find()
        .filter(settings::Column::Key.eq("general"))
        .select_only()
        .column(settings::Column::Value)
        .into_tuple()
        .one(orm())
        .await?;
    Ok(value
        .as_ref()
        .and_then(|v| v.get("maximum_scrape_attempts"))
        .and_then(serde_json::Value::as_i64)
        .map_or(0, |n| n as i32))
}

/// Parent-from-children rollup. `None` means the rollup didn't decide and the
/// caller should fall through to the leaf rules. Pure so it can be unit-tested.
pub fn aggregate_states(
    parent_type: MediaItemType,
    parent_state: MediaItemState,
    show_status: Option<ShowStatus>,
    child_states: &[MediaItemState],
) -> Option<MediaItemState> {
    if child_states.is_empty() {
        return None;
    }

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

    if all_eq(MediaItemState::Completed) {
        if parent_type == MediaItemType::Show && show_status == Some(ShowStatus::Continuing) {
            return Some(MediaItemState::Ongoing);
        }
        return Some(MediaItemState::Completed);
    }

    // Ongoing must not win while a child has actionable work: `ongoing` is not
    // picked up by `get_pending_items_for_retry`, so deriving it over an
    // Indexed/Scraped/PartiallyCompleted child would orphan that work.
    let any_actionable = child_states.iter().any(|s| {
        matches!(
            s,
            MediaItemState::Indexed | MediaItemState::Scraped | MediaItemState::PartiallyCompleted
        )
    });
    let any_ongoing_or_unreleased = child_states
        .iter()
        .any(|s| matches!(s, MediaItemState::Ongoing | MediaItemState::Unreleased));
    if !any_actionable
        && (any_ongoing_or_unreleased
            || (parent_type == MediaItemType::Show && show_status == Some(ShowStatus::Continuing)))
    {
        return Some(MediaItemState::Ongoing);
    }

    if child_states.iter().any(|s| {
        matches!(
            s,
            MediaItemState::Completed
                | MediaItemState::PartiallyCompleted
                | MediaItemState::Ongoing
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
#[derive(FromQueryResult)]
struct ItemFacts {
    item_type: MediaItemType,
    state: MediaItemState,
    show_status: Option<ShowStatus>,
    parent_id: Option<i64>,
    is_unreleased: bool,
    has_air_date: bool,
    has_been_indexed: bool,
    failed_attempts: i32,
    has_media_entry: bool,
    has_non_blacklisted_stream: bool,
}

async fn load_item_facts(item_id: i64) -> Result<Option<ItemFacts>> {
    Ok(ItemFacts::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        // Cast the PG enum columns to text: a raw statement decodes columns by
        // the FromQueryResult field types, and DeriveActiveEnum reads its
        // `string_value` from text rather than the native enum OID.
        r#"SELECT
              m.item_type::text AS item_type,
              m.state::text     AS state,
              m.show_status::text AS show_status,
              m.parent_id,
              m.failed_attempts,
              (m.aired_at IS NOT NULL AND m.aired_at > CURRENT_DATE) AS is_unreleased,
              (m.aired_at IS NOT NULL) AS has_air_date,
              (m.indexed_at IS NOT NULL) AS has_been_indexed,
              EXISTS(
                  SELECT 1 FROM filesystem_entries fe
                  WHERE fe.media_item_id = m.id AND fe.entry_type = 'media'
              ) AS has_media_entry,
              EXISTS(
                  SELECT 1 FROM media_item_streams ms
                  WHERE ms.media_item_id = m.id
                    AND ms.stream_id NOT IN (
                        SELECT stream_id FROM media_item_blacklisted_streams
                        WHERE media_item_id = m.id
                    )
              ) AS has_non_blacklisted_stream
           FROM media_items m WHERE m.id = $1"#,
        [item_id.into()],
    ))
    .one(orm())
    .await?)
}

async fn load_child_states(
    parent_id: i64,
    parent_type: MediaItemType,
) -> Result<Vec<MediaItemState>> {
    let base = media_items::Entity::find().filter(media_items::Column::ParentId.eq(parent_id));
    let query = match parent_type {
        MediaItemType::Show => base
            .filter(media_items::Column::ItemType.eq(MediaItemType::Season))
            .filter(media_items::Column::IsRequested.eq(true))
            .filter(media_items::Column::IsSpecial.eq(false)),
        MediaItemType::Season => {
            base.filter(media_items::Column::ItemType.eq(MediaItemType::Episode))
        }
        _ => return Ok(Vec::new()),
    };
    Ok(query
        .select_only()
        .column(media_items::Column::State)
        .into_tuple()
        .all(orm())
        .await?)
}

/// Apply the leaf-state rules. Pure so it can be unit-tested without a DB.
///
/// Order matters: `Unreleased` (a future air date) wins over everything,
/// then sticky `Paused`, then media-entry existence (movies / episodes only),
/// then a missing air date (movies / episodes only), then sticky `Failed`,
/// then the attempts ceiling, then any non-blacklisted stream. Default is
/// `Indexed`.
///
/// `Failed` is sticky against everything *except* a real media entry: a
/// season-pack download matches files by season+episode number regardless of
/// the target episode's current state, so an episode marked `Failed` long ago
/// can still receive a genuine file today (e.g. once a season-level retry
/// that was previously stuck on an earlier episode finally reaches it). A
/// media entry on disk is a stronger, more direct signal than the
/// `failed_attempts` counter — leaving the item stuck on `Failed` after that
/// would mean a real download nobody can see. `Paused` stays sticky
/// unconditionally: it is a deliberate user action, and a paused item is
/// excluded from the download pipeline in the first place, so it cannot
/// legitimately acquire a new media entry to override it with anyway.
///
/// A leaf with no air date *at all* is `Unreleased` too, but only after the
/// media-entry check: metadata sometimes omits a date for something that aired
/// years ago, and a file on disk settles that better than the missing date
/// does. With no file, an undated leaf is an announced-but-unscheduled entry —
/// TVDB carries those as `TBA` with a null date — and calling it `Indexed`
/// parks it in the scrape loop forever chasing a release nobody has posted.
///
/// That reading only holds once there *is* metadata to read. A movie is
/// created bare from a request and indexed a second or two later, so until
/// then a null date says nothing about the release — and saying `Unreleased`
/// flashed that badge on every newly added film before it settled to
/// `Indexed`. `indexed_at` separates the two, the same discriminator
/// `handle_scrape` uses for "never indexed, so there is no metadata to scrape
/// with". Episodes need no such guard and must not get one: an episode row
/// exists only because the show indexer created it, so it is born with
/// whatever metadata there was and a null date here is the answer rather than
/// a gap before one. They also never carry an `indexed_at` of their own —
/// only movies and shows do — so gating them on it would park every `TBA`
/// episode back in the scrape loop.
pub fn leaf_state(
    item_type: MediaItemType,
    current_state: MediaItemState,
    is_unreleased: bool,
    has_air_date: bool,
    has_been_indexed: bool,
    failed_attempts: i32,
    has_media_entry: bool,
    has_non_blacklisted_stream: bool,
    max_attempts: i32,
) -> MediaItemState {
    if is_unreleased {
        return MediaItemState::Unreleased;
    }
    if current_state == MediaItemState::Paused {
        return current_state;
    }
    if matches!(item_type, MediaItemType::Movie | MediaItemType::Episode) && has_media_entry {
        return MediaItemState::Completed;
    }
    let undated_means_unreleased = match item_type {
        MediaItemType::Movie => has_been_indexed,
        MediaItemType::Episode => true,
        _ => false,
    };
    if !has_air_date && undated_means_unreleased {
        return MediaItemState::Unreleased;
    }
    if current_state == MediaItemState::Failed {
        return current_state;
    }
    if max_attempts > 0 && failed_attempts >= max_attempts {
        return MediaItemState::Failed;
    }
    if has_non_blacklisted_stream {
        return MediaItemState::Scraped;
    }
    MediaItemState::Indexed
}

/// Recompute one item; if the state changed, return its `parent_id` so the
/// caller can cascade.
async fn recompute_one(item_id: i64, max_attempts: i32) -> Result<Option<i64>> {
    let Some(facts) = load_item_facts(item_id).await? else {
        return Ok(None);
    };

    let leaf = || {
        leaf_state(
            facts.item_type,
            facts.state,
            facts.is_unreleased,
            facts.has_air_date,
            facts.has_been_indexed,
            facts.failed_attempts,
            facts.has_media_entry,
            facts.has_non_blacklisted_stream,
            max_attempts,
        )
    };
    let new_state = match facts.item_type {
        MediaItemType::Show | MediaItemType::Season => {
            let children = load_child_states(item_id, facts.item_type).await?;
            aggregate_states(facts.item_type, facts.state, facts.show_status, &children)
                .unwrap_or_else(leaf)
        }
        MediaItemType::Movie | MediaItemType::Episode => leaf(),
    };

    if new_state == facts.state {
        return Ok(None);
    }

    media_items::ActiveModel {
        id: Unchanged(item_id),
        state: Set(new_state),
        updated_at: Set(Some(Utc::now())),
        ..Default::default()
    }
    .update(orm())
    .await?;

    Ok(facts.parent_id)
}

pub async fn recompute(item_ids: &[i64]) -> Result<()> {
    if item_ids.is_empty() {
        return Ok(());
    }
    let max_attempts = read_max_attempts().await?;

    let mut initial = HashSet::new();
    let mut queue: VecDeque<i64> = VecDeque::new();
    for &id in item_ids {
        if initial.insert(id) {
            queue.push_back(id);
        }
    }

    while let Some(id) = queue.pop_front() {
        if let Some(parent_id) = recompute_one(id, max_attempts).await? {
            queue.push_back(parent_id);
        }
    }
    Ok(())
}

/// User-driven exit from `Paused`. Flips paused rows to a non-sticky
/// placeholder (`Indexed`), then derives the real post-pause state from the
/// current facts via [`recompute`].
pub async fn unpause_items(ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    media_items::Entity::update_many()
        .col_expr(
            media_items::Column::State,
            MediaItemState::Indexed.as_enum(),
        )
        .col_expr(media_items::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(media_items::Column::Id.is_in(ids.iter().copied()))
        .filter(media_items::Column::State.eq(MediaItemState::Paused))
        .exec(orm())
        .await?;
    recompute(ids).await
}

/// Re-derive state for the given ids. Application writes already recompute via
/// the repo layer; this exists for admin tools and one-off backfills after data
/// fix-ups.
pub async fn force_recompute(ids: &[i64]) -> Result<()> {
    recompute(ids).await
}
