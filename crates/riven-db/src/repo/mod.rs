pub mod hierarchy;
pub mod media;
pub mod requests;
pub mod state;
pub mod stats;
pub mod streams;
pub mod usenet_health;
pub mod usenet_traffic;

pub use hierarchy::*;
pub use media::*;
pub use requests::*;
pub use state::*;
pub use stats::*;
pub use streams::*;
pub use usenet_health::*;
pub use usenet_traffic::*;

use anyhow::Result;
use riven_core::entities::{item_requests, media_items};
use riven_core::types::MediaItemState;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveEnum, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait, QueryFilter,
    QuerySelect, Statement,
};

use crate::orm;

pub async fn reset_library() -> Result<u64> {
    let count = media_items::Entity::find().count(orm()).await?;

    // SeaORM has no TRUNCATE builder; keep the multi-table reset raw.
    orm()
        .execute(Statement::from_string(
            DbBackend::Postgres,
            "TRUNCATE TABLE media_items, streams, item_requests, usenet_meta \
             RESTART IDENTITY CASCADE",
        ))
        .await?;

    Ok(count)
}

pub async fn reset_items_by_ids(ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = media_items::Entity::update_many()
        .col_expr(
            media_items::Column::State,
            MediaItemState::Indexed.as_enum(),
        )
        .col_expr(media_items::Column::FailedAttempts, Expr::value(0))
        .col_expr(media_items::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(media_items::Column::Id.is_in(ids.iter().copied()))
        .exec(orm())
        .await?;
    state::recompute(&ids).await?;
    Ok(result.rows_affected)
}

pub async fn retry_items_by_ids(ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = media_items::Entity::update_many()
        .col_expr(media_items::Column::FailedAttempts, Expr::value(0))
        // A manual retry must take effect on the very next `retry_library()`
        // tick, not remain subject to `FAILED_ATTEMPTS_COOLDOWN_SQL`'s
        // recency check — that cooldown exists to throttle *automatic*
        // re-attempts, not a user's explicit "retry now". Clearing
        // `last_scrape_attempt_at` alongside `failed_attempts` is what
        // actually restores eligibility; resetting `failed_attempts` alone
        // does nothing if the item was scraped within the last 30 minutes.
        .col_expr(media_items::Column::LastScrapeAttemptAt, Expr::cust("NULL"))
        .col_expr(media_items::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(media_items::Column::Id.is_in(ids.iter().copied()))
        .exec(orm())
        .await?;
    state::recompute(&ids).await?;
    Ok(result.rows_affected)
}

pub async fn pause_items_by_ids(ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = media_items::Entity::update_many()
        .col_expr(media_items::Column::State, MediaItemState::Paused.as_enum())
        .col_expr(media_items::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(media_items::Column::Id.is_in(ids.iter().copied()))
        .exec(orm())
        .await?;
    state::recompute(&ids).await?;
    Ok(result.rows_affected)
}

pub async fn unpause_items_by_ids(ids: Vec<i64>) -> Result<u64> {
    state::unpause_items(&ids).await?;
    Ok(ids.len() as u64)
}

pub async fn delete_items_by_ids(ids: Vec<i64>) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    // Capture parents before the DELETE: deleting children fires no recompute
    // on the parent, so they must be recomputed explicitly afterward.
    let parent_ids: Vec<i64> = media_items::Entity::find()
        .filter(media_items::Column::Id.is_in(ids.iter().copied()))
        .filter(media_items::Column::ParentId.is_not_null())
        .select_only()
        .column(media_items::Column::ParentId)
        .distinct()
        .into_tuple::<Option<i64>>()
        .all(orm())
        .await
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .collect();

    // Collect every usenet info_hash the about-to-be-deleted subtree
    // references before the cascade runs. `media_items.parent_id` and
    // `filesystem_entries.media_item_id` are both `ON DELETE CASCADE`, so a
    // show delete removes seasons/episodes/entries at the DB level directly —
    // never going through `streams::delete_filesystem_entry`, which is where
    // the equivalent orphan check normally lives. Without this, a deleted
    // show's info_hashes are left permanently cached in `usenet_meta`, and a
    // later re-scrape that lands back on the same (deterministic-hash)
    // release silently reuses whatever was parsed before — skipping every
    // ingest-time check, including PAR2 block verification — instead of
    // re-validating it.
    let info_hashes: Vec<String> = orm()
        .query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "WITH RECURSIVE descendants AS ( \
                 SELECT id FROM media_items WHERE id = ANY($1) \
                 UNION ALL \
                 SELECT mi.id FROM media_items mi \
                 JOIN descendants d ON mi.parent_id = d.id \
             ) \
             SELECT DISTINCT fe.usenet_info_hash AS info_hash \
             FROM filesystem_entries fe \
             JOIN descendants d ON fe.media_item_id = d.id \
             WHERE fe.usenet_info_hash IS NOT NULL",
            [ids.clone().into()],
        ))
        .await?
        .into_iter()
        .filter_map(|row| row.try_get::<String>("", "info_hash").ok())
        .collect();

    let result = media_items::Entity::delete_many()
        .filter(media_items::Column::Id.is_in(ids.iter().copied()))
        .exec(orm())
        .await?;

    for info_hash in &info_hashes {
        if let Err(e) = streams::delete_orphaned_usenet_meta(info_hash).await {
            tracing::warn!(info_hash, error = %e, "failed to clean up orphaned usenet_meta");
        }
    }

    // Prune item_requests no longer referenced by any media_item. The
    // `NOT IN (SELECT ...)` correlated subquery stays raw inside the filter.
    if let Err(e) = item_requests::Entity::delete_many()
        .filter(Expr::cust(
            "id NOT IN ( \
                 SELECT DISTINCT item_request_id FROM media_items \
                 WHERE item_request_id IS NOT NULL \
             )",
        ))
        .exec(orm())
        .await
    {
        tracing::warn!(error = %e, "failed to prune orphaned item_requests");
    }

    state::force_recompute(&parent_ids).await?;

    Ok(result.rows_affected)
}
