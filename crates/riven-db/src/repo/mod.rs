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
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait, QueryFilter, QuerySelect,
    Statement,
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
        .col_expr(media_items::Column::State, Expr::value(MediaItemState::Indexed))
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
        .col_expr(media_items::Column::State, Expr::value(MediaItemState::Paused))
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

    let result = media_items::Entity::delete_many()
        .filter(media_items::Column::Id.is_in(ids.iter().copied()))
        .exec(orm())
        .await?;

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
