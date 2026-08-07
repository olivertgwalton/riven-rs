//! Per-indexer query and grab accounting. Written by the flusher, read by the
//! dashboard.

use anyhow::Result;
use riven_core::entities::indexer_stats;
use riven_core::indexer_stats::IndexerCounters;
use sea_orm::ActiveValue::Set;
use sea_orm::ExprTrait;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{EntityTrait, QueryOrder};

pub use indexer_stats::Model as IndexerStats;

/// Add one indexer's counter delta to its lifetime totals. No-op for an empty
/// delta — the flusher already filters those, but it keeps this callable from
/// anywhere.
pub async fn add_indexer_stats(indexer: &str, delta: IndexerCounters) -> Result<()> {
    if delta.is_empty() {
        return Ok(());
    }

    indexer_stats::Entity::insert(indexer_stats::ActiveModel {
        indexer: Set(indexer.to_owned()),
        search_queries: Set(delta.search_queries),
        caps_queries: Set(delta.caps_queries),
        successful_grabs: Set(delta.successful_grabs),
        updated_at: Set(chrono::Utc::now().fixed_offset()),
    })
    .on_conflict(
        OnConflict::column(indexer_stats::Column::Indexer)
            .value(
                indexer_stats::Column::SearchQueries,
                Expr::col((indexer_stats::Entity, indexer_stats::Column::SearchQueries))
                    .add(delta.search_queries),
            )
            .value(
                indexer_stats::Column::CapsQueries,
                Expr::col((indexer_stats::Entity, indexer_stats::Column::CapsQueries))
                    .add(delta.caps_queries),
            )
            .value(
                indexer_stats::Column::SuccessfulGrabs,
                Expr::col((
                    indexer_stats::Entity,
                    indexer_stats::Column::SuccessfulGrabs,
                ))
                .add(delta.successful_grabs),
            )
            .value(indexer_stats::Column::UpdatedAt, Expr::cust("now()"))
            .to_owned(),
    )
    .exec(crate::orm())
    .await?;
    Ok(())
}

/// Lifetime totals per indexer, busiest first.
pub async fn list_indexer_stats() -> Result<Vec<IndexerStats>> {
    Ok(indexer_stats::Entity::find()
        .order_by_desc(indexer_stats::Column::SearchQueries)
        .all(crate::orm())
        .await?)
}
