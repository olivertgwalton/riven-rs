//! Per-indexer query and grab counters for the dashboard.

use async_graphql::{Context, Object, Result, SimpleObject};

/// Lifetime totals for one configured indexer.
#[derive(SimpleObject)]
pub struct IndexerStats {
    /// The indexer's configured name, as it appears in the newznab settings.
    pub indexer: String,
    /// Release searches issued, counted per request (a paged search is
    /// several).
    pub search_queries: i64,
    /// Capability probes (`t=caps`) issued.
    pub caps_queries: i64,
    /// Releases taken from this indexer that ingested and were picked as the
    /// item's download.
    pub successful_grabs: i64,
}

#[derive(Default)]
pub struct IndexerStatsQuery;

#[Object]
impl IndexerStatsQuery {
    /// Query and grab totals per indexer, busiest first. Counters are flushed
    /// from memory once a minute, so the newest activity can lag by that much.
    async fn indexer_stats(&self, _ctx: &Context<'_>) -> Result<Vec<IndexerStats>> {
        Ok(riven_db::repo::list_indexer_stats()
            .await?
            .into_iter()
            .map(|row| IndexerStats {
                indexer: row.indexer,
                search_queries: row.search_queries,
                caps_queries: row.caps_queries,
                successful_grabs: row.successful_grabs,
            })
            .collect())
    }
}
