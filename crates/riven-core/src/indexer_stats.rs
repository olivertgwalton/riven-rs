//! Per-indexer query and grab accounting.
//!
//! Counters live in process and are bumped on the request path, where the
//! indexer's name is the only place it is known. A flusher in the app
//! periodically drains them into `indexer_stats`, which is what the dashboard
//! reads — writing a row per API request would put a database round trip in
//! front of every page of every scrape.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// What an indexer was asked. Disjoint on purpose: a request is one kind or
/// the other, so the two can be stacked in a chart without double-counting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    /// A release search (`t=movie`, `t=tvsearch`, `t=search`), one per page.
    Search,
    /// A capabilities probe (`t=caps`) — the request that also proves the API
    /// key still works.
    Caps,
}

/// Counters for one indexer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexerCounters {
    pub search_queries: i64,
    pub caps_queries: i64,
    pub successful_grabs: i64,
}

impl IndexerCounters {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    fn merge(&mut self, other: Self) {
        self.search_queries += other.search_queries;
        self.caps_queries += other.caps_queries;
        self.successful_grabs += other.successful_grabs;
    }
}

fn counters() -> &'static Mutex<HashMap<String, IndexerCounters>> {
    static COUNTERS: OnceLock<Mutex<HashMap<String, IndexerCounters>>> = OnceLock::new();
    COUNTERS.get_or_init(Default::default)
}

fn bump(indexer: &str, apply: impl FnOnce(&mut IndexerCounters)) {
    if indexer.is_empty() {
        return;
    }
    let Ok(mut map) = counters().lock() else {
        return;
    };
    apply(map.entry(indexer.to_owned()).or_default());
}

/// Record one request issued to an indexer.
pub fn record_query(indexer: &str, kind: QueryKind) {
    bump(indexer, |c| match kind {
        QueryKind::Search => c.search_queries += 1,
        QueryKind::Caps => c.caps_queries += 1,
    });
}

/// Record a successful grab: a release taken from this indexer that ingested,
/// verified, and was picked as the item's download.
pub fn record_successful_grab(indexer: &str) {
    bump(indexer, |c| c.successful_grabs += 1);
}

/// Take everything accumulated so far, leaving the counters empty.
pub fn drain() -> Vec<(String, IndexerCounters)> {
    let Ok(mut map) = counters().lock() else {
        return Vec::new();
    };
    map.drain().filter(|(_, c)| !c.is_empty()).collect()
}

/// Put a drained delta back after a failed flush, so a database blip loses
/// nothing but the flush interval.
pub fn restore(indexer: &str, delta: IndexerCounters) {
    bump(indexer, |c| c.merge(delta));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialised because the counters are process-global: two tests draining
    /// concurrently would each see the other's writes.
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        static TEST_LOCK: Mutex<()> = Mutex::new(());
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn queries_and_grabs_accumulate_per_indexer() {
        let _guard = lock();
        drain();

        record_query("geek", QueryKind::Search);
        record_query("geek", QueryKind::Search);
        record_query("geek", QueryKind::Caps);
        record_successful_grab("geek");
        record_successful_grab("slug");

        let mut drained = drain();
        drained.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            drained,
            vec![
                (
                    "geek".to_string(),
                    IndexerCounters {
                        search_queries: 2,
                        caps_queries: 1,
                        successful_grabs: 1,
                    }
                ),
                (
                    "slug".to_string(),
                    IndexerCounters {
                        successful_grabs: 1,
                        ..Default::default()
                    }
                ),
            ]
        );
    }

    /// The flusher's failure path: a delta that could not be written has to
    /// survive to the next tick rather than being silently dropped.
    #[test]
    fn drain_empties_and_restore_puts_a_delta_back() {
        let _guard = lock();
        drain();

        record_query("geek", QueryKind::Search);
        let drained = drain();
        assert!(drain().is_empty(), "drain must leave nothing behind");

        for (indexer, delta) in drained {
            restore(&indexer, delta);
        }
        assert_eq!(
            drain(),
            vec![(
                "geek".to_string(),
                IndexerCounters {
                    search_queries: 1,
                    ..Default::default()
                }
            )]
        );
    }
}
