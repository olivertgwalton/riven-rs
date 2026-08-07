-- Per-indexer query and grab accounting. Lifetime cumulative counters (they
-- survive restarts), fed by a flusher that periodically writes the in-memory
-- deltas the scraper and download paths record.
CREATE TABLE IF NOT EXISTS indexer_stats (
    indexer          TEXT PRIMARY KEY,
    search_queries   BIGINT NOT NULL DEFAULT 0,
    caps_queries     BIGINT NOT NULL DEFAULT 0,
    successful_grabs BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
