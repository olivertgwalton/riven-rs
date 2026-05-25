-- Usenet download traffic accounting, per provider. Lifetime cumulative totals
-- (survive restarts) plus per-day buckets for usage trends. Fed by a flusher
-- that periodically writes the pool's in-memory session deltas here.
CREATE TABLE IF NOT EXISTS usenet_provider_traffic (
    host                TEXT PRIMARY KEY,
    bytes_downloaded    BIGINT NOT NULL DEFAULT 0,
    articles_downloaded BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS usenet_traffic_daily (
    day                 DATE NOT NULL,
    host                TEXT NOT NULL,
    bytes_downloaded    BIGINT NOT NULL DEFAULT 0,
    articles_downloaded BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, host)
);

CREATE INDEX IF NOT EXISTS idx_usenet_traffic_daily_day
    ON usenet_traffic_daily (day);
