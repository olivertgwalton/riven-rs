-- Persistent ("permanent") stream blacklisting. Normally a scrape clears an
-- item's blacklist so all releases are reconsidered; but a release confirmed
-- broken by the health check (missing data / unable to ingest) must NOT come
-- back on the next re-grab. Permanent entries survive the scrape-time clear.
ALTER TABLE media_item_blacklisted_streams
    ADD COLUMN IF NOT EXISTS permanent BOOLEAN NOT NULL DEFAULT false;
