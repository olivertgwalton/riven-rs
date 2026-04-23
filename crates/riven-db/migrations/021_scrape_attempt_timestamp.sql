-- Dedicated "last scrape attempt" timestamp, separate from `updated_at`.
-- `updated_at` is touched by many unrelated writes (state cascades, bulk
-- resets, etc.), which inadvertently slides the scrape-cooldown window.
-- This column is written only when a scrape attempt fails, so the cooldown
-- filter in `get_pending_items_for_retry` / `get_episodes_ready_for_scraping`
-- reflects actual scrape history.
ALTER TABLE media_items
    ADD COLUMN IF NOT EXISTS last_scrape_attempt_at TIMESTAMPTZ;

-- Seed existing stuck items so their first cooldown window starts from the
-- most recent known write rather than the epoch.
UPDATE media_items
SET last_scrape_attempt_at = COALESCE(updated_at, created_at)
WHERE failed_attempts > 0
  AND last_scrape_attempt_at IS NULL;

-- Covers the cooldown filter + ORDER BY used by the retry scheduler.
CREATE INDEX IF NOT EXISTS idx_media_items_scrape_retry
    ON media_items (state, item_type, failed_attempts, last_scrape_attempt_at)
    WHERE is_requested = true;
