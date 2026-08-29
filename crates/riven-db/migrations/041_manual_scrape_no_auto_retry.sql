-- A user who manually resolves a specific release via Manual Scrape (a picked
-- discovery result, a pasted magnet/hash, or a pasted NZB URL) has already
-- made the call the automatic retry scheduler exists to make on its own.
-- Letting `get_pending_items_for_retry` keep chasing the item afterwards can
-- silently pick something different and download it over/alongside what the
-- user actually chose. This flag opts an item out of that automatic loop
-- without touching its state, so manual re-scrape/retry actions still work.
ALTER TABLE media_items
    ADD COLUMN IF NOT EXISTS manual_scrape_only BOOLEAN NOT NULL DEFAULT false;
