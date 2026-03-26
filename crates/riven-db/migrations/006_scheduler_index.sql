-- Composite index for the scheduler's get_items_ready_for_processing query,
-- which runs every 60 s: WHERE state = ? AND item_type = ? AND is_requested = true
-- ORDER BY failed_attempts ASC, created_at ASC LIMIT N
CREATE INDEX idx_media_items_scheduler
    ON media_items (state, item_type, failed_attempts, created_at)
    WHERE is_requested = true;
