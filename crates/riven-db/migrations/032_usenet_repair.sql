-- Auto-repair bookkeeping for usenet health (altmount-style Repair Engine):
-- track how many automatic re-grabs a file has had, when the last one ran, and
-- when the next is allowed (exponential backoff). Cleared when a file returns
-- healthy; capped so a permanently-broken release isn't re-grabbed forever.
ALTER TABLE usenet_file_health
    ADD COLUMN IF NOT EXISTS repair_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_repair_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_repair_at  TIMESTAMPTZ;

-- Repair worker picks files that are due (next_repair_at in the past / null).
CREATE INDEX IF NOT EXISTS idx_usenet_file_health_next_repair
    ON usenet_file_health (next_repair_at);
