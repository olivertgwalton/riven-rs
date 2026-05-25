-- Per-title usenet health: tracks segment availability for each usenet-backed
-- media file so the UI can show which titles are at risk (missing articles)
-- and offer a re-grab. Populated by a background availability scanner that
-- STAT-samples each file's segments across providers.
CREATE TABLE IF NOT EXISTS usenet_file_health (
    info_hash         TEXT    NOT NULL,
    file_index        INTEGER NOT NULL,
    media_item_id     BIGINT  REFERENCES media_items (id) ON DELETE CASCADE,
    -- 'healthy' | 'unhealthy' | 'unknown' | 'checking'
    status            TEXT    NOT NULL DEFAULT 'unknown',
    total_segments    INTEGER NOT NULL DEFAULT 0,
    sampled_segments  INTEGER NOT NULL DEFAULT 0,
    missing_segments  INTEGER NOT NULL DEFAULT 0,
    error_segments    INTEGER NOT NULL DEFAULT 0,
    checked_at        TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (info_hash, file_index)
);

-- Scanner picks the least-recently-checked files first (never-checked sort first).
CREATE INDEX IF NOT EXISTS idx_usenet_file_health_checked
    ON usenet_file_health (checked_at ASC NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_usenet_file_health_media_item
    ON usenet_file_health (media_item_id);
