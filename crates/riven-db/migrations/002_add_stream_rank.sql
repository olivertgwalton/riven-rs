-- Add rank column to streams table for torrent ranking
ALTER TABLE streams ADD COLUMN rank BIGINT;

-- Index for ordering streams by rank
CREATE INDEX idx_streams_rank ON streams(rank DESC NULLS LAST);
