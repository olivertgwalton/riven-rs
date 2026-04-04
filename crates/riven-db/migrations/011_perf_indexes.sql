-- Performance indexes: close the gaps that cause sequential scans on
-- hot-path queries (stream fetches, state checks, episode/season listings).

-- Composite index for listing episodes and seasons by parent + type.
-- Replaces the separate idx_media_items_parent index on the two most common
-- child queries: list_episodes (parent_id + item_type = 'episode') and
-- list_seasons (parent_id + item_type = 'season').
CREATE INDEX IF NOT EXISTS idx_media_items_parent_type
    ON media_items(parent_id, item_type);

-- Composite index for the EXISTS subquery in compute_state:
-- EXISTS(SELECT 1 FROM filesystem_entries WHERE media_item_id = $1 AND entry_type = 'media')
CREATE INDEX IF NOT EXISTS idx_fs_entries_item_type_composite
    ON filesystem_entries(media_item_id, entry_type);

-- Partial unique index required for the single-statement upsert in
-- create_media_entry. Replaces the SELECT + INSERT/UPDATE two-round-trip
-- pattern with one ON CONFLICT DO UPDATE.
CREATE UNIQUE INDEX IF NOT EXISTS idx_fs_entries_media_path_unique
    ON filesystem_entries(media_item_id, path)
    WHERE entry_type = 'media';

-- Expression index on the resolution field extracted from parsed_data JSONB.
-- The stream sort ORDER BY uses this expression on every stream fetch;
-- without this index Postgres does a sequential scan + sort on large stream tables.
CREATE INDEX IF NOT EXISTS idx_streams_resolution
    ON streams((parsed_data->>'resolution'));

-- Covering index on media_item_streams(stream_id) for the reverse-lookup
-- subquery pattern used in blacklist checks.
CREATE INDEX IF NOT EXISTS idx_mis_stream_id
    ON media_item_streams(stream_id);
