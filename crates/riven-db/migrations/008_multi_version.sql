-- Multi-version support: track which stream produced each filesystem entry,
-- and store the resolution so the download flow can skip already-present versions.

ALTER TABLE filesystem_entries
    ADD COLUMN stream_id  BIGINT REFERENCES streams(id),
    ADD COLUMN resolution TEXT;

-- Backfill resolution for existing entries from already-parsed media_metadata.
UPDATE filesystem_entries
SET resolution = CASE
    WHEN (media_metadata -> 'video' ->> 'resolution_height')::int >= 2160 THEN '2160p'
    WHEN (media_metadata -> 'video' ->> 'resolution_height')::int >= 1440 THEN '1440p'
    WHEN (media_metadata -> 'video' ->> 'resolution_height')::int >= 1080 THEN '1080p'
    WHEN (media_metadata -> 'video' ->> 'resolution_height')::int >=  720 THEN '720p'
    WHEN (media_metadata -> 'video' ->> 'resolution_height')::int >=  480 THEN '480p'
    WHEN (media_metadata -> 'video' ->> 'resolution_height')::int >=  360 THEN '360p'
    ELSE NULL
END
WHERE entry_type = 'media'
  AND media_metadata IS NOT NULL
  AND media_metadata -> 'video' ->> 'resolution_height' IS NOT NULL;
