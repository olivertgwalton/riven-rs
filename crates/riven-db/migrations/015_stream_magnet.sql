ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS magnet TEXT NOT NULL DEFAULT '';

UPDATE streams
SET magnet = CONCAT('magnet:?xt=urn:btih:', LOWER(info_hash))
WHERE magnet = '';
