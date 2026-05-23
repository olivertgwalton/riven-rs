-- Store the usenet article address explicitly on each media entry instead of
-- encoding it in a loopback `http://127.0.0.1:<port>/usenet/{info_hash}/{idx}`
-- stream URL. The VFS now reads usenet content in-process (no loopback HTTP),
-- so it identifies usenet entries by these columns rather than parsing a fake
-- URL, and the `/usenet/` HTTP route is removed.

ALTER TABLE filesystem_entries
    ADD COLUMN IF NOT EXISTS usenet_info_hash TEXT,
    ADD COLUMN IF NOT EXISTS usenet_file_index INTEGER;

-- Backfill existing usenet entries by parsing their stored loopback URL of the
-- form `.../usenet/{info_hash}/{file_index}`. `substring` with a regex pulls
-- the two trailing path segments; rows that don't match (debrid CDN links,
-- subtitles) are left NULL.
UPDATE filesystem_entries
SET
    usenet_info_hash  = substring(download_url FROM '/usenet/([^/]+)/[0-9]+$'),
    usenet_file_index = substring(download_url FROM '/usenet/[^/]+/([0-9]+)$')::INTEGER
WHERE download_url ~ '/usenet/[^/]+/[0-9]+$'
  AND usenet_info_hash IS NULL;
