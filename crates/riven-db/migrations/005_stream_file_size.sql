-- Store the known file size (bytes) for a stream, learned on the first download attempt.
-- Used to pre-filter streams at scraping time before they enter the ranked candidate pool.
ALTER TABLE streams ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT;
