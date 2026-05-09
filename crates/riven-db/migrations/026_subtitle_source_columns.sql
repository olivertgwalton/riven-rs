-- Add subtitle source attribution columns and a per-language uniqueness
-- constraint so a media item only ever has one subtitle entry per language.

ALTER TABLE filesystem_entries
    ADD COLUMN IF NOT EXISTS source_provider TEXT,
    ADD COLUMN IF NOT EXISTS source_id TEXT;

-- Partial unique index so the constraint only applies to subtitle rows.
-- (Other entry types have NULL `language`, which would otherwise tie up
--  the (media_item_id, NULL) slot.)
CREATE UNIQUE INDEX IF NOT EXISTS idx_fs_entries_subtitle_unique_lang
    ON filesystem_entries (media_item_id, language)
    WHERE entry_type = 'subtitle';
