-- Track which ranking profile produced each filesystem entry.
-- Used by the multi-version download flow to know which profiles still
-- need a downloaded version and to generate distinct VFS paths.
ALTER TABLE filesystem_entries
    ADD COLUMN ranking_profile_name TEXT;
