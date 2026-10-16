-- Episode numbering as release groups use it (from TheXEM), when it differs
-- from or is confirmed against TVDB's. NULL when unknown.
ALTER TABLE media_items
    ADD COLUMN IF NOT EXISTS scene_season_number INTEGER,
    ADD COLUMN IF NOT EXISTS scene_episode_number INTEGER;
