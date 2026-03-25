-- Unique partial indexes so seasons and episodes can be upserted on re-index
-- without creating duplicate rows.
CREATE UNIQUE INDEX IF NOT EXISTS idx_seasons_unique
    ON media_items (parent_id, season_number)
    WHERE item_type = 'season';

CREATE UNIQUE INDEX IF NOT EXISTS idx_episodes_unique
    ON media_items (parent_id, episode_number)
    WHERE item_type = 'episode';
