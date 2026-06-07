-- Two housekeeping improvements:
--   1. One-time GC of orphan streams — scrape results referenced by nothing.
--   2. Partial-unique indexes on canonical identity, so a race or bad insert
--      path can never create two rows for the same movie/show.
--
-- NOTE: an `updated_at` auto-trigger was considered (riven-ts gets this from
-- its ORM's onUpdate hook) and deliberately REJECTED. In riven-rs `updated_at`
-- on media_items is not a cosmetic timestamp — it doubles as a retry/recovery
-- clock (get_stuck_ongoing_items, reset_failed_items order and filter on it,
-- treating NULL as "never touched, highest priority"). A blanket trigger that
-- bumped it on every incidental column change would reset those recovery
-- timers and could worsen stuck-state recovery. Every write path that should
-- advance updated_at already sets it to NOW() explicitly.

-- ---------------------------------------------------------------------------
-- 1. One-time orphan-stream GC
-- ---------------------------------------------------------------------------
-- A stream is dead weight when it is referenced by no candidate list, no
-- blacklist, no item's active_stream, and no filesystem entry. Such streams
-- are pure cached scrape output and are recreated on the next scrape, so this
-- delete is fully recoverable. (~30k rows / most of the streams table at time
-- of writing.) Ongoing GC runs hourly via the queue Scheduler's cleanup tick;
-- this clears the existing backlog in one shot.
DELETE FROM streams s
 WHERE NOT EXISTS (SELECT 1 FROM media_item_streams m WHERE m.stream_id = s.id)
   AND NOT EXISTS (SELECT 1 FROM media_item_blacklisted_streams b WHERE b.stream_id = s.id)
   AND NOT EXISTS (SELECT 1 FROM media_items i WHERE i.active_stream_id = s.id)
   AND NOT EXISTS (SELECT 1 FROM filesystem_entries f WHERE f.stream_id = s.id);

-- ---------------------------------------------------------------------------
-- 2. Canonical-identity uniqueness
-- ---------------------------------------------------------------------------
-- Movies are identified by tmdb_id, shows by tvdb_id (mirrors how riven-ts
-- types Movie.tmdbId / Show.tvdbId as the required external id). Seasons and
-- episodes are already covered by idx_seasons_unique / idx_episodes_unique on
-- (parent_id, number). Partial + NULL-excluded so unidentified rows are
-- unaffected. If a duplicate currently exists these will fail loudly — verified
-- none exist at authoring time.
CREATE UNIQUE INDEX idx_media_items_movie_tmdb_unique
    ON media_items(tmdb_id)
    WHERE item_type = 'movie' AND tmdb_id IS NOT NULL;

CREATE UNIQUE INDEX idx_media_items_show_tvdb_unique
    ON media_items(tvdb_id)
    WHERE item_type = 'show' AND tvdb_id IS NOT NULL;
