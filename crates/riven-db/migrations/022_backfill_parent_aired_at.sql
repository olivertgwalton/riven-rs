-- Auto-backfill aired_at and year on parent Season/Show when episode 1's date
-- becomes known. Replaces a per-call-site responsibility with an unconditional
-- DB-level invariant: if a child's air date is set first and the parent has none,
-- the parent inherits it.
--
-- Recursion-safe: the trigger only fires on UPDATE OF aired_at and only acts when
-- the *child*'s episode_number = 1 (or season_number = 1 when propagating up to a
-- show). Updating a parent row sets aired_at on a season or show, which has neither
-- an episode_number = 1 (it's null on those item types) nor a triggering child, so
-- the chain terminates.

CREATE OR REPLACE FUNCTION backfill_parent_aired_at() RETURNS TRIGGER AS $$
DECLARE
    season_id    BIGINT;
    season_num   INT;
    season_parent BIGINT;
BEGIN
    -- Only propagate up from episode 1.
    IF NEW.item_type <> 'episode'
       OR NEW.episode_number IS DISTINCT FROM 1
       OR NEW.aired_at IS NULL
       OR NEW.parent_id IS NULL THEN
        RETURN NEW;
    END IF;

    -- Backfill the season (NEW.parent_id) if it has no aired_at.
    UPDATE media_items
       SET aired_at = NEW.aired_at,
           aired_at_utc = COALESCE(aired_at_utc, NEW.aired_at_utc),
           year = COALESCE(year, EXTRACT(year FROM NEW.aired_at)::int)
     WHERE id = NEW.parent_id
       AND aired_at IS NULL
     RETURNING id, season_number, parent_id
          INTO season_id, season_num, season_parent;

    -- If we just backfilled season 1, propagate to the show as well.
    IF FOUND AND season_num = 1 AND season_parent IS NOT NULL THEN
        UPDATE media_items
           SET aired_at = NEW.aired_at,
               aired_at_utc = COALESCE(aired_at_utc, NEW.aired_at_utc),
               year = COALESCE(year, EXTRACT(year FROM NEW.aired_at)::int)
         WHERE id = season_parent
           AND aired_at IS NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS backfill_parent_aired_at_trg ON media_items;

CREATE TRIGGER backfill_parent_aired_at_trg
AFTER INSERT OR UPDATE OF aired_at ON media_items
FOR EACH ROW
WHEN (NEW.aired_at IS NOT NULL)
EXECUTE FUNCTION backfill_parent_aired_at();
