-- When episode 1's aired_at is set, backfill the parent season (and grandparent
-- show, if season 1) when they have none. Triggered only by episode rows, so
-- updates to season/show rows can't re-fire — the chain terminates.

CREATE OR REPLACE FUNCTION backfill_parent_aired_at() RETURNS TRIGGER AS $$
DECLARE
    season_id    BIGINT;
    season_num   INT;
    season_parent BIGINT;
BEGIN
    IF NEW.item_type <> 'episode'
       OR NEW.episode_number IS DISTINCT FROM 1
       OR NEW.aired_at IS NULL
       OR NEW.parent_id IS NULL THEN
        RETURN NEW;
    END IF;

    UPDATE media_items
       SET aired_at = NEW.aired_at,
           aired_at_utc = COALESCE(aired_at_utc, NEW.aired_at_utc),
           year = COALESCE(year, EXTRACT(year FROM NEW.aired_at)::int)
     WHERE id = NEW.parent_id
       AND aired_at IS NULL
     RETURNING id, season_number, parent_id
          INTO season_id, season_num, season_parent;

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
