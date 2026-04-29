-- Make `media_items.state` derived. The application no longer writes the
-- column except for user-driven sticky transitions (Paused). Every
-- fact-changing write — failed_attempts bump, stream link/blacklist, media
-- filesystem entry insert/delete — fires a trigger that recomputes.
--
-- Three pieces:
--   1. `media_item_aggregate_states(item, child_states[])` — parent-from-children rollup.
--   2. `media_item_state_compute(item_id, max_attempts)` — full leaf+rollup state derivation.
--   3. `media_item_state_recompute(item_id)` — read max_attempts from settings, run compute,
--      update if changed, recurse to parent.
-- Triggers wire the recompute to the relevant writes.

-- ── Helper: parent-from-children rollup ──────────────────────────────────────
CREATE OR REPLACE FUNCTION media_item_aggregate_states(
    item media_items,
    states media_item_state[]
) RETURNS media_item_state
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF states IS NULL OR array_length(states, 1) IS NULL THEN
        RETURN NULL;
    END IF;

    -- Sticky states on the parent itself short-circuit the rollup.
    IF item.state IN ('paused', 'failed') THEN
        RETURN item.state;
    END IF;

    -- All children share a propagable state → propagate.
    IF NOT EXISTS (SELECT 1 FROM unnest(states) s WHERE s != 'paused') THEN
        RETURN 'paused';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM unnest(states) s WHERE s != 'failed') THEN
        RETURN 'failed';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM unnest(states) s WHERE s != 'unreleased') THEN
        RETURN 'unreleased';
    END IF;

    -- All completed → ongoing for a continuing show, else completed.
    IF NOT EXISTS (SELECT 1 FROM unnest(states) s WHERE s != 'completed') THEN
        IF item.item_type = 'show' AND item.show_status = 'continuing' THEN
            RETURN 'ongoing';
        END IF;
        RETURN 'completed';
    END IF;

    -- Any ongoing/unreleased child or continuing show → ongoing.
    IF EXISTS (SELECT 1 FROM unnest(states) s WHERE s IN ('ongoing', 'unreleased'))
       OR (item.item_type = 'show' AND item.show_status = 'continuing') THEN
        RETURN 'ongoing';
    END IF;

    -- Any completed/partially_completed child → partially_completed.
    IF EXISTS (SELECT 1 FROM unnest(states) s WHERE s IN ('completed', 'partially_completed')) THEN
        RETURN 'partially_completed';
    END IF;

    IF 'scraped' = ANY(states) THEN
        RETURN 'scraped';
    END IF;

    RETURN NULL;
END;
$$;

-- ── Compute: leaf + rollup ──────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION media_item_state_compute(
    p_item_id BIGINT,
    p_max_attempts INT
) RETURNS media_item_state
LANGUAGE plpgsql
AS $$
DECLARE
    item media_items%ROWTYPE;
    child_states media_item_state[];
    rolled media_item_state;
    has_media BOOLEAN;
    has_streams BOOLEAN;
BEGIN
    SELECT * INTO item FROM media_items WHERE id = p_item_id;
    IF NOT FOUND THEN
        RETURN 'indexed';
    END IF;

    -- Show / Season: try parent rollup first.
    IF item.item_type = 'show' THEN
        SELECT array_agg(state)
          INTO child_states
          FROM media_items
          WHERE parent_id = p_item_id
            AND item_type = 'season'
            AND is_requested = true
            AND is_special = false;
        rolled := media_item_aggregate_states(item, child_states);
        IF rolled IS NOT NULL THEN
            RETURN rolled;
        END IF;
    ELSIF item.item_type = 'season' THEN
        SELECT array_agg(state)
          INTO child_states
          FROM media_items
          WHERE parent_id = p_item_id
            AND item_type = 'episode';
        rolled := media_item_aggregate_states(item, child_states);
        IF rolled IS NOT NULL THEN
            RETURN rolled;
        END IF;
    END IF;

    -- Leaf state derivation.
    IF item.aired_at IS NOT NULL AND item.aired_at > CURRENT_DATE THEN
        RETURN 'unreleased';
    END IF;

    IF item.state IN ('paused', 'failed') THEN
        RETURN item.state;
    END IF;

    IF p_max_attempts > 0 AND item.failed_attempts >= p_max_attempts THEN
        RETURN 'failed';
    END IF;

    -- Movies / episodes can have a media filesystem entry.
    IF item.item_type IN ('movie', 'episode') THEN
        SELECT EXISTS (
            SELECT 1 FROM filesystem_entries
            WHERE media_item_id = p_item_id AND entry_type = 'media'
        ) INTO has_media;
        IF has_media THEN
            RETURN 'completed';
        END IF;
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM media_item_streams ms
        WHERE ms.media_item_id = p_item_id
          AND ms.stream_id NOT IN (
              SELECT stream_id FROM media_item_blacklisted_streams
              WHERE media_item_id = p_item_id
          )
    ) INTO has_streams;
    IF has_streams THEN
        RETURN 'scraped';
    END IF;

    RETURN 'indexed';
END;
$$;

-- ── Recompute: write iff changed, cascade to parent ─────────────────────────
CREATE OR REPLACE FUNCTION media_item_state_recompute(p_item_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    max_attempts INT;
    new_state media_item_state;
    current_state media_item_state;
BEGIN
    -- Snapshot the per-item retry ceiling from the `general` settings blob.
    -- 0 disables the ceiling.
    SELECT COALESCE((value->>'maximum_scrape_attempts')::INT, 0)
      INTO max_attempts
      FROM settings
      WHERE key = 'general';
    IF max_attempts IS NULL THEN
        max_attempts := 0;
    END IF;

    new_state := media_item_state_compute(p_item_id, max_attempts);

    SELECT state INTO current_state
      FROM media_items WHERE id = p_item_id;

    IF current_state IS DISTINCT FROM new_state THEN
        -- The state-cascade trigger handles recomputing the parent.
        UPDATE media_items
           SET state = new_state, updated_at = NOW()
         WHERE id = p_item_id;
    END IF;
END;
$$;

-- ── Triggers ────────────────────────────────────────────────────────────────

-- State changed (any path: app pause, recompute UPDATE, …) → cascade to parent.
-- Recompute on the parent re-reads child states, so this is the cascade
-- mechanism. Bounded by tree depth (episode → season → show).
CREATE OR REPLACE FUNCTION trg_media_items_state_cascade() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.parent_id IS NOT NULL THEN
        PERFORM media_item_state_recompute(NEW.parent_id);
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS media_items_state_cascade ON media_items;
CREATE TRIGGER media_items_state_cascade
    AFTER UPDATE OF state ON media_items
    FOR EACH ROW
    WHEN (OLD.state IS DISTINCT FROM NEW.state)
    EXECUTE FUNCTION trg_media_items_state_cascade();

-- failed_attempts changed (by application) → recompute self.
-- Guarded with WHEN to avoid recursion when recompute itself UPDATEs state.
CREATE OR REPLACE FUNCTION trg_media_items_recompute() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM media_item_state_recompute(NEW.id);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS media_items_failed_attempts_changed ON media_items;
CREATE TRIGGER media_items_failed_attempts_changed
    AFTER UPDATE OF failed_attempts ON media_items
    FOR EACH ROW
    WHEN (OLD.failed_attempts IS DISTINCT FROM NEW.failed_attempts)
    EXECUTE FUNCTION trg_media_items_recompute();

-- show_status / aired_at / is_requested / is_special change can flip rollup.
DROP TRIGGER IF EXISTS media_items_inputs_changed ON media_items;
CREATE TRIGGER media_items_inputs_changed
    AFTER UPDATE OF show_status, aired_at, is_requested, is_special ON media_items
    FOR EACH ROW
    WHEN (
        OLD.show_status     IS DISTINCT FROM NEW.show_status
     OR OLD.aired_at        IS DISTINCT FROM NEW.aired_at
     OR OLD.is_requested    IS DISTINCT FROM NEW.is_requested
     OR OLD.is_special      IS DISTINCT FROM NEW.is_special
    )
    EXECUTE FUNCTION trg_media_items_recompute();

-- New row inserted → recompute (covers create_episode/season cases).
CREATE OR REPLACE FUNCTION trg_media_items_insert_recompute() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM media_item_state_recompute(NEW.id);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS media_items_inserted ON media_items;
CREATE TRIGGER media_items_inserted
    AFTER INSERT ON media_items
    FOR EACH ROW
    EXECUTE FUNCTION trg_media_items_insert_recompute();

-- Stream link added/removed.
CREATE OR REPLACE FUNCTION trg_media_item_streams_recompute() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM media_item_state_recompute(NEW.media_item_id);
    ELSE
        PERFORM media_item_state_recompute(OLD.media_item_id);
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS media_item_streams_changed ON media_item_streams;
CREATE TRIGGER media_item_streams_changed
    AFTER INSERT OR DELETE ON media_item_streams
    FOR EACH ROW
    EXECUTE FUNCTION trg_media_item_streams_recompute();

-- Blacklist entry added/removed.
DROP TRIGGER IF EXISTS media_item_blacklisted_streams_changed ON media_item_blacklisted_streams;
CREATE TRIGGER media_item_blacklisted_streams_changed
    AFTER INSERT OR DELETE ON media_item_blacklisted_streams
    FOR EACH ROW
    EXECUTE FUNCTION trg_media_item_streams_recompute();

-- Filesystem entry insert/delete/update of media type → recompute owner.
CREATE OR REPLACE FUNCTION trg_filesystem_entries_recompute() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.entry_type = 'media' THEN
            PERFORM media_item_state_recompute(NEW.media_item_id);
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD.entry_type = 'media' THEN
            PERFORM media_item_state_recompute(OLD.media_item_id);
        END IF;
    ELSE
        IF OLD.entry_type = 'media' OR NEW.entry_type = 'media' THEN
            PERFORM media_item_state_recompute(NEW.media_item_id);
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS filesystem_entries_changed ON filesystem_entries;
CREATE TRIGGER filesystem_entries_changed
    AFTER INSERT OR UPDATE OR DELETE ON filesystem_entries
    FOR EACH ROW
    EXECUTE FUNCTION trg_filesystem_entries_recompute();

-- ── Helper: unpause ─────────────────────────────────────────────────────────
-- `Paused` is the only sticky state the application authors. Coming back out
-- of it requires resetting the column to a non-sticky placeholder and asking
-- the recompute to derive the real state from current facts.
CREATE OR REPLACE FUNCTION media_item_unpause(p_item_ids BIGINT[])
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    item_id BIGINT;
BEGIN
    UPDATE media_items
       SET state = 'indexed', updated_at = NOW()
     WHERE id = ANY(p_item_ids) AND state = 'paused';
    FOREACH item_id IN ARRAY p_item_ids LOOP
        PERFORM media_item_state_recompute(item_id);
    END LOOP;
END;
$$;

-- ── One-time backfill ────────────────────────────────────────────────────────
-- Recompute every item once so the state column reflects current facts under
-- the new derivation rules (in particular the failed-attempts ceiling, which
-- the application previously enforced imperatively only at increment time).
DO $$
DECLARE
    item_id BIGINT;
BEGIN
    -- Bottom-up: episodes → seasons → movies → shows (so parent rollups see
    -- already-correct child states).
    FOR item_id IN
        SELECT id FROM media_items
         WHERE item_type = 'episode'
         ORDER BY id
    LOOP
        PERFORM media_item_state_recompute(item_id);
    END LOOP;
    FOR item_id IN
        SELECT id FROM media_items
         WHERE item_type IN ('season', 'movie')
         ORDER BY id
    LOOP
        PERFORM media_item_state_recompute(item_id);
    END LOOP;
    FOR item_id IN
        SELECT id FROM media_items
         WHERE item_type = 'show'
         ORDER BY id
    LOOP
        PERFORM media_item_state_recompute(item_id);
    END LOOP;
END;
$$;
