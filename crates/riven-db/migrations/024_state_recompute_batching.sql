-- Per-transaction batching for state recompute.
--
-- Migration 023 made every fact-changing write fire a row-level recompute.
-- That's correct but wasteful: 100 stream inserts on the same media_item =
-- 100 recomputes that all derive the same final state. The ORM-based
-- equivalent in riven-ts (MikroORM `onFlush`) recomputes once per affected
-- entity per transaction, regardless of how many individual writes happened.
--
-- This migration achieves the same in raw Postgres via:
--   1. An UNLOGGED dirty-queue table keyed `(txid, item_id)`.
--   2. Per-row "mark dirty" triggers that INSERT … ON CONFLICT DO NOTHING.
--      Within one transaction, only the first write per item succeeds —
--      the rest are silent no-ops (and the constraint trigger only fires for
--      actual inserts).
--   3. A `DEFERRABLE INITIALLY DEFERRED` CONSTRAINT TRIGGER on the queue
--      that runs `media_item_state_recompute(id)` at COMMIT and removes
--      the queue row.
--
-- Net: one recompute per unique item per transaction, matching the ORM
-- subscriber's behaviour. No application-side bulking required.

-- ── Dirty queue ─────────────────────────────────────────────────────────────
CREATE UNLOGGED TABLE IF NOT EXISTS _media_item_state_dirty (
    txid    BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    PRIMARY KEY (txid, item_id)
);

CREATE OR REPLACE FUNCTION mark_state_dirty(p_item_id BIGINT) RETURNS VOID
LANGUAGE plpgsql AS $$
BEGIN
    IF p_item_id IS NULL THEN
        RETURN;
    END IF;
    INSERT INTO _media_item_state_dirty (txid, item_id)
    VALUES (txid_current(), p_item_id)
    ON CONFLICT DO NOTHING;
END;
$$;

-- ── Mark-dirty trigger functions ────────────────────────────────────────────

CREATE OR REPLACE FUNCTION trg_mark_self_dirty() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM mark_state_dirty(NEW.id);
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION trg_mark_state_cascade_dirty() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.parent_id IS NOT NULL THEN
        PERFORM mark_state_dirty(NEW.parent_id);
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION trg_mark_streams_dirty() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM mark_state_dirty(NEW.media_item_id);
    ELSE
        PERFORM mark_state_dirty(OLD.media_item_id);
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION trg_mark_filesystem_dirty() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.entry_type = 'media' THEN
            PERFORM mark_state_dirty(NEW.media_item_id);
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD.entry_type = 'media' THEN
            PERFORM mark_state_dirty(OLD.media_item_id);
        END IF;
    ELSE
        IF OLD.entry_type = 'media' OR NEW.entry_type = 'media' THEN
            PERFORM mark_state_dirty(NEW.media_item_id);
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

-- ── Replace 023's triggers ──────────────────────────────────────────────────
-- The previous triggers called `media_item_state_recompute` directly per row.
-- Now they queue dirty markers; the constraint trigger drains at COMMIT.

DROP TRIGGER IF EXISTS media_items_inserted ON media_items;
CREATE TRIGGER media_items_inserted
    AFTER INSERT ON media_items
    FOR EACH ROW EXECUTE FUNCTION trg_mark_self_dirty();

DROP TRIGGER IF EXISTS media_items_failed_attempts_changed ON media_items;
CREATE TRIGGER media_items_failed_attempts_changed
    AFTER UPDATE OF failed_attempts ON media_items
    FOR EACH ROW
    WHEN (OLD.failed_attempts IS DISTINCT FROM NEW.failed_attempts)
    EXECUTE FUNCTION trg_mark_self_dirty();

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
    EXECUTE FUNCTION trg_mark_self_dirty();

DROP TRIGGER IF EXISTS media_items_state_cascade ON media_items;
CREATE TRIGGER media_items_state_cascade
    AFTER UPDATE OF state ON media_items
    FOR EACH ROW
    WHEN (OLD.state IS DISTINCT FROM NEW.state)
    EXECUTE FUNCTION trg_mark_state_cascade_dirty();

DROP TRIGGER IF EXISTS media_item_streams_changed ON media_item_streams;
CREATE TRIGGER media_item_streams_changed
    AFTER INSERT OR DELETE ON media_item_streams
    FOR EACH ROW EXECUTE FUNCTION trg_mark_streams_dirty();

DROP TRIGGER IF EXISTS media_item_blacklisted_streams_changed ON media_item_blacklisted_streams;
CREATE TRIGGER media_item_blacklisted_streams_changed
    AFTER INSERT OR DELETE ON media_item_blacklisted_streams
    FOR EACH ROW EXECUTE FUNCTION trg_mark_streams_dirty();

DROP TRIGGER IF EXISTS filesystem_entries_changed ON filesystem_entries;
CREATE TRIGGER filesystem_entries_changed
    AFTER INSERT OR UPDATE OR DELETE ON filesystem_entries
    FOR EACH ROW EXECUTE FUNCTION trg_mark_filesystem_dirty();

-- ── Drain at COMMIT ─────────────────────────────────────────────────────────
-- A `DEFERRABLE INITIALLY DEFERRED` constraint trigger fires once per row at
-- COMMIT (or earlier if `SET CONSTRAINTS … IMMEDIATE` is run). The drain
-- function runs the recompute for that item and then deletes the queue row.
--
-- If a recompute UPDATE flips the item's state, that fires
-- `media_items_state_cascade` which inserts the parent into the queue,
-- which queues another deferred drain. Postgres iterates deferred triggers
-- to fixed point, so the cascade climbs the parent chain naturally.

CREATE OR REPLACE FUNCTION trg_drain_state_dirty() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM media_item_state_recompute(NEW.item_id);
    DELETE FROM _media_item_state_dirty
        WHERE txid = NEW.txid AND item_id = NEW.item_id;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS drain_state_dirty ON _media_item_state_dirty;
CREATE CONSTRAINT TRIGGER drain_state_dirty
    AFTER INSERT ON _media_item_state_dirty
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trg_drain_state_dirty();
