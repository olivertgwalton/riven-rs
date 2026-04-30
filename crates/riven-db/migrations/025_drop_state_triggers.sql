-- State derivation moved into Rust (`crates/riven-db/src/repo/state.rs`).
-- Every fact-changing repo writer now calls `state::recompute(pool, &[id])`
-- after its write, replacing the trigger-based recompute introduced in
-- migrations 023 and 024.

-- ── Drop triggers ───────────────────────────────────────────────────────────
DROP TRIGGER IF EXISTS media_items_state_cascade           ON media_items;
DROP TRIGGER IF EXISTS media_items_failed_attempts_changed ON media_items;
DROP TRIGGER IF EXISTS media_items_inputs_changed          ON media_items;
DROP TRIGGER IF EXISTS media_items_inserted                ON media_items;
DROP TRIGGER IF EXISTS media_item_streams_changed          ON media_item_streams;
DROP TRIGGER IF EXISTS media_item_blacklisted_streams_changed
    ON media_item_blacklisted_streams;
DROP TRIGGER IF EXISTS filesystem_entries_changed          ON filesystem_entries;
DROP TRIGGER IF EXISTS drain_state_dirty                   ON _media_item_state_dirty;

-- ── Drop trigger functions ──────────────────────────────────────────────────
DROP FUNCTION IF EXISTS trg_media_items_state_cascade();
DROP FUNCTION IF EXISTS trg_media_items_recompute();
DROP FUNCTION IF EXISTS trg_media_items_insert_recompute();
DROP FUNCTION IF EXISTS trg_media_item_streams_recompute();
DROP FUNCTION IF EXISTS trg_filesystem_entries_recompute();
DROP FUNCTION IF EXISTS trg_mark_self_dirty();
DROP FUNCTION IF EXISTS trg_mark_state_cascade_dirty();
DROP FUNCTION IF EXISTS trg_mark_streams_dirty();
DROP FUNCTION IF EXISTS trg_mark_filesystem_dirty();
DROP FUNCTION IF EXISTS trg_drain_state_dirty();

-- ── Drop the dirty queue and helpers ────────────────────────────────────────
DROP TABLE    IF EXISTS _media_item_state_dirty;
DROP FUNCTION IF EXISTS mark_state_dirty(BIGINT);

-- ── Drop the state-derivation SQL functions ─────────────────────────────────
DROP FUNCTION IF EXISTS media_item_unpause(BIGINT[]);
DROP FUNCTION IF EXISTS media_item_state_recompute(BIGINT);
DROP FUNCTION IF EXISTS media_item_state_compute(BIGINT, INT);
DROP FUNCTION IF EXISTS media_item_aggregate_states(media_items, media_item_state[]);
