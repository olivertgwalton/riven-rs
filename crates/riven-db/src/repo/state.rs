//! State derivation lives in the database (see migration `023_state_triggers.sql`).
//!
//! Triggers on every fact-changing write — `media_items.failed_attempts`,
//! `media_item_streams`, `media_item_blacklisted_streams`, `filesystem_entries`,
//! and inserts on `media_items` — invoke `media_item_state_recompute(id)`,
//! which reads `maximum_scrape_attempts` from `settings`, derives the new
//! state, writes it iff different, and cascades to the parent via the
//! `media_items_state_cascade` trigger.
//!
//! The application no longer authors `state` except for the `Paused` sticky
//! transition (and its inverse via [`unpause_items`]). Everything else is
//! derived; touching the underlying facts is sufficient.

use anyhow::Result;
use sqlx::PgPool;

/// User-driven exit from `Paused`. The DB function flips the column to
/// `Indexed` (a non-sticky placeholder) and invokes the recompute, which
/// derives the real post-pause state from current facts.
pub async fn unpause_items(pool: &PgPool, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("SELECT media_item_unpause($1)")
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(())
}

/// Force a recompute for the given ids. Should never be necessary in the
/// application path (triggers cover every fact-changing write); kept for
/// admin tools and one-off backfills after data fix-ups.
pub async fn force_recompute(pool: &PgPool, ids: &[i64]) -> Result<()> {
    for id in ids {
        sqlx::query("SELECT media_item_state_recompute($1)")
            .bind(id)
            .execute(pool)
            .await?;
    }
    Ok(())
}
