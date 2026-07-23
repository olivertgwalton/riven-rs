//! Postgres-backed durable store for `NzbMeta`.
//!
//! Schema and rationale: see migration `028_usenet_meta.sql`. The streamer's
//! only persistence concern is "given an info_hash, can I rebuild the segment
//! map." Postgres holds that record for as long as it's relevant; the
//! in-memory LRU above absorbs hot reads. No TTL, no Redis hop.

use riven_core::entities::{filesystem_entries, usenet_meta};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter, Statement,
};

use super::{NzbMeta, StreamerError};

/// Load and deserialize one release's segment map.
///
/// Deliberately *not* `Entity::find_by_id(..).one(db)`: that materializes the
/// `jsonb` column as a `serde_json::Value` tree before `from_value` walks it
/// into `NzbMeta`, so a big multi-file release is paid for twice — and the
/// intermediate tree is by far the more expensive half (every string, map and
/// vec node individually boxed). A season pack observed here persists as
/// 80.6 MB of JSON across 387 files; the `Value` tree for it runs to the high
/// hundreds of MB, which is the single largest transient allocation in the
/// process. Selecting the column as text and streaming `from_str` straight
/// into the target struct skips the tree entirely, and `spawn_blocking` keeps
/// a parse that size off a reactor worker.
pub(super) async fn load(
    db: &DatabaseConnection,
    info_hash: &str,
) -> Result<Option<NzbMeta>, StreamerError> {
    let stmt = Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT meta::text FROM usenet_meta WHERE info_hash = $1",
        [info_hash.to_owned().into()],
    );
    let Some(row) = db.query_one(stmt).await? else {
        return Ok(None);
    };
    let json: String = row.try_get_by_index(0)?;
    let meta =
        tokio::task::spawn_blocking(move || serde_json::from_str::<NzbMeta>(&json)).await??;
    Ok(Some(meta))
}

pub(super) async fn store(
    db: &DatabaseConnection,
    info_hash: &str,
    meta: &NzbMeta,
) -> Result<(), StreamerError> {
    let now = chrono::Utc::now().fixed_offset();
    let value = serde_json::to_value(meta)?;
    usenet_meta::Entity::insert(usenet_meta::ActiveModel {
        info_hash: Set(info_hash.to_string()),
        meta: Set(value),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::column(usenet_meta::Column::InfoHash)
            .update_columns([usenet_meta::Column::Meta, usenet_meta::Column::UpdatedAt])
            .to_owned(),
    )
    .exec(db)
    .await?;
    Ok(())
}

/// Propagate a corrected `total_size` (e.g. from the `Direct` offset
/// auto-heal in `load_meta`) into `filesystem_entries.file_size` for every
/// library entry pointing at this `(info_hash, file_index)`. The two tables
/// are otherwise independent — `file_size` is only written once, at grab
/// time, from whatever size estimate `NzbMeta` had then — so without this
/// they silently drift apart whenever the meta's size estimate improves,
/// leaving the FUSE mount advertising a size larger than the source can
/// actually serve and every tail read past the real end failing with EIO.
pub(super) async fn sync_file_size(
    db: &DatabaseConnection,
    info_hash: &str,
    file_index: usize,
    file_size: u64,
) -> Result<u64, StreamerError> {
    let result = filesystem_entries::Entity::update_many()
        .set(filesystem_entries::ActiveModel {
            file_size: Set(file_size as i64),
            ..Default::default()
        })
        .filter(filesystem_entries::Column::UsenetInfoHash.eq(info_hash))
        .filter(filesystem_entries::Column::UsenetFileIndex.eq(file_index as i32))
        .exec(db)
        .await?;
    Ok(result.rows_affected)
}
