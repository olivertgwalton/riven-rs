//! Postgres-backed durable store for `NzbMeta`.
//!
//! Schema and rationale: see migration `028_usenet_meta.sql`. The streamer's
//! only persistence concern is "given an info_hash, can I rebuild the segment
//! map." Postgres holds that record for as long as it's relevant; the
//! in-memory LRU above absorbs hot reads. No TTL, no Redis hop.

use riven_core::entities::{filesystem_entries, usenet_meta};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};

use super::{NzbMeta, StreamerError};

pub(super) async fn load(
    db: &DatabaseConnection,
    info_hash: &str,
) -> Result<Option<NzbMeta>, StreamerError> {
    let row = usenet_meta::Entity::find_by_id(info_hash.to_string())
        .one(db)
        .await?;
    match row {
        Some(model) => {
            let meta: NzbMeta = serde_json::from_value(model.meta)?;
            Ok(Some(meta))
        }
        None => Ok(None),
    }
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
