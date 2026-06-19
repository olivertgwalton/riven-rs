//! Postgres-backed durable store for `NzbMeta`.
//!
//! Schema and rationale: see migration `028_usenet_meta.sql`. The streamer's
//! only persistence concern is "given an info_hash, can I rebuild the segment
//! map." Postgres holds that record for as long as it's relevant; the
//! in-memory LRU above absorbs hot reads. No TTL, no Redis hop.

use riven_core::entities::usenet_meta;
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::Set;
use sea_orm::{DatabaseConnection, EntityTrait};

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
