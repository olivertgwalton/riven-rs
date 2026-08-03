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
    let Some(row) = db.query_one_raw(stmt).await? else {
        return Ok(None);
    };
    let json: String = row.try_get_by_index(0)?;
    let meta =
        tokio::task::spawn_blocking(move || serde_json::from_str::<NzbMeta>(&json)).await??;
    Ok(Some(meta))
}

/// Load and deserialize **one file** of a release's segment map.
///
/// The same argument as [`load`], taken one step further. `load` already avoids
/// the `Value` tree, but it still parses every file in the release to serve a
/// read of one of them — and the release is the wrong unit: a season pack ships
/// one row for the whole season, so playing one episode paid for all of them.
/// Measured on this library: 2 510 rows totalling 4 489 MB, the largest a
/// single 90 MB row holding 520 files, and 15 rows individually larger than the
/// entire `nzb-meta` budget (so they could not be cached at all and were
/// re-parsed on every read).
///
/// `meta -> 'files' -> $2` makes Postgres do the indexing, so what crosses the
/// wire and reaches `from_str` is one file's segments rather than every file's.
/// Returns `Ok(None)` both when the release is absent and when the index is
/// past the end — the caller cannot distinguish, and neither can serve a read.
/// The password is selected alongside because it lives on the release rather
/// than the file, and the RAR read path needs it. Pulling it in the same
/// statement keeps this to one round trip.
pub(super) async fn load_file(
    db: &DatabaseConnection,
    info_hash: &str,
    file_index: usize,
) -> Result<Option<super::FileMeta>, StreamerError> {
    // The volumes are fetched alongside, because `NzbMetaSource::Rar::parts`
    // is `#[serde(skip)]` — the deduplicated document stores them once in
    // `rar_sets` rather than inside each file, so a file on its own cannot
    // carry them. `COALESCE` covers both shapes: the referenced set for a
    // deduplicated row, the inline array for one written before that.
    let stmt = Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT (meta -> 'files' -> $2)::text, meta ->> 'password', \
                COALESCE( \
                  meta -> 'rar_sets' -> \
                    ((meta -> 'files' -> $2 -> 'source' -> 'Rar' ->> 'set')::int), \
                  meta -> 'files' -> $2 -> 'source' -> 'Rar' -> 'parts' \
                )::text \
         FROM usenet_meta WHERE info_hash = $1",
        [
            info_hash.to_owned().into(),
            (i32::try_from(file_index).unwrap_or(i32::MAX)).into(),
        ],
    );
    let Some(row) = db.query_one_raw(stmt).await? else {
        return Ok(None);
    };
    // NULL when the array index is out of range, which `->` reports rather
    // than erroring.
    let Some(json): Option<String> = row.try_get_by_index(0)? else {
        return Ok(None);
    };
    let password: Option<String> = row.try_get_by_index(1)?;
    let parts_json: Option<String> = row.try_get_by_index(2)?;
    let mut file =
        tokio::task::spawn_blocking(move || serde_json::from_str::<super::NzbMetaFile>(&json))
            .await??;
    if let super::NzbMetaSource::Rar { parts, .. } = &mut file.source {
        let loaded: Vec<super::NzbRarPart> = match parts_json {
            Some(raw) => tokio::task::spawn_blocking(move || serde_json::from_str(&raw)).await??,
            None => Vec::new(),
        };
        *parts = std::sync::Arc::new(loaded);
    }
    Ok(Some(super::FileMeta { file, password }))
}

/// Just the `(filename, total_size)` of each file, projected in Postgres.
///
/// The idempotent-ingest check needs to know a release is already stored and
/// what files it yields — not its segment maps. Building the answer server-side
/// keeps a re-scrape from deserialising a document that runs to 145 MB on this
/// library to produce ~56 KB of it.
pub(super) async fn load_file_index(
    db: &DatabaseConnection,
    info_hash: &str,
) -> Result<Option<Vec<super::IngestedFile>>, StreamerError> {
    #[derive(serde::Deserialize)]
    struct Row {
        filename: String,
        total_size: u64,
    }

    let stmt = Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT (SELECT jsonb_agg(jsonb_build_object( \
                    'filename', f -> 'filename', 'total_size', f -> 'total_size') ORDER BY ord) \
                 FROM jsonb_array_elements(meta -> 'files') WITH ORDINALITY t(f, ord))::text \
         FROM usenet_meta WHERE info_hash = $1",
        [info_hash.to_owned().into()],
    );
    let Some(row) = db.query_one_raw(stmt).await? else {
        return Ok(None);
    };
    // NULL when the release stores no files at all.
    let Some(json): Option<String> = row.try_get_by_index(0)? else {
        return Ok(Some(Vec::new()));
    };
    let rows: Vec<Row> = serde_json::from_str(&json)?;
    Ok(Some(
        rows.into_iter()
            .map(|r| super::IngestedFile {
                filename: r.filename,
                total_size: r.total_size,
            })
            .collect(),
    ))
}

/// On-disk size of one stored release, for reporting what a rewrite reclaimed.
pub(super) async fn stored_size(
    db: &DatabaseConnection,
    info_hash: &str,
) -> Result<Option<i64>, StreamerError> {
    let stmt = Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT pg_column_size(meta)::bigint FROM usenet_meta WHERE info_hash = $1",
        [info_hash.to_owned().into()],
    );
    let Some(row) = db.query_one_raw(stmt).await? else {
        return Ok(None);
    };
    Ok(Some(row.try_get_by_index(0)?))
}

/// Releases still stored in an older format, largest first so an interrupted
/// run has already dealt with the rows that matter.
///
/// One comparison against the version marker rather than a scan for a missing
/// field, so this stays cheap to ask repeatedly.
pub(super) async fn outdated_info_hashes(
    db: &DatabaseConnection,
    limit: u32,
) -> Result<Vec<String>, StreamerError> {
    let stmt = Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT info_hash FROM usenet_meta          WHERE COALESCE((meta ->> 'v')::int, 1) < $1          ORDER BY pg_column_size(meta) DESC LIMIT $2",
        [
            (super::meta::META_FORMAT_VERSION as i32).into(),
            (limit as i32).into(),
        ],
    );
    let rows = db.query_all_raw(stmt).await?;
    rows.into_iter()
        .map(|r| r.try_get_by_index::<String>(0).map_err(Into::into))
        .collect()
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
