//! Per-title usenet health storage. Populated by the background availability
//! scanner; read by the API's usenet-health view.

use anyhow::Result;
use riven_core::entities::usenet_file_health;
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter, QuerySelect, Statement,
};

use crate::orm;

/// A usenet-backed media file the scanner may check, with its last check time
/// (via the join) implicitly driving ordering.
#[derive(Debug, Clone, FromQueryResult)]
pub struct UsenetFileToCheck {
    pub info_hash: String,
    pub file_index: i32,
    pub media_item_id: Option<i64>,
    /// Library path of the entry, carried purely so the scanner's logs can
    /// name the title they are talking about instead of only its info_hash.
    pub path: String,
}

/// Distinct usenet files ordered least-recently-checked first (never-checked
/// lead). The scanner pulls a small batch each tick.
pub async fn usenet_files_due_for_check(limit: i64) -> Result<Vec<UsenetFileToCheck>> {
    // DISTINCT ON + a LEFT JOIN into a derived table with NULLS-FIRST ordering
    // has no clean builder form; keep the raw statement. No enum columns here.
    Ok(
        UsenetFileToCheck::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"
        SELECT u.info_hash, u.file_index, u.media_item_id, u.path
        FROM (
            SELECT DISTINCT ON (usenet_info_hash, usenet_file_index)
                   usenet_info_hash AS info_hash,
                   usenet_file_index AS file_index,
                   media_item_id,
                   path
            FROM filesystem_entries
            WHERE usenet_info_hash IS NOT NULL
              AND usenet_file_index IS NOT NULL
            ORDER BY usenet_info_hash, usenet_file_index, media_item_id
        ) u
        LEFT JOIN usenet_file_health h
               ON h.info_hash = u.info_hash AND h.file_index = u.file_index
        ORDER BY h.checked_at ASC NULLS FIRST
        LIMIT $1
        "#,
            [limit.into()],
        ))
        .all(orm())
        .await?,
    )
}

/// One health update for [`upsert_usenet_file_health`].
pub struct UsenetHealthUpdate<'a> {
    pub info_hash: &'a str,
    pub file_index: i32,
    pub media_item_id: Option<i64>,
    pub status: &'a str,
    pub total_segments: i32,
    pub sampled_segments: i32,
    pub missing_segments: i32,
    pub error_segments: i32,
}

/// Insert or update the health record for one usenet file. Stamps `checked_at`.
pub async fn upsert_usenet_file_health(u: UsenetHealthUpdate<'_>) -> Result<()> {
    let now = chrono::Utc::now().fixed_offset();
    usenet_file_health::Entity::insert(usenet_file_health::ActiveModel {
        info_hash: Set(u.info_hash.to_owned()),
        file_index: Set(u.file_index),
        media_item_id: Set(u.media_item_id),
        status: Set(u.status.to_owned()),
        total_segments: Set(u.total_segments),
        sampled_segments: Set(u.sampled_segments),
        missing_segments: Set(u.missing_segments),
        error_segments: Set(u.error_segments),
        checked_at: Set(Some(now)),
        updated_at: Set(now),
        ..Default::default()
    })
    .on_conflict(
        OnConflict::columns([
            usenet_file_health::Column::InfoHash,
            usenet_file_health::Column::FileIndex,
        ])
        .update_columns([
            usenet_file_health::Column::MediaItemId,
            usenet_file_health::Column::Status,
            usenet_file_health::Column::TotalSegments,
            usenet_file_health::Column::SampledSegments,
            usenet_file_health::Column::MissingSegments,
            usenet_file_health::Column::ErrorSegments,
            usenet_file_health::Column::CheckedAt,
            usenet_file_health::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec(orm())
    .await?;
    Ok(())
}

/// Delete health rows whose usenet file no longer exists in the library — e.g.
/// the title was re-grabbed onto a different (or non-usenet) release. Keeps the
/// health view in sync so stale "not ingested"/"missing data" rows don't linger.
pub async fn prune_orphaned_usenet_health() -> Result<u64> {
    // Single-table DELETE; only the correlated NOT EXISTS against
    // filesystem_entries stays raw inside the filter.
    let result = usenet_file_health::Entity::delete_many()
        .filter(Expr::cust(
            "NOT EXISTS ( \
                 SELECT 1 FROM filesystem_entries fe \
                 WHERE fe.usenet_info_hash = usenet_file_health.info_hash \
                   AND fe.usenet_file_index = usenet_file_health.file_index \
             )",
        ))
        .exec(orm())
        .await?;
    Ok(result.rows_affected)
}

/// Auto-repair: if a file is eligible for another automatic re-grab (under the
/// retry cap and past its backoff window), returns its current attempt count.
/// `None` means not due / exhausted.
pub async fn usenet_repair_due(
    info_hash: &str,
    file_index: i32,
    max_retries: i32,
) -> Result<Option<i32>> {
    Ok(usenet_file_health::Entity::find()
        .filter(usenet_file_health::Column::InfoHash.eq(info_hash))
        .filter(usenet_file_health::Column::FileIndex.eq(file_index))
        .filter(usenet_file_health::Column::RepairAttempts.lt(max_retries))
        .filter(
            usenet_file_health::Column::NextRepairAt
                .is_null()
                .or(Expr::col(usenet_file_health::Column::NextRepairAt).lte(Expr::cust("now()"))),
        )
        .select_only()
        .column(usenet_file_health::Column::RepairAttempts)
        .into_tuple()
        .one(orm())
        .await?)
}

/// Record an auto-repair attempt: bump the counter and schedule the next
/// attempt `backoff_secs` from now.
pub async fn record_usenet_repair_attempt(
    info_hash: &str,
    file_index: i32,
    backoff_secs: i64,
) -> Result<()> {
    usenet_file_health::Entity::update_many()
        .col_expr(
            usenet_file_health::Column::RepairAttempts,
            Expr::col(usenet_file_health::Column::RepairAttempts).add(1),
        )
        .col_expr(
            usenet_file_health::Column::LastRepairAt,
            Expr::cust("now()"),
        )
        .col_expr(
            usenet_file_health::Column::NextRepairAt,
            Expr::cust_with_values("now() + ($1 * interval '1 second')", [backoff_secs]),
        )
        .filter(usenet_file_health::Column::InfoHash.eq(info_hash))
        .filter(usenet_file_health::Column::FileIndex.eq(file_index))
        .exec(orm())
        .await?;
    Ok(())
}

/// Clear repair bookkeeping once a file is healthy again (altmount's
/// "resolve on import"). No-op when there's nothing to clear.
pub async fn clear_usenet_repair_state(info_hash: &str, file_index: i32) -> Result<()> {
    usenet_file_health::Entity::update_many()
        .col_expr(usenet_file_health::Column::RepairAttempts, Expr::value(0))
        .col_expr(
            usenet_file_health::Column::LastRepairAt,
            Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
        )
        .col_expr(
            usenet_file_health::Column::NextRepairAt,
            Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
        )
        .filter(usenet_file_health::Column::InfoHash.eq(info_hash))
        .filter(usenet_file_health::Column::FileIndex.eq(file_index))
        .filter(
            usenet_file_health::Column::RepairAttempts
                .gt(0)
                .or(usenet_file_health::Column::NextRepairAt.is_not_null()),
        )
        .exec(orm())
        .await?;
    Ok(())
}
