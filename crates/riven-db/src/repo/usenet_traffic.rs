//! Usenet download-traffic accounting: lifetime per-provider totals and
//! per-day buckets for usage trends. Written by the flusher, read by the API.

use anyhow::Result;
use riven_core::entities::{usenet_provider_traffic, usenet_traffic_daily};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::ActiveValue::Set;
use sea_orm::{DbBackend, EntityTrait, FromQueryResult, QueryOrder, Statement};

use crate::orm;

#[derive(Debug, Clone, FromQueryResult)]
pub struct ProviderTrafficTotal {
    pub host: String,
    pub bytes_downloaded: i64,
    pub articles_downloaded: i64,
}

#[derive(Debug, Clone, FromQueryResult)]
pub struct DailyTraffic {
    /// `YYYY-MM-DD`.
    pub day: String,
    pub host: String,
    pub bytes_downloaded: i64,
    pub articles_downloaded: i64,
}

/// Add a provider's traffic delta to both the lifetime total and today's
/// bucket. No-op for a non-positive delta — callers guard, but it's cheap
/// insurance.
pub async fn add_provider_traffic(
    host: &str,
    bytes_delta: i64,
    articles_delta: i64,
) -> Result<()> {
    if bytes_delta <= 0 && articles_delta <= 0 {
        return Ok(());
    }

    // Lifetime total: increment existing counters on conflict.
    usenet_provider_traffic::Entity::insert(usenet_provider_traffic::ActiveModel {
        host: Set(host.to_owned()),
        bytes_downloaded: Set(bytes_delta),
        articles_downloaded: Set(articles_delta),
        updated_at: Set(chrono::Utc::now().fixed_offset()),
    })
    .on_conflict(
        OnConflict::column(usenet_provider_traffic::Column::Host)
            .value(
                usenet_provider_traffic::Column::BytesDownloaded,
                Expr::col((
                    usenet_provider_traffic::Entity,
                    usenet_provider_traffic::Column::BytesDownloaded,
                ))
                .add(bytes_delta),
            )
            .value(
                usenet_provider_traffic::Column::ArticlesDownloaded,
                Expr::col((
                    usenet_provider_traffic::Entity,
                    usenet_provider_traffic::Column::ArticlesDownloaded,
                ))
                .add(articles_delta),
            )
            .value(
                usenet_provider_traffic::Column::UpdatedAt,
                Expr::cust("now()"),
            )
            .to_owned(),
    )
    .exec(orm())
    .await?;

    // Today's per-day bucket: same accumulate-on-conflict pattern.
    usenet_traffic_daily::Entity::insert(usenet_traffic_daily::ActiveModel {
        day: Set(chrono::Utc::now().date_naive()),
        host: Set(host.to_owned()),
        bytes_downloaded: Set(bytes_delta),
        articles_downloaded: Set(articles_delta),
    })
    .on_conflict(
        OnConflict::columns([
            usenet_traffic_daily::Column::Day,
            usenet_traffic_daily::Column::Host,
        ])
        .value(
            usenet_traffic_daily::Column::BytesDownloaded,
            Expr::col((
                usenet_traffic_daily::Entity,
                usenet_traffic_daily::Column::BytesDownloaded,
            ))
            .add(bytes_delta),
        )
        .value(
            usenet_traffic_daily::Column::ArticlesDownloaded,
            Expr::col((
                usenet_traffic_daily::Entity,
                usenet_traffic_daily::Column::ArticlesDownloaded,
            ))
            .add(articles_delta),
        )
        .to_owned(),
    )
    .exec(orm())
    .await?;
    Ok(())
}

/// Lifetime per-provider totals, busiest first.
pub async fn list_provider_traffic_totals() -> Result<Vec<ProviderTrafficTotal>> {
    Ok(usenet_provider_traffic::Entity::find()
        .order_by_desc(usenet_provider_traffic::Column::BytesDownloaded)
        .into_model::<ProviderTrafficTotal>()
        .all(orm())
        .await?)
}

/// Per-provider per-day traffic over the last `days` days (oldest first).
pub async fn list_provider_traffic_daily(days: i32) -> Result<Vec<DailyTraffic>> {
    // `day` is a PG date; the original projected it with to_char(...) as a
    // YYYY-MM-DD string, which `DailyTraffic.day: String` expects. Keep the raw
    // statement so the formatting and the `current_date - (n-1)` window match.
    Ok(DailyTraffic::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        SELECT to_char(day, 'YYYY-MM-DD') AS day, host, bytes_downloaded, articles_downloaded
        FROM usenet_traffic_daily
        WHERE day >= current_date - ($1::int - 1)
        ORDER BY day ASC, host ASC
        "#,
        [days.into()],
    ))
    .all(orm())
    .await?)
}
