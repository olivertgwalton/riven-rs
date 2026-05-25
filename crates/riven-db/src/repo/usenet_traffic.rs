//! Usenet download-traffic accounting: lifetime per-provider totals and
//! per-day buckets for usage trends. Written by the flusher, read by the API.

use anyhow::Result;
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ProviderTrafficTotal {
    pub host: String,
    pub bytes_downloaded: i64,
    pub articles_downloaded: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
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
    pool: &PgPool,
    host: &str,
    bytes_delta: i64,
    articles_delta: i64,
) -> Result<()> {
    if bytes_delta <= 0 && articles_delta <= 0 {
        return Ok(());
    }
    sqlx::query(
        r#"
        INSERT INTO usenet_provider_traffic (host, bytes_downloaded, articles_downloaded, updated_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (host) DO UPDATE SET
            bytes_downloaded    = usenet_provider_traffic.bytes_downloaded + EXCLUDED.bytes_downloaded,
            articles_downloaded = usenet_provider_traffic.articles_downloaded + EXCLUDED.articles_downloaded,
            updated_at          = now()
        "#,
    )
    .bind(host)
    .bind(bytes_delta)
    .bind(articles_delta)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO usenet_traffic_daily (day, host, bytes_downloaded, articles_downloaded)
        VALUES (current_date, $1, $2, $3)
        ON CONFLICT (day, host) DO UPDATE SET
            bytes_downloaded    = usenet_traffic_daily.bytes_downloaded + EXCLUDED.bytes_downloaded,
            articles_downloaded = usenet_traffic_daily.articles_downloaded + EXCLUDED.articles_downloaded
        "#,
    )
    .bind(host)
    .bind(bytes_delta)
    .bind(articles_delta)
    .execute(pool)
    .await?;
    Ok(())
}

/// Lifetime per-provider totals, busiest first.
pub async fn list_provider_traffic_totals(pool: &PgPool) -> Result<Vec<ProviderTrafficTotal>> {
    Ok(sqlx::query_as::<_, ProviderTrafficTotal>(
        r#"
        SELECT host, bytes_downloaded, articles_downloaded
        FROM usenet_provider_traffic
        ORDER BY bytes_downloaded DESC
        "#,
    )
    .fetch_all(pool)
    .await?)
}

/// Per-provider per-day traffic over the last `days` days (oldest first).
pub async fn list_provider_traffic_daily(pool: &PgPool, days: i32) -> Result<Vec<DailyTraffic>> {
    Ok(sqlx::query_as::<_, DailyTraffic>(
        r#"
        SELECT to_char(day, 'YYYY-MM-DD') AS day, host, bytes_downloaded, articles_downloaded
        FROM usenet_traffic_daily
        WHERE day >= current_date - ($1::int - 1)
        ORDER BY day ASC, host ASC
        "#,
    )
    .bind(days)
    .fetch_all(pool)
    .await?)
}
