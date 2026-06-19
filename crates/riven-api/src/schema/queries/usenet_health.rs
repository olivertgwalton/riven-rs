//! Usenet streaming diagnostics: NNTP provider health (connections + circuit
//! breaker) and the in-process streaming engine's cache/fetch metrics.
//!
//! Both read live, in-process state — the shared `UsenetStreamer`'s connection
//! pool and the process-global `StreamerState`. Neither touches the network:
//! `existing_shared()` returns the already-built streamer (or `None` when
//! usenet isn't configured) without spinning up a pool.

use async_graphql::{Context, Object, Result, SimpleObject};
use riven_db::orm;
use riven_usenet::UsenetStreamer;
use sea_orm::{DbBackend, FromQueryResult, Statement};

/// Live health of one configured NNTP provider.
#[derive(SimpleObject)]
pub struct NntpProviderHealth {
    pub host: String,
    pub port: i32,
    /// Lower = preferred. Primaries are tried before backups.
    pub priority: i32,
    pub is_backup: bool,
    /// Connection ceiling (`max_connections`).
    pub max_connections: i32,
    /// Open sockets right now (idle + in-flight).
    pub open_connections: i32,
    /// Open sockets sitting idle in the pool.
    pub idle_connections: i32,
    /// Open sockets currently servicing a fetch.
    pub active_connections: i32,
    /// Circuit breaker is muting this provider after repeated failures.
    pub breaker_tripped: bool,
    /// Seconds until the breaker re-allows the provider (0 if healthy).
    pub cooldown_seconds_remaining: i64,
    /// Consecutive transient failures since the last success.
    pub consecutive_failures: i64,
}

/// In-process streaming engine health (segment cache + NNTP fetch counters).
#[derive(SimpleObject)]
pub struct UsenetStreamingHealth {
    pub cache_bytes_used: i64,
    pub cache_bytes_max: i64,
    pub cache_entries: i64,
    pub cache_hits: i64,
    pub cache_misses: i64,
    /// Cache hit rate over all lookups since start, 0.0–1.0.
    pub cache_hit_rate: f64,
    /// Successful wire fetches (cache misses that decoded cleanly).
    pub fetches_ok: i64,
    /// Fetches that exhausted retries or hit a missing article.
    pub fetches_failed: i64,
    /// Fetch success rate over all wire fetches, 0.0–1.0.
    pub fetch_success_rate: f64,
    /// Total decoded bytes served from the wire (poll deltas for throughput).
    pub bytes_decoded: i64,
    /// Segments being fetched + decoded right now.
    pub in_flight: i32,
    /// Segments known permanently missing on every provider.
    pub dead_segments: i64,
    /// Usenet file handles the VFS is currently serving.
    pub active_streams: i32,
}

/// Lifetime download total for one provider.
#[derive(SimpleObject)]
pub struct UsenetProviderTraffic {
    pub host: String,
    pub bytes_downloaded: i64,
    pub articles_downloaded: i64,
}

/// One provider's traffic on one day (for the usage-trend chart).
#[derive(SimpleObject)]
pub struct UsenetDailyTraffic {
    /// `YYYY-MM-DD`.
    pub day: String,
    pub host: String,
    pub bytes_downloaded: i64,
    pub articles_downloaded: i64,
}

/// Download-traffic accounting across all usenet providers.
#[derive(SimpleObject)]
pub struct UsenetTraffic {
    /// Lifetime totals per provider, busiest first.
    pub providers: Vec<UsenetProviderTraffic>,
    /// Per-provider per-day series over the last two weeks (oldest first).
    pub daily: Vec<UsenetDailyTraffic>,
    pub total_bytes_downloaded: i64,
    pub total_articles_downloaded: i64,
}

/// Health of one usenet-backed title, enriched for display.
#[derive(SimpleObject)]
pub struct UsenetTitleHealth {
    pub info_hash: String,
    pub file_index: i32,
    pub media_item_id: Option<i64>,
    /// `healthy` | `unhealthy` | `unknown` | `checking`.
    pub status: String,
    pub total_segments: i32,
    pub sampled_segments: i32,
    pub missing_segments: i32,
    pub error_segments: i32,
    /// Missing segments as a percentage of those sampled.
    pub missing_pct: f64,
    /// Unix seconds of the last check (null if never checked).
    pub checked_at: Option<i64>,
    /// Auto-repair attempts made so far (0 if none / not applicable).
    pub repair_attempts: i32,
    /// Unix seconds of the next scheduled auto-repair (null if none pending).
    pub next_repair_at: Option<i64>,
    /// Show/movie title for display.
    pub title: Option<String>,
    /// `S05E03 · Sand Job` for episodes, year for movies.
    pub subtitle: Option<String>,
    pub poster_path: Option<String>,
    pub media_type: Option<String>,
}

/// Title-health counts grouped by status, for the dashboard summary line.
#[derive(SimpleObject)]
pub struct UsenetTitleHealthSummary {
    pub healthy: i64,
    pub unhealthy: i64,
    pub not_ingested: i64,
    /// Catch-all for any other status (e.g. `checking`/`unknown`).
    pub unknown: i64,
    pub total: i64,
}

#[derive(FromQueryResult)]
struct HealthSummaryRow {
    healthy: i64,
    unhealthy: i64,
    not_ingested: i64,
    total: i64,
}

#[derive(FromQueryResult)]
struct HealthRow {
    info_hash: String,
    file_index: i32,
    media_item_id: Option<i64>,
    status: String,
    total_segments: i32,
    sampled_segments: i32,
    missing_segments: i32,
    error_segments: i32,
    checked_at: Option<i64>,
    repair_attempts: i32,
    next_repair_at: Option<i64>,
    item_type: Option<String>,
    title: Option<String>,
    full_title: Option<String>,
    poster_path: Option<String>,
    year: Option<i32>,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    show_title: Option<String>,
    show_poster: Option<String>,
}

/// Build `(title, subtitle, poster)` for a health row, matching the dashboard's
/// other media displays (show name + `S__E__ · episode` for episodes, year for
/// movies).
fn display_for(r: &HealthRow) -> (Option<String>, Option<String>, Option<String>) {
    match r.item_type.as_deref() {
        Some("episode") => {
            let title = r.show_title.clone().or_else(|| r.full_title.clone());
            let label = match (r.season_number, r.episode_number) {
                (Some(s), Some(ep)) => Some(format!("S{s:02}E{ep:02}")),
                _ => None,
            };
            let subtitle = match (label, r.title.clone()) {
                (Some(l), Some(t)) => Some(format!("{l} · {t}")),
                (Some(l), None) => Some(l),
                (None, t) => t,
            };
            (
                title,
                subtitle,
                r.show_poster.clone().or_else(|| r.poster_path.clone()),
            )
        }
        Some("movie") => (
            r.title.clone().or_else(|| r.full_title.clone()),
            r.year.map(|y| y.to_string()),
            r.poster_path.clone(),
        ),
        _ => (
            r.title.clone().or_else(|| r.full_title.clone()),
            None,
            r.poster_path.clone(),
        ),
    }
}

#[derive(Default)]
pub struct UsenetHealthQuery;

#[Object]
impl UsenetHealthQuery {
    /// Per-provider NNTP health (connections + circuit-breaker state). Empty
    /// when usenet isn't configured.
    async fn nntp_providers(&self, _ctx: &Context<'_>) -> Result<Vec<NntpProviderHealth>> {
        let providers = match UsenetStreamer::existing_shared() {
            Some(streamer) => streamer.pool().health(),
            None => Vec::new(),
        };
        Ok(providers
            .into_iter()
            .map(|p| NntpProviderHealth {
                host: p.host,
                port: p.port as i32,
                priority: p.priority,
                is_backup: p.is_backup,
                max_connections: p.max_connections as i32,
                open_connections: p.open_connections as i32,
                idle_connections: p.idle_connections as i32,
                active_connections: p.active_connections as i32,
                breaker_tripped: p.breaker_tripped,
                cooldown_seconds_remaining: p.cooldown_seconds_remaining as i64,
                consecutive_failures: p.consecutive_failures as i64,
            })
            .collect())
    }

    /// Per-title usenet health from the background availability scanner.
    /// Ordered worst-first (unhealthy, then most missing segments).
    async fn usenet_title_health(&self, _ctx: &Context<'_>) -> Result<Vec<UsenetTitleHealth>> {
        let rows = HealthRow::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"
            SELECT h.info_hash,
                   h.file_index,
                   h.media_item_id,
                   h.status,
                   h.total_segments,
                   h.sampled_segments,
                   h.missing_segments,
                   h.error_segments,
                   extract(epoch FROM h.checked_at)::bigint AS checked_at,
                   h.repair_attempts,
                   extract(epoch FROM h.next_repair_at)::bigint AS next_repair_at,
                   mi.item_type::text AS item_type,
                   mi.title,
                   mi.full_title,
                   mi.poster_path,
                   mi.year,
                   mi.season_number,
                   mi.episode_number,
                   sh.title       AS show_title,
                   sh.poster_path AS show_poster
            FROM usenet_file_health h
            LEFT JOIN media_items mi ON mi.id = h.media_item_id
            LEFT JOIN media_items se ON mi.item_type = 'episode' AND se.id = mi.parent_id
            LEFT JOIN media_items sh ON sh.id = se.parent_id
            -- Only titles that still have this usenet file in the library; rows
            -- orphaned by a re-grab onto a different release are excluded.
            WHERE EXISTS (
                SELECT 1 FROM filesystem_entries fe
                WHERE fe.usenet_info_hash = h.info_hash
                  AND fe.usenet_file_index = h.file_index
            )
            ORDER BY (h.status = 'unhealthy') DESC, h.missing_segments DESC,
                     h.checked_at DESC NULLS LAST
            "#,
            [],
        ))
        .all(orm())
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let missing_pct = if r.sampled_segments > 0 {
                    (r.missing_segments as f64 / r.sampled_segments as f64) * 100.0
                } else {
                    0.0
                };
                let (title, subtitle, poster_path) = display_for(&r);
                UsenetTitleHealth {
                    info_hash: r.info_hash,
                    file_index: r.file_index,
                    media_item_id: r.media_item_id,
                    status: r.status,
                    total_segments: r.total_segments,
                    sampled_segments: r.sampled_segments,
                    missing_segments: r.missing_segments,
                    error_segments: r.error_segments,
                    missing_pct,
                    checked_at: r.checked_at,
                    repair_attempts: r.repair_attempts,
                    next_repair_at: r.next_repair_at,
                    title,
                    subtitle,
                    poster_path,
                    media_type: r.item_type,
                }
            })
            .collect())
    }

    /// Title-health counts grouped by status. The `WHERE EXISTS` filter must
    /// mirror `usenet_title_health` so the summary matches the listed rows.
    async fn usenet_title_health_summary(
        &self,
        _ctx: &Context<'_>,
    ) -> Result<UsenetTitleHealthSummary> {
        let row = HealthSummaryRow::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"
            SELECT
                COUNT(*) FILTER (WHERE h.status = 'healthy')::bigint      AS healthy,
                COUNT(*) FILTER (WHERE h.status = 'unhealthy')::bigint    AS unhealthy,
                COUNT(*) FILTER (WHERE h.status = 'not_ingested')::bigint AS not_ingested,
                COUNT(*)::bigint                                          AS total
            FROM usenet_file_health h
            WHERE EXISTS (
                SELECT 1 FROM filesystem_entries fe
                WHERE fe.usenet_info_hash = h.info_hash
                  AND fe.usenet_file_index = h.file_index
            )
            "#,
            [],
        ))
        .one(orm())
        .await?
        .ok_or_else(|| async_graphql::Error::new("health summary query returned no rows"))?;

        Ok(UsenetTitleHealthSummary {
            healthy: row.healthy,
            unhealthy: row.unhealthy,
            not_ingested: row.not_ingested,
            unknown: row.total - row.healthy - row.unhealthy - row.not_ingested,
            total: row.total,
        })
    }

    /// Per-provider download traffic — lifetime totals + a daily series for
    /// the usage-trend chart.
    async fn usenet_traffic(&self, _ctx: &Context<'_>) -> Result<UsenetTraffic> {
        let totals = riven_db::repo::list_provider_traffic_totals().await?;
        let daily = riven_db::repo::list_provider_traffic_daily(14).await?;
        let total_bytes_downloaded = totals.iter().map(|t| t.bytes_downloaded).sum();
        let total_articles_downloaded = totals.iter().map(|t| t.articles_downloaded).sum();
        Ok(UsenetTraffic {
            providers: totals
                .into_iter()
                .map(|t| UsenetProviderTraffic {
                    host: t.host,
                    bytes_downloaded: t.bytes_downloaded,
                    articles_downloaded: t.articles_downloaded,
                })
                .collect(),
            daily: daily
                .into_iter()
                .map(|d| UsenetDailyTraffic {
                    day: d.day,
                    host: d.host,
                    bytes_downloaded: d.bytes_downloaded,
                    articles_downloaded: d.articles_downloaded,
                })
                .collect(),
            total_bytes_downloaded,
            total_articles_downloaded,
        })
    }

    /// Cache + fetch metrics for the in-process usenet streaming engine.
    async fn usenet_streaming_health(&self, _ctx: &Context<'_>) -> Result<UsenetStreamingHealth> {
        let h = riven_usenet::streamer::streaming_health();
        let lookups = h.cache_hits + h.cache_misses;
        let cache_hit_rate = if lookups > 0 {
            h.cache_hits as f64 / lookups as f64
        } else {
            0.0
        };
        let fetches = h.fetches_ok + h.fetches_failed;
        let fetch_success_rate = if fetches > 0 {
            h.fetches_ok as f64 / fetches as f64
        } else {
            0.0
        };
        Ok(UsenetStreamingHealth {
            cache_bytes_used: h.cache_bytes_used as i64,
            cache_bytes_max: h.cache_bytes_max as i64,
            cache_entries: h.cache_entries as i64,
            cache_hits: h.cache_hits as i64,
            cache_misses: h.cache_misses as i64,
            cache_hit_rate,
            fetches_ok: h.fetches_ok as i64,
            fetches_failed: h.fetches_failed as i64,
            fetch_success_rate,
            bytes_decoded: h.bytes_decoded as i64,
            in_flight: h.in_flight as i32,
            dead_segments: h.dead_segments as i64,
            active_streams: h.active_streams as i32,
        })
    }
}
