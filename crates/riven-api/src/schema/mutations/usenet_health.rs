//! On-demand usenet health re-check. The dashboard offers this for titles the
//! background scanner left "unverified" (provider was unreachable) so the user
//! can confirm them immediately once connectivity is restored.

use std::sync::Arc;

use async_graphql::{Context, Object, Result};
use riven_queue::JobQueue;
use riven_usenet::UsenetStreamer;
use sqlx::PgPool;

#[derive(Default)]
pub struct UsenetHealthMutations;

#[Object]
impl UsenetHealthMutations {
    /// Re-acquire a usenet title whose release is broken (missing data) or was
    /// never ingested. The item is "completed" only because it still has a
    /// media filesystem entry, so reset alone bounces back to completed; we
    /// delete that entry to genuinely un-complete it, then re-process. The
    /// re-scrape's ingest availability probe skips any incomplete release, so a
    /// complete one is picked.
    async fn regrab_usenet_title(&self, ctx: &Context<'_>, media_item_id: i64) -> Result<String> {
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        job_queue.regrab_media_item(media_item_id).await?;
        Ok("re-grab queued".to_string())
    }

    /// Re-run the availability scan for one usenet file now and persist the
    /// result. Returns the new status (`healthy` / `unhealthy` / `unknown`).
    async fn rescan_usenet_health(
        &self,
        ctx: &Context<'_>,
        info_hash: String,
        file_index: i32,
    ) -> Result<String> {
        let pool = ctx.data::<PgPool>()?;
        let Some(streamer) = UsenetStreamer::existing_shared() else {
            return Ok("unknown".to_string());
        };

        // Preserve the row's media_item_id so the title display survives the upsert.
        let media_item_id: Option<i64> = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT media_item_id FROM filesystem_entries \
             WHERE usenet_info_hash = $1 AND usenet_file_index = $2 LIMIT 1",
        )
        .bind(&info_hash)
        .bind(file_index)
        .fetch_optional(pool)
        .await?
        .flatten();

        let idx = usize::try_from(file_index).unwrap_or(0);
        let (status, total, sampled, missing, errors) = match streamer
            .scan_availability(&info_hash, idx, riven_usenet::DEFAULT_AVAILABILITY_SAMPLE_PERCENT)
            .await
        {
            Ok(scan) => (
                scan.status(),
                scan.total_segments as i32,
                scan.sampled_segments as i32,
                scan.missing_segments as i32,
                scan.error_segments as i32,
            ),
            // No segment map — never ingested / meta gone, so not streamable.
            Err(riven_usenet::StreamerError::NotIngested(_)) => ("not_ingested", 0, 0, 0, 0),
            Err(_) => ("unknown", 0, 0, 0, 0),
        };

        riven_db::repo::upsert_usenet_file_health(
            pool,
            riven_db::repo::UsenetHealthUpdate {
                info_hash: &info_hash,
                file_index,
                media_item_id,
                status,
                total_segments: total,
                sampled_segments: sampled,
                missing_segments: missing,
                error_segments: errors,
            },
        )
        .await?;

        Ok(status.to_string())
    }
}
