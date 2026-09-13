//! One usenet availability scan, persisted — shared by the `rescanUsenetHealth`
//! mutation and the background health scanner in `riven-app`.

use riven_usenet::{StreamerError, UsenetStreamer};

/// Scan one file and upsert its health row. Returns the status (`healthy` /
/// `unhealthy` / `not_ingested` / `unknown`) alongside the upsert's outcome, so
/// a caller can act on the status even when persisting it failed.
pub async fn rescan_file(
    streamer: &UsenetStreamer,
    info_hash: &str,
    file_index: i32,
    media_item_id: Option<i64>,
    sample_percent: usize,
) -> (&'static str, anyhow::Result<()>) {
    let idx = usize::try_from(file_index).unwrap_or(0);
    let (status, total, sampled, missing, errors) = match streamer
        .scan_availability(info_hash, idx, sample_percent)
        .await
    {
        Ok(scan) => (
            scan.status(),
            scan.total_segments as i32,
            scan.sampled_segments as i32,
            scan.missing_segments as i32,
            scan.error_segments as i32,
        ),
        Err(StreamerError::NotIngested(_)) => ("not_ingested", 0, 0, 0, 0),
        Err(error) => {
            tracing::debug!(info_hash, file_index, %error, "usenet health: scan failed");
            ("unknown", 0, 0, 0, 0)
        }
    };

    let saved = riven_db::repo::upsert_usenet_file_health(riven_db::repo::UsenetHealthUpdate {
        info_hash,
        file_index,
        media_item_id,
        status,
        total_segments: total,
        sampled_segments: sampled,
        missing_segments: missing,
        error_segments: errors,
    })
    .await;
    (status, saved)
}
