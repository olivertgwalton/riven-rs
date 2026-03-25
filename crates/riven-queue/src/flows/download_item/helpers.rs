
use riven_core::events::RivenEvent;
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::JobQueue;

/// Matches multi-episode patterns in filenames, capturing the start and end episode numbers.

/// Log a bitrate failure, blacklist the stream, and send a PartialSuccess event.
pub async fn handle_bitrate_failure(
    id: i64,
    info_hash: &str,
    file_size: u64,
    runtime: Option<i32>,
    context: &str,
    queue: &JobQueue,
) {
    tracing::warn!(
        id,
        file_size,
        runtime = ?runtime,
        info_hash = %info_hash,
        "{context} failed bitrate check — blacklisting stream"
    );
    if !info_hash.is_empty() {
        let _ = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await;
        let _ = repo::update_stream_file_size(&queue.db_pool, info_hash, file_size).await;
    }
    queue
        .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
        .await;
    queue.fan_out_download(id).await;
}

/// Load a media item by id, or send a `MediaItemDownloadError` event and return `None`.
pub async fn load_item_or_err(
    id: i64,
    queue: &JobQueue,
    error_msg: &str,
) -> Option<MediaItem> {
    match repo::get_media_item(&queue.db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => {
            tracing::error!(id, "{error_msg}");
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: String::new(),
                    error: error_msg.into(),
                })
                .await;
            None
        }
        Err(e) => {
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: String::new(),
                    error: e.to_string(),
                })
                .await;
            None
        }
    }
}

/// Parse a file path by merging metadata from all segments.
/// Mirrors riven-ts `parseFilePath`.
pub fn parse_file_path(path: &str) -> riven_rank::ParsedData {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let mut merged = riven_rank::ParsedData::default();

    for segment in segments {
        let parsed = riven_rank::parse(segment);
        merged.merge(parsed);
    }

    merged
}
