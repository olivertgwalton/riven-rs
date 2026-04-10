use std::time::Instant;

use anyhow::Result;
use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::{DownloadResult, MediaItemType};
use riven_db::{entities::{MediaItem, Stream}, repo};

use crate::JobQueue;
use crate::context::DownloadHierarchyContext;
use crate::flows::download_item::persist::{
    SeasonPersistOutcome, persist_episode, persist_movie, persist_season,
};

pub enum DownloadAttemptOutcome {
    Failed,
    Succeeded,
    TerminalHandled,
}

pub async fn attempt_download(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    stream: &Stream,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
    start_time: Instant,
    hierarchy: Option<&DownloadHierarchyContext>,
) -> DownloadAttemptOutcome {
    let info_hash = &stream.info_hash;
    let stream_id = Some(stream.id);
    let resolution = stream_resolution(stream).to_owned();
    let resolution_ref: Option<&str> = Some(resolution.as_str());
    let raw_title = stream
        .parsed_data
        .as_ref()
        .and_then(|parsed| parsed.get("raw_title"))
        .and_then(|value| value.as_str())
        .unwrap_or("");

    tracing::debug!(
        id,
        info_hash,
        resolution,
        profile = profile_name,
        raw_title,
        "attempting cached stream download"
    );

    let event = RivenEvent::MediaItemDownloadRequested {
        id,
        info_hash: info_hash.clone(),
        magnet: stream.magnet.clone(),
    };

    let results = queue.registry.dispatch(&event).await;
    let mut download_result: Option<Box<DownloadResult>> = None;
    let mut saw_unavailable = false;

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::Download(download)) => {
                tracing::debug!(
                    plugin = plugin_name,
                    info_hash,
                    files = download.files.len(),
                    "download responded"
                );
                download_result = Some(download);
                break;
            }
            Ok(HookResponse::DownloadStreamUnavailable) => {
                saw_unavailable = true;
                tracing::debug!(
                    plugin = plugin_name,
                    info_hash,
                    "stream unexpectedly not cached"
                );
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(
                    plugin = plugin_name,
                    info_hash,
                    error = %error,
                    "download hook failed (transient)"
                );
                return DownloadAttemptOutcome::Failed;
            }
        }
    }

    let Some(download) = download_result else {
        if saw_unavailable {
            if let Err(error) = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await {
                tracing::error!(
                    id,
                    info_hash,
                    error = %error,
                    "failed to blacklist stale cached stream"
                );
            } else {
                tracing::debug!(id, info_hash, "blacklisted stale cached stream after provider rejection");
            }
            if let Err(error) = clear_stremthru_cache_check_keys(queue, info_hash).await {
                tracing::error!(
                    id,
                    info_hash,
                    error = %error,
                    "failed to clear stale stremthru cache-check keys"
                );
            }
        }
        tracing::debug!(id, info_hash, "no download provider accepted cached stream");
        return DownloadAttemptOutcome::Failed;
    };
    let download = *download;

    match item.item_type {
        MediaItemType::Movie => {
            if persist_movie(
                item,
                &download,
                info_hash,
                queue,
                stream_id,
                resolution_ref,
                path_tag,
                profile_name,
            )
            .await
            {
                tracing::debug!(id, info_hash, "movie download persisted");
                DownloadAttemptOutcome::Succeeded
            } else {
                tracing::debug!(id, info_hash, "movie download rejected during persist");
                DownloadAttemptOutcome::Failed
            }
        }
        MediaItemType::Episode => {
            if persist_episode(
                item,
                &download,
                info_hash,
                queue,
                hierarchy.expect("episode downloads require hierarchy context"),
                stream_id,
                resolution_ref,
                path_tag,
                profile_name,
            )
            .await
            {
                tracing::debug!(id, info_hash, "episode download persisted");
                DownloadAttemptOutcome::Succeeded
            } else {
                tracing::debug!(id, info_hash, "episode download rejected during persist");
                DownloadAttemptOutcome::Failed
            }
        }
        MediaItemType::Season => {
            match persist_season(
                item,
                download,
                info_hash,
                queue,
                hierarchy.expect("season downloads require hierarchy context"),
                start_time,
                stream_id,
                path_tag,
                profile_name,
            )
            .await
            {
                SeasonPersistOutcome::Complete | SeasonPersistOutcome::Partial => {
                    tracing::debug!(id, info_hash, "season download handled during persist");
                    DownloadAttemptOutcome::TerminalHandled
                }
                SeasonPersistOutcome::Failed => {
                    tracing::debug!(id, info_hash, "season download rejected during persist");
                    DownloadAttemptOutcome::Failed
                }
            }
        }
        _ => DownloadAttemptOutcome::Failed,
    }
}

fn stream_resolution(stream: &Stream) -> &str {
    stream
        .parsed_data
        .as_ref()
        .and_then(|parsed| parsed.get("resolution"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
}

async fn clear_stremthru_cache_check_keys(queue: &JobQueue, info_hash: &str) -> Result<()> {
    let mut conn = queue.redis.clone();
    let pattern = format!("plugin:stremthru:cache-check:*:{}", info_hash.to_lowercase());
    let mut cursor = 0u64;
    let mut keys = Vec::new();

    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(100u32)
            .query_async(&mut conn)
            .await?;
        keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }

    if !keys.is_empty() {
        let _: () = redis::cmd("DEL").arg(&keys).query_async(&mut conn).await?;
        tracing::debug!(info_hash, cleared = keys.len(), "cleared stale stremthru cache-check keys");
    }

    Ok(())
}
