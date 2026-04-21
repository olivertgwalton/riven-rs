use std::time::Instant;

use anyhow::Result;
use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::{CachedStoreEntry, DownloadResult, MediaItemType};
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;

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
    _item: &MediaItem,
    queue: &JobQueue,
    stream: &Stream,
    stores: Vec<CachedStoreEntry>,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
    start_time: Instant,
    hierarchy: Option<&DownloadHierarchyContext>,
    skip_bitrate_check: bool,
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
        "attempting stream download"
    );

    let event = RivenEvent::MediaItemDownloadRequested {
        id,
        info_hash: info_hash.clone(),
        magnet: stream.magnet.clone(),
        cached_stores: stores.clone(),
    };

    async fn dispatch_once(
        queue: &JobQueue,
        event: &RivenEvent,
        info_hash: &str,
    ) -> (Option<Box<DownloadResult>>, bool) {
        let results = queue.registry.dispatch(event).await;
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
                }
            }
        }
        (download_result, saw_unavailable)
    }

    let (mut download_result, mut saw_unavailable) = dispatch_once(queue, &event, info_hash).await;

    // Retry once after clearing stale cache-check keys. A parallel item can get a stale
    // "cached" hit, call add_torrent, find it's not actually available, and return
    // `DownloadStreamUnavailable`. Clearing + re-dispatching forces a fresh cache check,
    // which usually reveals the torrent IS cached and add_torrent succeeds.
    if download_result.is_none() && saw_unavailable {
        if let Err(error) = clear_stremthru_cache_check_keys(queue, info_hash).await {
            tracing::error!(
                id,
                info_hash,
                error = %error,
                "failed to clear stale stremthru cache-check keys before retry"
            );
        } else {
            tracing::debug!(
                id,
                info_hash,
                "retrying download after clearing stale cache-check"
            );
            // Empty `cached_stores` forces the plugin to run an on-demand cache check rather
            // than reusing the stale pre-checked entries from the original dispatch (which
            // are what sent us down the `add_torrent → unavailable` path in the first place).
            let retry_event = RivenEvent::MediaItemDownloadRequested {
                id,
                info_hash: info_hash.clone(),
                magnet: stream.magnet.clone(),
                cached_stores: Vec::new(),
            };
            let (retry_result, retry_unavailable) =
                dispatch_once(queue, &retry_event, info_hash).await;
            download_result = retry_result;
            saw_unavailable = retry_unavailable;
        }
    }

    let Some(download) = download_result else {
        if saw_unavailable {
            // Still unavailable after retry — clear keys one more time and give up on this
            // candidate. Do not blacklist; stream may become available later.
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

    let fresh_item = match repo::get_media_item(&queue.db_pool, id).await {
        Ok(Some(fresh)) => fresh,
        Ok(None) => {
            tracing::debug!(
                id,
                info_hash,
                "media item disappeared before persist; skipping"
            );
            return DownloadAttemptOutcome::Failed;
        }
        Err(error) => {
            tracing::error!(id, info_hash, %error, "failed to reload item before persist");
            return DownloadAttemptOutcome::Failed;
        }
    };
    let item = &fresh_item;

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
                skip_bitrate_check,
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
                skip_bitrate_check,
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
    let pattern = format!(
        "plugin:stremthru:cache-check:*:{}",
        info_hash.to_lowercase()
    );
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
        tracing::debug!(
            info_hash,
            cleared = keys.len(),
            "cleared stale stremthru cache-check keys"
        );
    }

    Ok(())
}
