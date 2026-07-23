use std::time::Instant;

use anyhow::Result;
use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::{CachedStoreEntry, DownloadResult, MediaItemType};
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;

use super::helpers::stream_resolution;
use super::persist::{
    SeasonPersistOutcome, persist_episode, persist_movie, persist_season, persist_show,
};
use crate::JobQueue;
use crate::context::DownloadHierarchyContext;

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
        raw_title,
        resolution,
        profile = profile_name,
        "download: asking the debrid services for this release"
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
        raw_title: &str,
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
                        raw_title,
                        files = download.files.len(),
                        "download: debrid service returned the torrent's file list"
                    );
                    download_result = Some(download);
                    break;
                }
                Ok(HookResponse::DownloadStreamUnavailable) => {
                    saw_unavailable = true;
                    tracing::debug!(
                        plugin = plugin_name,
                        info_hash,
                        raw_title,
                        "download: the debrid service reported this torrent as cached earlier but no longer has it"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        plugin = plugin_name,
                        info_hash,
                        raw_title,
                        error = %error,
                        "download: debrid service errored on this release; moving on to the next service or stream"
                    );
                }
            }
        }
        (download_result, saw_unavailable)
    }

    let (mut download_result, mut saw_unavailable) =
        dispatch_once(queue, &event, info_hash, raw_title).await;

    if download_result.is_none() && saw_unavailable {
        if let Err(error) = clear_stremthru_cache_check_keys(queue, info_hash, raw_title).await {
            tracing::error!(
                id,
                info_hash,
                raw_title,
                error = %error,
                "download: could not clear the stale cached-availability entry, so the retry would hit the same wrong answer; skipping the retry"
            );
        } else {
            tracing::debug!(
                id,
                info_hash,
                raw_title,
                "download: cleared the stale cached-availability entry, asking the debrid services again"
            );
            let retry_event = RivenEvent::MediaItemDownloadRequested {
                id,
                info_hash: info_hash.clone(),
                magnet: stream.magnet.clone(),
                cached_stores: Vec::new(),
            };
            let (retry_result, retry_unavailable) =
                dispatch_once(queue, &retry_event, info_hash, raw_title).await;
            download_result = retry_result;
            saw_unavailable = retry_unavailable;
        }
    }

    let Some(download) = download_result else {
        if saw_unavailable
            && let Err(error) = clear_stremthru_cache_check_keys(queue, info_hash, raw_title).await
        {
            tracing::error!(
                id,
                info_hash,
                raw_title,
                error = %error,
                "download: could not clear the stale cached-availability entry; the next attempt may repeat this failure"
            );
        }
        tracing::debug!(
            id,
            info_hash,
            raw_title,
            "download: no debrid service could provide this release"
        );
        return DownloadAttemptOutcome::Failed;
    };
    let download = *download;

    let fresh_item = match repo::get_media_item(id).await {
        Ok(Some(fresh)) => fresh,
        Ok(None) => {
            tracing::debug!(
                id,
                info_hash,
                raw_title,
                "download: item was deleted while the download was being set up; discarding it"
            );
            return DownloadAttemptOutcome::Failed;
        }
        Err(error) => {
            tracing::error!(
                id,
                info_hash,
                raw_title,
                %error,
                "download: could not re-read the item from the database before saving the files; discarding this download"
            );
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
                tracing::debug!(
                    id,
                    info_hash,
                    raw_title,
                    "download: movie files saved and linked to the item"
                );
                DownloadAttemptOutcome::Succeeded
            } else {
                tracing::debug!(
                    id,
                    info_hash,
                    raw_title,
                    "download: release rejected, its files did not match this movie (wrong title, no video file, or size outside the limits)"
                );
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
                raw_title,
                resolution_ref,
                path_tag,
                profile_name,
                skip_bitrate_check,
            )
            .await
            {
                tracing::debug!(
                    id,
                    info_hash,
                    raw_title,
                    "download: episode files saved and linked to the item"
                );
                DownloadAttemptOutcome::Succeeded
            } else {
                tracing::debug!(
                    id,
                    info_hash,
                    raw_title,
                    "download: release rejected, its files did not match this episode (wrong episode, no video file, or size outside the limits)"
                );
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
                raw_title,
                path_tag,
                profile_name,
            )
            .await
            {
                SeasonPersistOutcome::Complete | SeasonPersistOutcome::Partial => {
                    tracing::debug!(
                        id,
                        info_hash,
                        raw_title,
                        "download: season pack processed, matching episodes saved"
                    );
                    DownloadAttemptOutcome::TerminalHandled
                }
                SeasonPersistOutcome::Failed => {
                    tracing::debug!(
                        id,
                        info_hash,
                        raw_title,
                        "download: season pack rejected, none of its files matched the episodes of this season"
                    );
                    DownloadAttemptOutcome::Failed
                }
            }
        }
        MediaItemType::Show => {
            match persist_show(
                item,
                download,
                info_hash,
                queue,
                hierarchy.expect("show downloads require hierarchy context"),
                start_time,
                stream_id,
                path_tag,
                profile_name,
            )
            .await
            {
                SeasonPersistOutcome::Complete | SeasonPersistOutcome::Partial => {
                    tracing::debug!(
                        id,
                        info_hash,
                        raw_title,
                        "download: show pack processed, matching episodes saved"
                    );
                    DownloadAttemptOutcome::TerminalHandled
                }
                SeasonPersistOutcome::Failed => {
                    tracing::debug!(
                        id,
                        info_hash,
                        raw_title,
                        "download: show pack rejected, none of its files matched the episodes of this show"
                    );
                    DownloadAttemptOutcome::Failed
                }
            }
        }
    }
}

async fn clear_stremthru_cache_check_keys(
    queue: &JobQueue,
    info_hash: &str,
    raw_title: &str,
) -> Result<()> {
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
            raw_title,
            cleared = keys.len(),
            "download: dropped cached-availability entries for this release so it gets re-checked"
        );
    }

    Ok(())
}
