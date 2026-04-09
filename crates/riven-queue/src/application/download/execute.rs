use std::time::Instant;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::{DownloadResult, MediaItemType};
use riven_db::entities::{MediaItem, Stream};

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

    let event = RivenEvent::MediaItemDownloadRequested {
        id,
        info_hash: info_hash.clone(),
        magnet: stream.magnet.clone(),
    };

    let results = queue.registry.dispatch(&event).await;
    let mut download_result: Option<Box<DownloadResult>> = None;

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::Download(download)) => {
                tracing::debug!(
                    plugin = plugin_name,
                    files = download.files.len(),
                    "download responded"
                );
                download_result = Some(download);
                break;
            }
            Ok(HookResponse::DownloadStreamUnavailable) => {
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
                    error = %error,
                    "download hook failed (transient)"
                );
                return DownloadAttemptOutcome::Failed;
            }
        }
    }

    let Some(download) = download_result else {
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
                DownloadAttemptOutcome::Succeeded
            } else {
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
                DownloadAttemptOutcome::Succeeded
            } else {
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
                    DownloadAttemptOutcome::TerminalHandled
                }
                SeasonPersistOutcome::Failed => DownloadAttemptOutcome::Failed,
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
