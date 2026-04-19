mod candidates;
mod execute;

use std::collections::HashSet;
use std::time::Instant;

use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;
use riven_rank::RankSettings;
use serde::Deserialize;

use crate::context::{DownloadHierarchyContext, load_download_hierarchy_context};
use crate::flows::download_item::helpers::{load_item_or_err, load_bitrate_limits};
use crate::flows::download_item::persist::{finalize_download_success, persist_supplied_download};
use crate::flows::load_active_profiles;
use crate::{DownloadJob, JobQueue};

use self::candidates::pick_best_for_profile;
use self::execute::{DownloadAttemptOutcome, attempt_download};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualDownloadFileInput {
    pub name: String,
    pub size: u64,
    #[serde(default)]
    pub link: Option<String>,
    #[serde(default)]
    pub matched_media_item_id: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualDownloadTorrentInput {
    pub info_hash: String,
    pub files: Vec<ManualDownloadFileInput>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub torrent_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualDownloadErrorKind {
    IncorrectState,
    DownloadError,
}

#[derive(Debug, Clone)]
pub struct ManualDownloadError {
    pub kind: ManualDownloadErrorKind,
    pub item: Option<MediaItem>,
    pub message: String,
}

impl ManualDownloadError {
    fn incorrect_state(item: MediaItem) -> Self {
        Self {
            kind: ManualDownloadErrorKind::IncorrectState,
            item: Some(item),
            message: "media item is not in a downloadable state".to_string(),
        }
    }

    fn download_error(item: Option<MediaItem>, message: impl Into<String>) -> Self {
        Self {
            kind: ManualDownloadErrorKind::DownloadError,
            item,
            message: message.into(),
        }
    }
}

pub async fn persist_manual_download(
    id: i64,
    torrent: ManualDownloadTorrentInput,
    processed_by: &str,
    queue: &JobQueue,
) -> Result<MediaItem, ManualDownloadError> {
    let Some(item) = load_item_or_err(id, queue, "media item not found for download").await else {
        return Err(ManualDownloadError::download_error(
            None,
            "media item not found for download",
        ));
    };

    if !matches!(
        item.state,
        MediaItemState::Scraped | MediaItemState::Ongoing | MediaItemState::PartiallyCompleted
    ) {
        queue
            .notify(RivenEvent::MediaItemDownloadErrorIncorrectState { id })
            .await;
        return Err(ManualDownloadError::incorrect_state(item));
    }

    let streams = repo::get_streams_for_item(&queue.db_pool, id)
        .await
        .map_err(|error| {
            ManualDownloadError::download_error(Some(item.clone()), error.to_string())
        })?;
    let Some(stream) = streams
        .into_iter()
        .find(|stream| stream.info_hash.eq_ignore_ascii_case(&torrent.info_hash))
    else {
        return Err(ManualDownloadError::download_error(
            Some(item),
            format!("no linked stream found for info hash {}", torrent.info_hash),
        ));
    };

    if let Err(error) = repo::set_active_stream(&queue.db_pool, id, stream.id).await {
        return Err(ManualDownloadError::download_error(
            Some(item),
            error.to_string(),
        ));
    }

    let start_time = Instant::now();
    let download = DownloadResult {
        info_hash: torrent.info_hash.clone(),
        files: torrent
            .files
            .iter()
            .map(|file| DownloadFile {
                filename: file.name.clone(),
                file_size: file.size,
                download_url: file.link.clone(),
                stream_url: file
                    .matched_media_item_id
                    .map(|episode_id| format!("matched:{episode_id}")),
            })
            .collect(),
        provider: torrent.provider.clone(),
        plugin_name: processed_by.to_string(),
    };

    match persist_supplied_download(&item, &stream, download, queue, start_time).await {
        Ok(()) => repo::get_media_item(&queue.db_pool, id)
            .await
            .map_err(|error| {
                ManualDownloadError::download_error(Some(item.clone()), error.to_string())
            })?
            .ok_or_else(|| {
                ManualDownloadError::download_error(
                    Some(item),
                    "media item disappeared after download persist",
                )
            }),
        Err(error) => Err(ManualDownloadError::download_error(
            Some(item),
            error.to_string(),
        )),
    }
}

pub async fn run(id: i64, job: &DownloadJob, queue: &JobQueue) {
    let start_time = Instant::now();
    tracing::debug!(id, "running download flow");

    let item = match load_item_or_err(id, queue, "media item not found for download").await {
        Some(item) => item,
        None => return,
    };

    if !matches!(
        item.state,
        MediaItemState::Scraped | MediaItemState::Ongoing | MediaItemState::PartiallyCompleted
    ) {
        tracing::debug!(id, state = ?item.state, "download requested for non-processable item");
        queue
            .notify(RivenEvent::MediaItemDownloadErrorIncorrectState { id })
            .await;
        return;
    }

    let hierarchy = match item.item_type {
        MediaItemType::Episode | MediaItemType::Season => {
            Some(load_download_hierarchy_context(&queue.db_pool, &item).await)
        }
        _ => None,
    };

    let active_profiles: Vec<(String, RankSettings)> = load_active_profiles(&queue.db_pool).await;
    let profile_mode = !active_profiles.is_empty();

    let ranks = queue.resolution_ranks.read().await.clone();
    let all_streams = match repo::get_non_blacklisted_streams(&queue.db_pool, id, &ranks).await {
        Ok(streams) => streams,
        Err(error) => {
            tracing::error!(id, error = %error, "failed to fetch streams for download");
            return;
        }
    };

    if all_streams.is_empty() {
        tracing::debug!(id, "no streams available for download");
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "no streams available for download".into(),
            })
            .await;
        return;
    }

    // Pre-filter streams whose size is already known and fails the configured
    // bitrate limits. This avoids touching the debrid service for streams we
    // will reject anyway. Streams with no stored size pass through; they are
    // checked after add-torrent returns the file list, and their size is stored
    // at that point so they are caught here on all future attempts.
    let (max_size_bytes, min_size_bytes) = load_bitrate_limits(queue, &item).await;
    let streams_to_try: Vec<&Stream> = all_streams
        .iter()
        .filter(|s| {
            let Some(size) = s.file_size_bytes else { return true };
            let size = size as u64;
            if max_size_bytes.is_some_and(|max| size > max) {
                tracing::debug!(id, info_hash = %s.info_hash, size, "skipping stream: known size exceeds max bitrate limit");
                return false;
            }
            if min_size_bytes.is_some_and(|min| size < min) {
                tracing::debug!(id, info_hash = %s.info_hash, size, "skipping stream: known size below min bitrate limit");
                return false;
            }
            true
        })
        .collect();

    // Streams are tried sequentially via add-torrent requests. The debrid
    // provider's response determines availability — no batch cache pre-check.
    if let Some(preferred_info_hash) = job.preferred_info_hash.as_ref() {
        let _ = run_preferred_stream(
            id,
            &item,
            queue,
            start_time,
            preferred_info_hash,
            &all_streams,
            hierarchy.as_ref(),
        )
        .await;
    } else if profile_mode {
        let _ = run_multi_version(
            id,
            &item,
            queue,
            start_time,
            &active_profiles,
            &streams_to_try,
            hierarchy.as_ref(),
        )
        .await;
    } else {
        let _ = run_single_version(
            id,
            &item,
            queue,
            start_time,
            &streams_to_try,
            hierarchy.as_ref(),
        )
        .await;
    }
}

async fn run_preferred_stream(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    preferred_info_hash: &str,
    streams: &[Stream],
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    let Some(stream) = streams
        .iter()
        .find(|s| s.info_hash.eq_ignore_ascii_case(preferred_info_hash))
    else {
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "selected stream is not linked to this item".into(),
            })
            .await;
        return false;
    };

    match attempt_download(
        id,
        item,
        queue,
        stream,
        None,
        None,
        start_time,
        hierarchy,
    )
    .await
    {
        DownloadAttemptOutcome::Failed => return false,
        DownloadAttemptOutcome::TerminalHandled => return true,
        DownloadAttemptOutcome::Succeeded => {}
    }

    finalize_download_success(id, item, queue, start_time, None, None).await;
    true
}

async fn run_multi_version(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    active_profiles: &[(String, RankSettings)],
    streams: &[&Stream],
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    // Seed with profiles already downloaded before this job ran; augmented
    // below on each success so we avoid a second DB round-trip at the end.
    let mut done_profiles: HashSet<String> = fetch_done_profiles(queue, id, item.item_type)
        .await
        .into_iter()
        .collect();
    let mut any_success = false;
    let mut attempted_hashes: HashSet<String> = HashSet::new();

    for (profile_name, profile_settings) in active_profiles {
        if done_profiles.contains(profile_name.as_str()) {
            tracing::debug!(
                id,
                profile = profile_name,
                "download already exists for active profile, skipping"
            );
            continue;
        }

        let Some(stream) = pick_best_for_profile(streams, item, profile_settings) else {
            tracing::debug!(
                id,
                profile = profile_name,
                "no valid stream found for profile"
            );
            continue;
        };

        if attempted_hashes.contains(&stream.info_hash) {
            tracing::debug!(
                id,
                profile = profile_name,
                info_hash = %stream.info_hash,
                "skipping already-attempted stream for this profile"
            );
            continue;
        }

        match attempt_download(
            id,
            item,
            queue,
            stream,
            Some(profile_name.as_str()),
            Some(profile_name.as_str()),
            start_time,
            hierarchy,
        )
        .await
        {
            DownloadAttemptOutcome::Failed => {
                attempted_hashes.insert(stream.info_hash.clone());
            }
            DownloadAttemptOutcome::TerminalHandled => return true,
            DownloadAttemptOutcome::Succeeded => {
                done_profiles.insert(profile_name.clone());
                any_success = true;
                tracing::debug!(
                    id,
                    profile = profile_name,
                    "downloaded stream for active profile"
                );
            }
        }
    }

    if !any_success {
        tracing::debug!(id, title = %item.title, "no valid torrent found after trying ranked streams");
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "no valid torrent found after trying ranked streams".into(),
            })
            .await;
        return false;
    }

    if !active_profiles
        .iter()
        .all(|(name, _)| done_profiles.contains(name.as_str()))
    {
        tracing::debug!(
            id,
            "downloads for some active profiles are still missing; re-queuing"
        );
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
    }

    finalize_download_success(id, item, queue, start_time, None, None).await;
    true
}

async fn run_single_version(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    streams: &[&Stream],
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    for &stream in streams {
        match attempt_download(
            id,
            item,
            queue,
            stream,
            None,
            None,
            start_time,
            hierarchy,
        )
        .await
        {
            DownloadAttemptOutcome::Failed => continue,
            DownloadAttemptOutcome::TerminalHandled => return true,
            DownloadAttemptOutcome::Succeeded => {
                finalize_download_success(id, item, queue, start_time, None, None).await;
                return true;
            }
        }
    }

    tracing::debug!(id, title = %item.title, "no valid torrent found after trying ranked streams");
    queue
        .notify(RivenEvent::MediaItemDownloadError {
            id,
            title: item.title.clone(),
            error: "no valid torrent found after trying ranked streams".into(),
        })
        .await;
    false
}

async fn fetch_done_profiles(queue: &JobQueue, id: i64, item_type: MediaItemType) -> Vec<String> {
    let result = if item_type == MediaItemType::Season {
        repo::get_downloaded_profile_names_for_season(&queue.db_pool, id).await
    } else {
        repo::get_downloaded_profile_names(&queue.db_pool, id).await
    };
    result.unwrap_or_default()
}
