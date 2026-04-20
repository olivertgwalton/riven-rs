mod candidates;
mod execute;

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use riven_core::events::{CacheCheckPurpose, HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;
use riven_rank::RankSettings;
use serde::Deserialize;

use crate::context::{DownloadHierarchyContext, load_download_hierarchy_context};
use crate::flows::download_item::helpers::load_item_or_err;
use crate::flows::download_item::persist::{finalize_download_success, persist_supplied_download};
use crate::flows::load_active_profiles;
use crate::{DownloadJob, JobQueue};

use self::candidates::{filter_by_bitrate, rank_streams_for_profile};
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
    let mut all_streams = match repo::get_non_blacklisted_streams(&queue.db_pool, id, &ranks).await {
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

    // Batch /torz/check up front: warms Redis so per-stream add requests skip
    // the per-hash API call, and returns availability so we can drop hashes no
    // store reports as cached before burning a debrid round trip.
    let cache_status = warm_and_collect_cache(queue, &all_streams).await;
    enrich_stream_sizes(&mut all_streams, item.item_type, &cache_status);

    let config = queue.downloader_config.read().await.clone();
    let mut streams_to_try: Vec<&Stream> =
        filter_by_bitrate(all_streams.iter().collect(), &item, &config);

    streams_to_try = filter_by_cache_status(streams_to_try, &cache_status, id);

    if streams_to_try.is_empty() && job.preferred_info_hash.is_none() {
        tracing::debug!(id, "no streams survived prefilter");
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "no valid streams after prefilter".into(),
            })
            .await;
        return;
    }

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

        let ranked = rank_streams_for_profile(streams, item, profile_settings);
        if ranked.is_empty() {
            tracing::debug!(id, profile = profile_name, "no streams match profile filters");
            continue;
        }

        let mut profile_succeeded = false;
        for stream in ranked {
            if attempted_hashes.contains(&stream.info_hash) {
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
                    profile_succeeded = true;
                    tracing::debug!(id, profile = profile_name, "downloaded stream for active profile");
                    break;
                }
            }
        }

        if !profile_succeeded {
            tracing::debug!(id, profile = profile_name, "no available stream found for profile");
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

#[derive(Default)]
struct CacheStatusMap {
    /// Keyed by lowercased info_hash. Absent = no plugin responded with data
    /// for this hash (treat as unknown — don't filter it out).
    entries: HashMap<String, CacheStatusEntry>,
    any_response: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Availability {
    /// At least one store reported the hash as cached/downloaded.
    Available,
    /// All responding stores reported a definite negative status (Failed,
    /// Invalid, Queued, Downloading, Processing, Uploading). Safe to drop.
    Unavailable,
    /// A store responded with `Unknown` (e.g. direct-mode masking) — we have
    /// size info but must not gate download on the availability claim.
    Unknown,
}

struct CacheStatusEntry {
    availability: Availability,
    total_size: Option<u64>,
}

async fn warm_and_collect_cache(queue: &JobQueue, streams: &[Stream]) -> CacheStatusMap {
    let hashes: Vec<String> = streams
        .iter()
        .map(|s| s.info_hash.to_lowercase())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if hashes.is_empty() {
        return CacheStatusMap::default();
    }

    let results = queue
        .registry
        .dispatch(&RivenEvent::MediaItemDownloadCacheCheckRequested {
            hashes,
            bypass_cache: vec![],
            purpose: CacheCheckPurpose::DownloadFlow,
        })
        .await;

    let mut map = CacheStatusMap::default();
    for (_, result) in results {
        let Ok(HookResponse::CacheCheck(items)) = result else {
            continue;
        };
        map.any_response = true;
        for item in items {
            let hash = item.hash.to_lowercase();
            let availability = match item.status {
                TorrentStatus::Cached | TorrentStatus::Downloaded => Availability::Available,
                TorrentStatus::Unknown => Availability::Unknown,
                _ => Availability::Unavailable,
            };
            let total: u64 = item.files.iter().filter_map(|f| f.size).sum();
            let total_size = (total > 0).then_some(total);
            map.entries
                .entry(hash)
                .and_modify(|e| {
                    // Best signal wins: Available > Unknown > Unavailable.
                    if availability == Availability::Available
                        || (availability == Availability::Unknown
                            && e.availability == Availability::Unavailable)
                    {
                        e.availability = availability;
                    }
                    if total_size.is_some()
                        && (e.total_size.is_none() || total_size > e.total_size)
                    {
                        e.total_size = total_size;
                    }
                })
                .or_insert(CacheStatusEntry {
                    availability,
                    total_size,
                });
        }
    }
    map
}

fn enrich_stream_sizes(
    streams: &mut [Stream],
    item_type: MediaItemType,
    cache_status: &CacheStatusMap,
) {
    // Only safe for movies: total torrent size ≈ movie file size. For
    // episode/season packs the cache-check total is the whole pack, not a
    // single episode file, so it would mislead the bitrate filter.
    if item_type != MediaItemType::Movie {
        return;
    }
    for stream in streams.iter_mut() {
        if stream.file_size_bytes.is_some() {
            continue;
        }
        let Some(entry) = cache_status.entries.get(&stream.info_hash.to_lowercase()) else {
            continue;
        };
        if let Some(size) = entry.total_size {
            stream.file_size_bytes = Some(size as i64);
        }
    }
}

fn filter_by_cache_status<'a>(
    streams: Vec<&'a Stream>,
    cache_status: &CacheStatusMap,
    id: i64,
) -> Vec<&'a Stream> {
    // No plugin returned cache data (e.g. all debrid plugins are in direct
    // mode). Fall back to the full list — availability will be decided
    // per-stream via add_torrent.
    if !cache_status.any_response {
        return streams;
    }
    streams
        .into_iter()
        .filter(|stream| {
            match cache_status.entries.get(&stream.info_hash.to_lowercase()) {
                Some(entry) if entry.availability == Availability::Unavailable => {
                    tracing::debug!(
                        id,
                        info_hash = %stream.info_hash,
                        "stream prefiltered: not cached on any store"
                    );
                    false
                }
                // Available, Unknown, or absent — keep. Unknown means a plugin
                // (e.g. direct-mode debrid) surfaced size info but won't
                // commit to an availability claim; add_torrent decides.
                _ => true,
            }
        })
        .collect()
}

async fn fetch_done_profiles(queue: &JobQueue, id: i64, item_type: MediaItemType) -> Vec<String> {
    let result = if item_type == MediaItemType::Season {
        repo::get_downloaded_profile_names_for_season(&queue.db_pool, id).await
    } else {
        repo::get_downloaded_profile_names(&queue.db_pool, id).await
    };
    result.unwrap_or_default()
}
