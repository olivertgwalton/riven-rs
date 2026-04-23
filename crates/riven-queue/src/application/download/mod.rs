mod candidates;
mod execute;

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::{self, *};
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;
use riven_rank::{QualityProfile, RankSettings};
use serde::Deserialize;

use crate::context::{DownloadHierarchyContext, load_download_hierarchy_context};
use crate::flows::download_item::helpers::load_item_or_err;
use crate::flows::download_item::persist::{finalize_download_success, persist_supplied_download};
use crate::flows::load_active_profiles;
use crate::{DownloadJob, JobQueue, RankStreamsJob};

use self::candidates::{
    CachedCandidate, build_cached_candidates, find_preferred_candidate, rank_candidates_for_profile,
};
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

/// Step 1 of the download flow loads the
/// item, fetches non-blacklisted streams, asks every downloader plugin for
/// their cache check, and stores the ranked result in Redis for the
/// `DownloadJob` (find-valid-torrent + persist) to pick up.
pub async fn run_rank_streams(id: i64, job: &RankStreamsJob, queue: &JobQueue) {
    tracing::debug!(id, "running rank-streams step");

    let Some(item) = load_item_silently(queue, id, "rank-streams").await else {
        return;
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

    let attempt_unknown = queue.downloader_config.read().await.attempt_unknown_downloads;
    let (cached_info, cache_checked) = collect_cached_info(queue, &all_streams, attempt_unknown).await;

    let preferred = job.preferred_info_hash.clone();
    let magnet_for_preferred = preferred
        .as_ref()
        .and_then(|hash| {
            all_streams
                .iter()
                .find(|stream| stream.info_hash.eq_ignore_ascii_case(hash))
        })
        .map(|stream| stream.magnet.clone());

    store_rank_result(
        queue,
        id,
        &RankStreamsResult {
            cached_info,
            cache_checked,
        },
    )
    .await;

    // For the preferred path we carry info_hash/magnet through to the next
    // step; for the normal path placeholders are fine since DownloadJob only
    // consults `preferred_info_hash` for selection.
    let download_job = DownloadJob {
        id,
        info_hash: preferred.clone().unwrap_or_default(),
        magnet: magnet_for_preferred.unwrap_or_default(),
        preferred_info_hash: preferred,
    };
    // Clear any stale `download` dedup key before the hand-off. The rank-streams
    // dedup guard ensures we are the sole caller for this id, so a lingering
    // key can only come from an earlier crash/restart (30-min safety TTL). If
    // we don't clear it, `push_deduped` silently no-ops and the item is stuck
    // at Scraped.
    queue.release_dedup("download", id).await;
    queue.push_download(download_job).await;
}

/// find-valid-torrent` + `download-item`
/// fused. Reloads the item (matches `findOneOrFail` semantics at the step
/// boundary), loads the ranked result produced by `run_rank_streams`, and
/// walks cached candidates until one downloads + persists successfully.
pub async fn run(id: i64, job: &DownloadJob, queue: &JobQueue) {
    let start_time = Instant::now();
    tracing::debug!(id, "running download (find-valid-torrent + persist) step");

    let Some(item) = load_item_silently(queue, id, "download").await else {
        return;
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

    let mut active_profiles: Vec<(String, RankSettings)> =
        load_active_profiles(&queue.db_pool).await;
    if active_profiles.is_empty() {
        active_profiles.push((
            "ultra_hd".to_string(),
            QualityProfile::UltraHd.base_settings().prepare(),
        ));
    }

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

    // Prefer the handed-off ranked result. If it's missing (legacy job, TTL
    // expiry, or the rank step crashed after enqueueing us), recompute.
    let ranked = match load_rank_result(queue, id).await {
        Some(result) => result,
        None => {
            tracing::debug!(id, "rank-streams result missing; recomputing inline");
            let attempt_unknown = queue.downloader_config.read().await.attempt_unknown_downloads;
            let (cached_info, cache_checked) =
                collect_cached_info(queue, &all_streams, attempt_unknown).await;
            RankStreamsResult {
                cached_info,
                cache_checked,
            }
        }
    };

    let is_manual = job.preferred_info_hash.is_some();
    let (max_size_bytes, min_size_bytes) = if is_manual {
        (None, None)
    } else {
        load_bitrate_limits(queue, &item).await
    };
    let mut candidates: Vec<CachedCandidate<'_>> = if ranked.cache_checked {
        build_cached_candidates(
            id,
            &item,
            hierarchy.as_ref(),
            &all_streams,
            &ranked.cached_info,
            max_size_bytes,
            min_size_bytes,
        )
    } else {
        // Direct mode: no plugin did a cache check — pass all streams through with empty stores.
        all_streams
            .iter()
            .map(|s| CachedCandidate {
                stream: s,
                stores: vec![],
            })
            .collect()
    };

    // For manually chosen streams, ensure the preferred hash is always in candidates even
    // if the download-job cache check didn't confirm it (e.g. Redis miss, transient API
    // variance). The user explicitly selected this stream from the scrape UI where it was
    // already verified as cached; we trust that choice and let attempt_download do the
    // final check. Empty stores triggers an on-demand cache check in the plugin.
    if let Some(preferred) = job.preferred_info_hash.as_deref() {
        if !candidates
            .iter()
            .any(|c| c.stream.info_hash.eq_ignore_ascii_case(preferred))
        {
            if let Some(stream) = all_streams
                .iter()
                .find(|s| s.info_hash.eq_ignore_ascii_case(preferred))
            {
                let stores = ranked
                    .cached_info
                    .get(&stream.info_hash.to_lowercase())
                    .cloned()
                    .unwrap_or_default();
                tracing::debug!(
                    id,
                    info_hash = %stream.info_hash,
                    "preferred stream not in cache-checked candidates; including for direct attempt"
                );
                candidates.push(CachedCandidate { stream, stores });
            }
        }
    }

    if let Some(preferred_info_hash) = job.preferred_info_hash.as_ref() {
        let _ = run_preferred_stream(
            id,
            &item,
            queue,
            start_time,
            preferred_info_hash,
            &candidates,
            hierarchy.as_ref(),
        )
        .await;
    } else {
        let _ = run_downloads(
            id,
            &item,
            queue,
            start_time,
            &active_profiles,
            &candidates,
            hierarchy.as_ref(),
        )
        .await;
    }

    clear_rank_result(queue, id).await;
}

/// Load the media item; return `None` (without emitting a user-visible event)
/// when it's gone
async fn load_item_silently(queue: &JobQueue, id: i64, phase: &str) -> Option<MediaItem> {
    match repo::get_media_item(&queue.db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => {
            tracing::debug!(
                id,
                phase,
                "media item disappeared before phase ran; skipping"
            );
            None
        }
        Err(error) => {
            tracing::error!(id, phase, %error, "failed to load media item");
            None
        }
    }
}

// ── Ranked-state hand-off between steps ───────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RankStreamsResult {
    cached_info: HashMap<String, Vec<CachedStoreEntry>>,
    cache_checked: bool,
}

fn rank_result_key(id: i64) -> String {
    format!("riven:download:rank-result:{id}")
}

async fn store_rank_result(queue: &JobQueue, id: i64, result: &RankStreamsResult) {
    let Ok(payload) = serde_json::to_string(result) else {
        tracing::error!(id, "failed to serialize rank-streams result");
        return;
    };
    let mut conn = queue.redis.clone();
    let _: Result<(), _> = redis::cmd("SET")
        .arg(rank_result_key(id))
        .arg(payload)
        .arg("EX")
        .arg(3600i64)
        .query_async(&mut conn)
        .await;
}

async fn load_rank_result(queue: &JobQueue, id: i64) -> Option<RankStreamsResult> {
    let mut conn = queue.redis.clone();
    let raw: Option<String> = redis::cmd("GET")
        .arg(rank_result_key(id))
        .query_async(&mut conn)
        .await
        .ok()
        .flatten();
    raw.and_then(|s| serde_json::from_str(&s).ok())
}

async fn clear_rank_result(queue: &JobQueue, id: i64) {
    let mut conn = queue.redis.clone();
    let _: Result<(), _> = redis::cmd("DEL")
        .arg(rank_result_key(id))
        .query_async(&mut conn)
        .await;
}

/// Returns `(cached_info, any_plugin_responded)`.
/// `any_plugin_responded` is false only when every plugin returned `Empty` (direct mode).
/// A plugin error counts as "responded" so we never accidentally fall into direct mode.
async fn collect_cached_info(
    queue: &JobQueue,
    streams: &[Stream],
    attempt_unknown_downloads: bool,
) -> (HashMap<String, Vec<CachedStoreEntry>>, bool) {
    let hashes: Vec<String> = streams.iter().map(|s| s.info_hash.clone()).collect();
    let cache_event = RivenEvent::MediaItemDownloadCacheCheckRequested { hashes };
    let cache_results = queue.registry.dispatch(&cache_event).await;

    let mut cached_info: HashMap<String, Vec<CachedStoreEntry>> = HashMap::new();
    let mut any_responded = false;
    for (_, result) in cache_results {
        match result {
            Ok(HookResponse::CacheCheck(results)) => {
                any_responded = true;
                for result in results {
                    let is_candidate = matches!(
                        result.status,
                        TorrentStatus::Cached | TorrentStatus::Downloaded
                    ) || (attempt_unknown_downloads
                        && result.status == TorrentStatus::Unknown);
                    if is_candidate {
                        cached_info
                            .entry(result.hash.to_lowercase())
                            .or_default()
                            .push(types::CachedStoreEntry {
                                store: result.store,
                                files: result.files,
                            });
                    }
                }
            }
            Ok(HookResponse::Empty) => {}
            Err(_) => {
                any_responded = true;
            }
            _ => {}
        }
    }
    (cached_info, any_responded)
}

async fn load_bitrate_limits(queue: &JobQueue, item: &MediaItem) -> (Option<u64>, Option<u64>) {
    let config = queue.downloader_config.read().await;
    let bitrate_threshold = |movies: Option<u32>, episodes: Option<u32>| {
        let mbps = match item.item_type {
            MediaItemType::Movie => movies,
            MediaItemType::Episode => episodes,
            _ => None,
        };
        mbps.zip(item.runtime)
            .map(|(m, rt)| riven_core::downloader::DownloaderConfig::threshold_bytes(m, rt))
    };

    let max_size_bytes = bitrate_threshold(
        config.maximum_average_bitrate_movies,
        config.maximum_average_bitrate_episodes,
    );
    let min_size_bytes = bitrate_threshold(
        config.minimum_average_bitrate_movies,
        config.minimum_average_bitrate_episodes,
    );
    (max_size_bytes, min_size_bytes)
}

async fn run_preferred_stream(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    preferred_info_hash: &str,
    candidates: &[CachedCandidate<'_>],
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    let Some(candidate) = find_preferred_candidate(candidates, preferred_info_hash) else {
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "selected stream is not cached, valid, or linked to this item".into(),
            })
            .await;
        return false;
    };

    match attempt_download(
        id,
        item,
        queue,
        candidate.stream,
        candidate.stores.clone(),
        None,
        None,
        start_time,
        hierarchy,
        true,
    )
    .await
    {
        DownloadAttemptOutcome::Failed => {
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: "selected stream could not be downloaded from any provider".into(),
                })
                .await;
            return false;
        }
        DownloadAttemptOutcome::TerminalHandled => return true,
        DownloadAttemptOutcome::Succeeded => {}
    }

    finalize_download_success(id, item, queue, start_time, None, None).await;
    true
}

async fn run_downloads(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    profiles: &[(String, RankSettings)],
    candidates: &[CachedCandidate<'_>],
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    let mut done_profiles: HashSet<String> = fetch_done_profiles(queue, id, item.item_type)
        .await
        .into_iter()
        .collect();
    let mut any_success = false;
    let mut attempted_hashes: HashSet<String> = HashSet::new();

    for (profile_name, profile_settings) in profiles {
        if done_profiles.contains(profile_name.as_str()) {
            tracing::debug!(
                id,
                profile = profile_name,
                "profile already downloaded, skipping"
            );
            continue;
        }

        let ranked = rank_candidates_for_profile(candidates, item, profile_settings);
        if ranked.is_empty() {
            tracing::debug!(
                id,
                profile = profile_name,
                "no cached stream found for profile"
            );
            continue;
        }

        for candidate in ranked {
            if queue.is_cancelled(id).await {
                tracing::debug!(id, "item cancelled; aborting candidate loop");
                return false;
            }
            if attempted_hashes.contains(&candidate.stream.info_hash) {
                continue;
            }

            match attempt_download(
                id,
                item,
                queue,
                candidate.stream,
                candidate.stores.clone(),
                Some(profile_name.as_str()),
                Some(profile_name.as_str()),
                start_time,
                hierarchy,
                false,
            )
            .await
            {
                DownloadAttemptOutcome::Failed => {
                    attempted_hashes.insert(candidate.stream.info_hash.clone());
                }
                DownloadAttemptOutcome::TerminalHandled => return true,
                DownloadAttemptOutcome::Succeeded => {
                    done_profiles.insert(profile_name.clone());
                    any_success = true;
                    break;
                }
            }
        }
    }

    if !any_success {
        tracing::debug!(id, title = %item.title, "no valid torrent found after trying cached candidates");
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "no valid torrent found after trying cached candidates".into(),
            })
            .await;
        return false;
    }

    if !profiles
        .iter()
        .all(|(name, _)| done_profiles.contains(name.as_str()))
    {
        tracing::debug!(id, "some profiles still missing a download; re-queuing");
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
    }

    finalize_download_success(id, item, queue, start_time, None, None).await;
    true
}

async fn fetch_done_profiles(queue: &JobQueue, id: i64, item_type: MediaItemType) -> Vec<String> {
    let result = if item_type == MediaItemType::Season {
        repo::get_downloaded_profile_names_for_season(&queue.db_pool, id).await
    } else {
        repo::get_downloaded_profile_names(&queue.db_pool, id).await
    };
    result.unwrap_or_default()
}
