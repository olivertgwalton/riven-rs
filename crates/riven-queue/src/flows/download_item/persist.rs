use std::time::Instant;

use riven_core::events::RivenEvent;
use riven_core::settings::{FilesystemContentType, FilesystemItemMetadata};
use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

use super::helpers::{
    episode_vfs_path, handle_bitrate_failure, is_video_file, load_item_or_err, matches_episode,
    parse_file_path, select_episode_files,
};
use crate::orchestrator::LibraryOrchestrator;
use crate::JobQueue;

fn metadata_from_item(item: &MediaItem) -> FilesystemItemMetadata {
    let genres = item
        .genres
        .as_ref()
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str())
                .map(|value| value.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    FilesystemItemMetadata {
        genres,
        content_rating: item.content_rating,
        language: item.language.clone(),
        country: item.country.clone(),
        is_anime: item.is_anime,
    }
}

/// Persist a movie download result. Returns `true` on success.
///
/// `stream_id` links the created entry to the source stream for version tracking.
/// `resolution` is stored in the DB for metadata.
/// `path_tag` is embedded in the VFS filename when `Some` (multi-version mode).
/// `profile_name` is stored on the entry for version-profile tracking.
pub async fn persist_movie(
    item: &MediaItem,
    dl: &DownloadResult,
    info_hash: &str,
    queue: &JobQueue,
    stream_id: Option<i64>,
    resolution: Option<&str>,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
) -> bool {
    let id = item.id;

    tracing::debug!(id, info_hash, files = dl.files.len(), "persisting movie");

    let mut video_files: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .filter(|f| is_video_file(&f.filename))
        .map(|f| (f, parse_file_path(&f.filename)))
        .filter(|(_, parsed)| parsed.media_type() == "movie")
        .collect();

    video_files.sort_by_key(|(f, _)| std::cmp::Reverse(f.file_size));

    let file = if let Some(first) = video_files.first() {
        first.0
    } else if let Some(largest) = dl.files.iter().max_by_key(|f| f.file_size) {
        tracing::warn!(
            id,
            info_hash,
            "no movie file found in torrent; falling back to largest file"
        );
        largest
    } else {
        tracing::warn!(id, info_hash = %info_hash, "torrent has no files — blacklisting stream");
        if !info_hash.is_empty() {
            let _ = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await;
        }
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        LibraryOrchestrator::new(queue)
            .fan_out_download_failure(id)
            .await;
        return false;
    };

    let config = queue.downloader_config.read().await;
    if !config.movie_passes(file.file_size, item.runtime) {
        handle_bitrate_failure(id, info_hash, file.file_size, item.runtime, "movie", queue).await;
        return false;
    }
    drop(config);

    let ext = file.filename.rsplit('.').next().unwrap_or("mkv");
    let tag_suffix = path_tag.map(|t| format!(" [{t}]")).unwrap_or_default();
    let base_name = item.pretty_name();
    let vfs_name = format!("{base_name}{tag_suffix}.{ext}");
    let path = format!("/movies/{base_name}/{vfs_name}");
    let metadata = metadata_from_item(item);
    let filesystem_settings = queue.filesystem_settings.read().await;
    let library_profiles =
        filesystem_settings.matching_profile_keys(&metadata, FilesystemContentType::Movie);
    let library_profiles_json = library_profiles.into_json();
    drop(filesystem_settings);

    if let Err(e) = repo::create_media_entry(
        &queue.db_pool,
        id,
        &path,
        file.file_size as i64,
        &file.filename,
        file.download_url.as_deref(),
        file.stream_url.as_deref(),
        &dl.plugin_name,
        dl.provider.as_deref(),
        stream_id,
        resolution,
        profile_name,
        Some(&library_profiles_json),
    )
    .await
    {
        tracing::error!(error = %e, "failed to create media entry");
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: e.to_string(),
            })
            .await;
        return false;
    }

    true
}

/// Persist an episode download result. Returns `true` on success.
pub async fn persist_episode(
    item: &MediaItem,
    dl: &DownloadResult,
    info_hash: &str,
    queue: &JobQueue,
    stream_id: Option<i64>,
    resolution: Option<&str>,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
) -> bool {
    let id = item.id;

    let season_id = match item.parent_id {
        Some(sid) => sid,
        None => {
            tracing::error!(id, "episode has no parent_id");
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: "episode has no parent season".into(),
                })
                .await;
            return false;
        }
    };

    let season = match load_item_or_err(season_id, queue, "could not load parent season").await {
        Some(s) => s,
        None => return false,
    };

    let show_id = match season.parent_id {
        Some(sid) => sid,
        None => {
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: "season has no parent show".into(),
                })
                .await;
            return false;
        }
    };

    let show = match load_item_or_err(show_id, queue, "could not load parent show").await {
        Some(s) => s,
        None => return false,
    };

    let season_number = season.season_number.unwrap_or(1);
    let episode_number = item.episode_number.unwrap_or(1);
    let metadata = metadata_from_item(&show);
    let filesystem_settings = queue.filesystem_settings.read().await;
    let library_profiles =
        filesystem_settings.matching_profile_keys(&metadata, FilesystemContentType::Show);
    let library_profiles_json = library_profiles.into_json();
    drop(filesystem_settings);

    tracing::debug!(id, info_hash, files = dl.files.len(), "persisting episode");

    let matched: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .filter(|f| is_video_file(&f.filename))
        .map(|f| (f, parse_file_path(&f.filename)))
        .filter(|(_, p)| matches_episode(p, season_number, episode_number, item.absolute_number))
        .collect();

    if matched.is_empty() {
        tracing::info!(
            id, season = season_number, episode = episode_number,
            info_hash = %info_hash,
            "no torrent file matched episode — blacklisting stream"
        );
        if !info_hash.is_empty() {
            let _ = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await;
        }
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        LibraryOrchestrator::new(queue)
            .fan_out_download_failure(id)
            .await;
        return false;
    }

    let largest = matched.iter().max_by_key(|(f, _)| f.file_size).unwrap().0;
    let config = queue.downloader_config.read().await;
    if !config.episode_passes(largest.file_size, item.runtime) {
        drop(config);
        handle_bitrate_failure(
            id,
            info_hash,
            largest.file_size,
            item.runtime,
            "episode",
            queue,
        )
        .await;
        return false;
    }
    drop(config);

    let show_name = show.pretty_name();
    for (file, part) in select_episode_files(&matched) {
        let path = episode_vfs_path(&show_name, season_number, episode_number, part, path_tag);
        if let Err(e) = repo::create_media_entry(
            &queue.db_pool,
            id,
            &path,
            file.file_size as i64,
            &file.filename,
            file.download_url.as_deref(),
            file.stream_url.as_deref(),
            &dl.plugin_name,
            dl.provider.as_deref(),
            stream_id,
            resolution,
            profile_name,
            Some(&library_profiles_json),
        )
        .await
        {
            tracing::error!(error = %e, "failed to create media entry");
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: e.to_string(),
                })
                .await;
            return false;
        }
    }

    true
}

/// Persist a season (pack) download result.
/// On success this function also sends the `MediaItemDownloadSuccess` event and
/// updates state, so the caller should return immediately after.
pub async fn persist_season(
    item: &MediaItem,
    dl: DownloadResult,
    info_hash: &str,
    queue: &JobQueue,
    start_time: Instant,
    stream_id: Option<i64>,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
) -> bool {
    let id = item.id;

    let show_id = match item.parent_id {
        Some(sid) => sid,
        None => {
            tracing::error!(id, "season has no parent show");
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: "season has no parent show".into(),
                })
                .await;
            return false;
        }
    };

    let show = match load_item_or_err(show_id, queue, "could not load parent show").await {
        Some(s) => s,
        None => return false,
    };

    let episodes = match repo::list_episodes(&queue.db_pool, id).await {
        Ok(eps) => eps,
        Err(e) => {
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: e.to_string(),
                })
                .await;
            return false;
        }
    };

    let season_number = item.season_number.unwrap_or(1);
    let show_name = show.pretty_name();
    let metadata = metadata_from_item(&show);
    let filesystem_settings = queue.filesystem_settings.read().await;
    let library_profiles =
        filesystem_settings.matching_profile_keys(&metadata, FilesystemContentType::Show);
    let library_profiles_json = library_profiles.into_json();
    drop(filesystem_settings);
    let mut any_matched = false;
    let mut completed_episode_ids: Vec<i64> = Vec::new();

    // Pre-parse all video files once — reused across every episode filter below.
    let parsed_video_files: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .filter(|f| is_video_file(&f.filename))
        .map(|f| (f, parse_file_path(&f.filename)))
        .collect();

    let config = queue.downloader_config.read().await;

    for ep in &episodes {
        let episode_number = ep.episode_number.unwrap_or(1);

        let matched: Vec<(&DownloadFile, riven_rank::ParsedData)> = parsed_video_files
            .iter()
            .filter(|(_, p)| matches_episode(p, season_number, episode_number, ep.absolute_number))
            .map(|(f, p)| (*f, p.clone()))
            .collect();

        if matched.is_empty() {
            continue;
        }

        let largest = matched.iter().max_by_key(|(f, _)| f.file_size).unwrap().0;
        if !config.episode_passes(largest.file_size, ep.runtime) {
            tracing::info!(
                id = ep.id, file_size = largest.file_size, runtime = ?ep.runtime,
                info_hash = %info_hash,
                "episode failed bitrate check in season download — blacklisting stream for episode"
            );
            if !info_hash.is_empty() {
                let _ = repo::blacklist_stream_by_hash(&queue.db_pool, ep.id, info_hash).await;
                let _ = repo::update_stream_file_size(&queue.db_pool, info_hash, largest.file_size)
                    .await;
            }
            continue;
        }

        for (file, part) in select_episode_files(&matched) {
            let path = episode_vfs_path(&show_name, season_number, episode_number, part, path_tag);
            match repo::create_media_entry(
                &queue.db_pool,
                ep.id,
                &path,
                file.file_size as i64,
                &file.filename,
                file.download_url.as_deref(),
                file.stream_url.as_deref(),
                &dl.plugin_name,
                dl.provider.as_deref(),
                stream_id,
                None,
                profile_name,
                Some(&library_profiles_json),
            )
            .await
            {
                Ok(_) => {
                    completed_episode_ids.push(ep.id);
                    any_matched = true;
                }
                Err(e) => {
                    tracing::error!(error = %e, ep_id = ep.id, "failed to create media entry for episode");
                }
            }
        }
    }

    drop(config);

    if !any_matched {
        tracing::info!(
            id, season = season_number, info_hash = %info_hash,
            "no episodes matched in season download — blacklisting stream"
        );
        if !info_hash.is_empty() {
            let _ = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await;
        }
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        LibraryOrchestrator::new(queue)
            .fan_out_download_failure(id)
            .await;
        return false;
    }

    // Batch-set all matched episodes to Completed in one UPDATE, then refresh
    // season and show states once each — replaces N×(SELECT+UPDATE) per episode
    // with 1 batch UPDATE + 2 lightweight state refreshes.
    if let Err(e) = repo::batch_set_completed(&queue.db_pool, &completed_episode_ids).await {
        tracing::error!(error = %e, "failed to batch-set episodes completed");
    }
    if let Err(e) = repo::refresh_state(&queue.db_pool, item).await {
        tracing::error!(error = %e, "failed to refresh season state after download");
    }
    if let Ok(Some(show_item)) = repo::get_media_item(&queue.db_pool, show_id).await {
        if let Err(e) = repo::refresh_state(&queue.db_pool, &show_item).await {
            tracing::error!(error = %e, "failed to refresh show state after download");
        }
    }
    LibraryOrchestrator::new(queue)
        .sync_item_request_state(item)
        .await;

    let duration = start_time.elapsed();
    let display_title = format!("{} - {}", show.title, item.title);
    queue
        .notify(RivenEvent::MediaItemDownloadSuccess {
            id,
            title: display_title,
            full_title: item.full_title.clone(),
            item_type: item.item_type,
            year: item.year,
            imdb_id: item.imdb_id.clone(),
            tmdb_id: item.tmdb_id.clone(),
            poster_path: item.poster_path.clone(),
            plugin_name: dl.plugin_name,
            provider: dl.provider,
            duration_seconds: duration.as_secs_f64(),
        })
        .await;
    tracing::info!(
        id,
        duration_secs = duration.as_secs_f64(),
        "season download flow completed"
    );
    true
}
