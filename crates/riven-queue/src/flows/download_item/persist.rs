use std::time::Instant;

use riven_core::events::RivenEvent;
use riven_core::settings::{FilesystemContentType, FilesystemItemMetadata};
use riven_core::types::*;
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;

use super::helpers::{
    episode_vfs_path, handle_bitrate_failure, is_video_file, load_item_or_err,
    matches_episode_lookup, parse_file_path, select_episode_files,
};
use crate::JobQueue;
use crate::context::{DownloadHierarchyContext, load_download_hierarchy_context};
use crate::orchestrator::LibraryOrchestrator;
pub enum SeasonPersistOutcome {
    Failed,
    Partial,
    Complete,
}

pub(crate) fn metadata_from_show_context(ctx: &DownloadHierarchyContext) -> FilesystemItemMetadata {
    let genres = ctx
        .show_genres
        .as_ref()
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str())
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    FilesystemItemMetadata {
        genres,
        network: ctx.show_network.clone(),
        content_rating: ctx.show_content_rating,
        language: ctx.show_language.clone(),
        country: ctx.show_country.clone(),
        year: ctx.show_year,
        rating: ctx.show_rating,
        is_anime: ctx.show_is_anime,
    }
}

pub(crate) fn pretty_show_name(ctx: &DownloadHierarchyContext, fallback_title: &str) -> String {
    let title = ctx.show_title.as_deref().unwrap_or(fallback_title);
    let year_str = ctx.show_year.map(|y| format!(" ({y})")).unwrap_or_default();
    let id_str = ctx
        .show_tvdb_id
        .as_ref()
        .map(|id| format!(" {{tvdb-{id}}}"))
        .unwrap_or_default();
    format!("{title}{year_str}{id_str}")
}

pub(crate) fn selected_stream_resolution(stream: &Stream) -> Option<&str> {
    Some(
        stream
            .parsed_data
            .as_ref()
            .and_then(|parsed| parsed.get("resolution"))
            .and_then(|value| value.as_str())
            .unwrap_or("unknown"),
    )
}

/// Persist a movie download result. Returns `true` on success.
///
/// `stream_id` links the created entry to the source stream for version tracking.
/// `resolution` is stored in the DB for metadata.
/// `path_tag` is embedded in the VFS filename when `Some` (active profile mode).
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
        if !info_hash.is_empty()
            && let Err(err) = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await
        {
            tracing::warn!(id, info_hash, %err, "failed to blacklist stream");
        }
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
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
    let metadata = item.filesystem_metadata();
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
    hierarchy: &crate::context::DownloadHierarchyContext,
    stream_id: Option<i64>,
    resolution: Option<&str>,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
) -> bool {
    let id = item.id;

    match hierarchy.season_id {
        Some(_) => {}
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
    }

    let season_number = hierarchy.season_number.unwrap_or(1);
    let episode_number = item.episode_number.unwrap_or(1);
    let metadata = metadata_from_show_context(hierarchy);
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
        .filter(|(_, p)| {
            matches_episode_lookup(p, season_number, episode_number, item.absolute_number)
        })
        .collect();

    if matched.is_empty() {
        tracing::info!(
            id, season = season_number, episode = episode_number,
            info_hash = %info_hash,
            "no torrent file matched episode — blacklisting stream"
        );
        if !info_hash.is_empty()
            && let Err(err) = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await
        {
            tracing::warn!(id, info_hash, %err, "failed to blacklist stream");
        }
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
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

    let show_name = pretty_show_name(hierarchy, &item.title);
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
    hierarchy: &crate::context::DownloadHierarchyContext,
    start_time: Instant,
    stream_id: Option<i64>,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
) -> SeasonPersistOutcome {
    let id = item.id;

    let show_id = match hierarchy.show_id {
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
            return SeasonPersistOutcome::Failed;
        }
    };

    let show = match load_item_or_err(show_id, queue, "could not load parent show").await {
        Some(s) => s,
        None => return SeasonPersistOutcome::Failed,
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
            return SeasonPersistOutcome::Failed;
        }
    };

    let season_number = hierarchy
        .season_number
        .unwrap_or_else(|| item.season_number.unwrap_or(1));
    let show_name = show.pretty_name();
    let metadata = metadata_from_show_context(hierarchy);
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

    for ep in &episodes {
        let episode_number = ep.episode_number.unwrap_or(1);

        let matched: Vec<(&DownloadFile, riven_rank::ParsedData)> = parsed_video_files
            .iter()
            .filter(|(_, p)| {
                matches_episode_lookup(p, season_number, episode_number, ep.absolute_number)
            })
            .map(|(f, p)| (*f, p.clone()))
            .collect();

        if matched.is_empty() {
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

    if !any_matched {
        tracing::info!(
            id, season = season_number, info_hash = %info_hash,
            "no episodes matched in season download — blacklisting stream"
        );
        if !info_hash.is_empty()
            && let Err(err) = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await
        {
            tracing::warn!(id, info_hash, %err, "failed to blacklist stream");
        }
        return SeasonPersistOutcome::Failed;
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
    if let Err(e) = repo::refresh_state(&queue.db_pool, &show).await {
        tracing::error!(error = %e, "failed to refresh show state after download");
    }
    LibraryOrchestrator::new(queue)
        .sync_item_request_state(item)
        .await;

    let season_complete = matches!(
        repo::compute_state(&queue.db_pool, item).await,
        Ok(MediaItemState::Completed)
    );

    if !season_complete {
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        return SeasonPersistOutcome::Partial;
    }

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
    SeasonPersistOutcome::Complete
}

pub async fn persist_supplied_download(
    item: &MediaItem,
    stream: &Stream,
    download: DownloadResult,
    queue: &JobQueue,
    start_time: Instant,
) -> anyhow::Result<()> {
    let info_hash = download.info_hash.clone();
    match item.item_type {
        MediaItemType::Movie => {
            if !persist_movie(
                item,
                &download,
                &info_hash,
                queue,
                Some(stream.id),
                selected_stream_resolution(stream),
                None,
                None,
            )
            .await
            {
                anyhow::bail!("failed to persist movie download");
            }

            finalize_download_success(
                item.id,
                item,
                queue,
                start_time,
                download.provider.clone(),
                Some(download.plugin_name.clone()),
            )
            .await;
        }
        MediaItemType::Episode => {
            let hierarchy = load_download_hierarchy_context(&queue.db_pool, item).await;
            if !persist_episode(
                item,
                &download,
                &info_hash,
                queue,
                &hierarchy,
                Some(stream.id),
                selected_stream_resolution(stream),
                None,
                None,
            )
            .await
            {
                anyhow::bail!("failed to persist episode download");
            }

            finalize_download_success(
                item.id,
                item,
                queue,
                start_time,
                download.provider.clone(),
                Some(download.plugin_name.clone()),
            )
            .await;
        }
        MediaItemType::Season => {
            let hierarchy = load_download_hierarchy_context(&queue.db_pool, item).await;
            match persist_season(
                item,
                download,
                &info_hash,
                queue,
                &hierarchy,
                start_time,
                Some(stream.id),
                None,
                None,
            )
            .await
            {
                SeasonPersistOutcome::Complete | SeasonPersistOutcome::Partial => {}
                SeasonPersistOutcome::Failed => anyhow::bail!("failed to persist season download"),
            }
        }
        MediaItemType::Show => {
            persist_supplied_show_download(item, stream, &download, queue, start_time).await?;
        }
    }
    Ok(())
}

async fn persist_supplied_show_download(
    item: &MediaItem,
    stream: &Stream,
    download: &DownloadResult,
    queue: &JobQueue,
    start_time: Instant,
) -> anyhow::Result<()> {
    let matched_files: Vec<_> = download
        .files
        .iter()
        .filter_map(|file| {
            file.stream_url
                .as_deref()
                .and_then(|value| value.strip_prefix("matched:"))
                .and_then(|value| value.parse::<i64>().ok())
                .map(|episode_id| (file, episode_id))
        })
        .collect();

    if matched_files.is_empty() {
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id: item.id })
            .await;
        anyhow::bail!("show downloads require matched media item ids");
    }

    let episode_ids: Vec<i64> = matched_files
        .iter()
        .map(|(_, episode_id)| *episode_id)
        .collect();
    let episodes = repo::list_media_items_by_ids(&queue.db_pool, &episode_ids).await?;
    let episode_map: std::collections::HashMap<i64, MediaItem> = episodes
        .into_iter()
        .map(|episode| (episode.id, episode))
        .collect();

    let mut completed_episode_ids: Vec<i64> = Vec::new();
    let mut season_ids: Vec<i64> = Vec::new();

    for (file, episode_id) in matched_files {
        let Some(episode) = episode_map.get(&episode_id) else {
            anyhow::bail!("matched episode {episode_id} not found");
        };
        if episode.item_type != MediaItemType::Episode {
            anyhow::bail!("matched item {episode_id} is not an episode");
        }
        if !matches!(
            episode.state,
            MediaItemState::Indexed
                | MediaItemState::Scraped
                | MediaItemState::Ongoing
                | MediaItemState::PartiallyCompleted
                | MediaItemState::Unreleased
        ) {
            continue;
        }

        let hierarchy = load_download_hierarchy_context(&queue.db_pool, episode).await;
        let season_number = hierarchy
            .season_number
            .unwrap_or_else(|| episode.season_number.unwrap_or(1));
        let episode_number = episode.episode_number.unwrap_or(1);
        let show_name = pretty_show_name(&hierarchy, &episode.title);
        let metadata = metadata_from_show_context(&hierarchy);
        let filesystem_settings = queue.filesystem_settings.read().await;
        let library_profiles = filesystem_settings
            .matching_profile_keys(&metadata, FilesystemContentType::Show)
            .into_json();
        drop(filesystem_settings);

        let parsed = parse_file_path(&file.filename);
        let path = episode_vfs_path(&show_name, season_number, episode_number, parsed.part, None);
        repo::create_media_entry(
            &queue.db_pool,
            episode.id,
            &path,
            file.file_size as i64,
            &file.filename,
            file.download_url.as_deref(),
            file.stream_url
                .as_deref()
                .filter(|value| !value.starts_with("matched:")),
            &download.plugin_name,
            download.provider.as_deref(),
            Some(stream.id),
            None,
            None,
            Some(&library_profiles),
        )
        .await?;

        completed_episode_ids.push(episode.id);
        if let Some(season_id) = episode.parent_id {
            season_ids.push(season_id);
        }
    }

    if completed_episode_ids.is_empty() {
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id: item.id })
            .await;
        anyhow::bail!("no episode files were persisted from the torrent");
    }

    repo::batch_set_completed(&queue.db_pool, &completed_episode_ids).await?;
    season_ids.sort_unstable();
    season_ids.dedup();
    for season_id in season_ids {
        if let Some(season) = repo::get_media_item(&queue.db_pool, season_id).await?
            && let Err(err) = repo::refresh_state(&queue.db_pool, &season).await
        {
            tracing::warn!(season_id, %err, "failed to refresh season state");
        }
    }
    if let Err(err) = repo::refresh_state(&queue.db_pool, item).await {
        tracing::warn!(id = item.id, %err, "failed to refresh item state");
    }
    LibraryOrchestrator::new(queue)
        .sync_item_request_state(item)
        .await;

    let completed = matches!(
        repo::compute_state(&queue.db_pool, item).await,
        Ok(MediaItemState::Completed)
    );

    if completed {
        finalize_download_success(
            item.id,
            item,
            queue,
            start_time,
            download.provider.clone(),
            Some(download.plugin_name.clone()),
        )
        .await;
    } else {
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id: item.id })
            .await;
    }

    Ok(())
}

pub async fn finalize_download_success(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    provider: Option<String>,
    plugin_name: Option<String>,
) {
    if let Err(error) = repo::refresh_state_cascade(&queue.db_pool, item).await {
        tracing::error!(error = %error, "failed to refresh state after download");
    }
    LibraryOrchestrator::new(queue)
        .sync_item_request_state(item)
        .await;

    let duration = start_time.elapsed();
    queue
        .notify(RivenEvent::MediaItemDownloadSuccess {
            id,
            title: item.title.clone(),
            full_title: item.full_title.clone(),
            item_type: item.item_type,
            year: item.year,
            imdb_id: item.imdb_id.clone(),
            tmdb_id: item.tmdb_id.clone(),
            poster_path: item.poster_path.clone(),
            plugin_name: plugin_name.unwrap_or_default(),
            provider,
            duration_seconds: duration.as_secs_f64(),
        })
        .await;
    tracing::debug!(
        id,
        duration_secs = duration.as_secs_f64(),
        "download flow completed"
    );
}
