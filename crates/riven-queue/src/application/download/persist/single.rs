use super::*;

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
    skip_bitrate_check: bool,
) -> bool {
    let id = item.id;

    tracing::debug!(id, info_hash, files = dl.files.len(), "persisting movie");

    let mut video_files: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .filter(|f| is_persistable_video_file(&f.filename))
        .map(|f| (f, parse_file_path(&f.filename)))
        .filter(|(_, parsed)| parsed.media_type() == "movie")
        .collect();

    video_files.sort_by_key(|(f, _)| std::cmp::Reverse(f.file_size));

    let file = if let Some(first) = video_files.first() {
        first.0
    } else if let Some(largest) = dl
        .files
        .iter()
        .filter(|f| is_persistable_video_file(&f.filename))
        .max_by_key(|f| f.file_size)
    {
        tracing::warn!(
            id,
            info_hash,
            "no movie-typed video file found; falling back to largest video file"
        );
        largest
    } else {
        // Blacklist only — this is one candidate among possibly many the
        // outer stream loop still has left to try; a per-candidate notify
        // here would fire `MediaItemDownloadPartialSuccess` once per dead
        // candidate, each independently pushing the item to `Validate` and
        // scheduling its own 30-minute re-scrape. The loop's single
        // exhausted-all-candidates check is the only place that should
        // notify for this attempt.
        tracing::warn!(id, info_hash = %info_hash, "torrent has no files — blacklisting stream");
        blacklist_stream(id, info_hash).await;
        return false;
    };

    let config = queue.downloader_config.read().await;
    if !skip_bitrate_check && !config.movie_passes(file.file_size, item.runtime) {
        drop(config);
        handle_bitrate_failure(id, info_hash, file.file_size, item.runtime, "movie").await;
        return false;
    }
    drop(config);

    if !has_playable_url(file) {
        // See the "torrent has no files" branch above: blacklist and let
        // the outer stream loop try the next candidate without firing a
        // per-candidate notify.
        tracing::warn!(
            id, info_hash = %info_hash, filename = %file.filename,
            "matched movie file has no playable URL — blacklisting stream"
        );
        blacklist_stream(id, info_hash).await;
        return false;
    }

    let ext = file
        .filename
        .rfind('.')
        .filter(|&i| i > 0)
        .map_or("mkv", |i| &file.filename[i + 1..]);
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

    if let Err(e) = repo::create_media_entry(repo::MediaEntryInput {
        media_item_id: id,
        path: &path,
        file_size: file.file_size as i64,
        original_filename: &file.filename,
        download_url: file.download_url.as_deref(),
        stream_url: file.stream_url.as_deref(),
        plugin: &dl.plugin_name,
        provider: dl.provider.as_deref(),
        stream_id,
        resolution,
        ranking_profile_name: profile_name,
        library_profiles: Some(&library_profiles_json),
        usenet_info_hash: file.usenet_info_hash.as_deref(),
        usenet_file_index: file.usenet_file_index,
    })
    .await
    {
        if is_item_deleted_fk_error(&e) {
            tracing::debug!(id, "movie was deleted mid-persist, skipping");
            return false;
        }
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
    raw_title: &str,
    resolution: Option<&str>,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
    skip_bitrate_check: bool,
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

    let playable_videos: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .filter(|f| is_persistable_video_file(&f.filename))
        .filter(|f| has_playable_url(f))
        .map(|f| (f, parse_file_path(&f.filename)))
        .collect();

    let mut matched: Vec<(&DownloadFile, riven_rank::ParsedData)> = playable_videos
        .iter()
        .filter(|(_, p)| {
            matches_episode_lookup(p, season_number, episode_number, item.absolute_number)
        })
        .map(|(f, p)| (*f, p.clone()))
        .collect();

    if matched.is_empty() && super::super::event_match::parse_episode_event(&item.title).is_some() {
        let single_playable = playable_videos.len() == 1;
        for (file, parsed) in &playable_videos {
            let candidate = if looks_obfuscated(&file.filename) {
                if single_playable && !raw_title.is_empty() {
                    raw_title
                } else {
                    continue;
                }
            } else {
                file.filename.as_str()
            };
            if super::super::event_match::release_matches_episode(
                candidate,
                &item.title,
                item.aired_at,
            ) {
                tracing::debug!(
                    id, season = season_number, episode = episode_number,
                    info_hash = %info_hash,
                    episode_title = %item.title,
                    candidate = %candidate,
                    "event-matched release to episode"
                );
                matched.push((*file, parsed.clone()));
            }
        }
    }

    if matched.is_empty()
        && playable_videos.len() == 1
        && looks_obfuscated(&playable_videos[0].0.filename)
    {
        tracing::debug!(
            id, season = season_number, episode = episode_number,
            info_hash = %info_hash,
            filename = %playable_videos[0].0.filename,
            "obfuscated single-file NZB; accepting based on stream release title"
        );
        let (file, _) = &playable_videos[0];
        matched.push((*file, riven_rank::ParsedData::default()));
    }

    if matched.is_empty() {
        // Blacklist only — see the equivalent branch in `persist_movie` for
        // why this must not notify per candidate.
        tracing::warn!(
            id, season = season_number, episode = episode_number,
            info_hash = %info_hash,
            "no playable torrent file matched episode — blacklisting stream"
        );
        blacklist_stream(id, info_hash).await;
        return false;
    }

    let largest = matched.iter().max_by_key(|(f, _)| f.file_size).unwrap().0;
    let config = queue.downloader_config.read().await;
    if !skip_bitrate_check && !config.episode_passes(largest.file_size, item.runtime) {
        drop(config);
        handle_bitrate_failure(id, info_hash, largest.file_size, item.runtime, "episode").await;
        return false;
    }
    drop(config);

    let show_name = pretty_show_name(hierarchy, &item.title);
    for (file, part) in select_episode_files(&matched) {
        let path = episode_vfs_path(&show_name, season_number, episode_number, part, path_tag);
        if let Err(e) = repo::create_media_entry(repo::MediaEntryInput {
            media_item_id: id,
            path: &path,
            file_size: file.file_size as i64,
            original_filename: &file.filename,
            download_url: file.download_url.as_deref(),
            stream_url: file.stream_url.as_deref(),
            plugin: &dl.plugin_name,
            provider: dl.provider.as_deref(),
            stream_id,
            resolution,
            ranking_profile_name: profile_name,
            library_profiles: Some(&library_profiles_json),
            usenet_info_hash: file.usenet_info_hash.as_deref(),
            usenet_file_index: file.usenet_file_index,
        })
        .await
        {
            if is_item_deleted_fk_error(&e) {
                tracing::debug!(id, "episode was deleted mid-persist, skipping");
                return false;
            }
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
