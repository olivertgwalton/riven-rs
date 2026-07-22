use super::*;

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
                Some(stream_resolution(stream)),
                None,
                None,
                true,
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
            let hierarchy = load_download_hierarchy_context(item).await;
            if !persist_episode(
                item,
                &download,
                &info_hash,
                queue,
                &hierarchy,
                Some(stream.id),
                stream_raw_title(stream),
                Some(stream_resolution(stream)),
                None,
                None,
                true,
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
            let hierarchy = load_download_hierarchy_context(item).await;
            match persist_season(
                item,
                download,
                &info_hash,
                queue,
                &hierarchy,
                start_time,
                Some(stream.id),
                stream_raw_title(stream),
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
    let episodes = repo::list_media_items_by_ids(&episode_ids).await?;
    let episode_map: std::collections::HashMap<i64, MediaItem> = episodes
        .into_iter()
        .map(|episode| (episode.id, episode))
        .collect();

    let mut completed_episode_ids: Vec<i64> = Vec::new();

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

        if !has_playable_url(file) {
            tracing::warn!(
                ep_id = episode.id,
                filename = %file.filename,
                "supplied-show file has no playable URL; skipping"
            );
            continue;
        }

        let hierarchy = load_download_hierarchy_context(episode).await;
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
        match repo::create_media_entry(repo::MediaEntryInput {
            media_item_id: episode.id,
            path: &path,
            file_size: file.file_size as i64,
            original_filename: &file.filename,
            download_url: file.download_url.as_deref(),
            stream_url: file
                .stream_url
                .as_deref()
                .filter(|value| !value.starts_with("matched:")),
            plugin: &download.plugin_name,
            provider: download.provider.as_deref(),
            stream_id: Some(stream.id),
            resolution: None,
            ranking_profile_name: None,
            library_profiles: Some(&library_profiles),
            usenet_info_hash: file.usenet_info_hash.as_deref(),
            usenet_file_index: file.usenet_file_index,
        })
        .await
        {
            Ok(_) => {}
            Err(e) if is_item_deleted_fk_error(&e) => {
                tracing::debug!(
                    ep_id = episode.id,
                    "episode was deleted mid-persist, skipping"
                );
                continue;
            }
            Err(e) => return Err(e),
        }

        completed_episode_ids.push(episode.id);
    }

    if completed_episode_ids.is_empty() {
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id: item.id })
            .await;
        anyhow::bail!("no episode files were persisted from the torrent");
    }

    sync_item_request_state(item).await;

    let completed = repo::get_media_item(item.id)
        .await
        .ok()
        .flatten()
        .is_some_and(|i| i.state == MediaItemState::Completed);

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
