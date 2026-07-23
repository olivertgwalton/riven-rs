use super::*;

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
    raw_title: &str,
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

    let show = match repo::get_media_item(show_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            tracing::debug!(
                id,
                show_id,
                "parent show disappeared mid-flight; aborting season persist"
            );
            return SeasonPersistOutcome::Failed;
        }
        Err(error) => {
            tracing::error!(id, show_id, %error, "failed to load parent show");
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: format!("failed to load parent show: {error}"),
                })
                .await;
            return SeasonPersistOutcome::Failed;
        }
    };

    let episodes = match repo::list_episodes(id).await {
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

    let parsed_video_files: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .filter(|f| is_persistable_video_file(&f.filename))
        .filter(|f| has_playable_url(f))
        .map(|f| (f, parse_file_path(&f.filename)))
        .collect();

    let mut episode_matches: Vec<(&MediaItem, Vec<(&DownloadFile, riven_rank::ParsedData)>)> =
        Vec::with_capacity(episodes.len());
    for ep in &episodes {
        let episode_number = ep.episode_number.unwrap_or(1);
        let matched: Vec<(&DownloadFile, riven_rank::ParsedData)> = parsed_video_files
            .iter()
            .filter(|(_, p)| {
                matches_episode_lookup(p, season_number, episode_number, ep.absolute_number)
            })
            .map(|(f, p)| (*f, p.clone()))
            .collect();
        episode_matches.push((ep, matched));
    }

    let events = super::super::event_match::episode_events(&episodes);
    if !events.is_empty() {
        let single_playable = parsed_video_files.len() == 1;
        let already_matched: std::collections::HashSet<&str> = episode_matches
            .iter()
            .flat_map(|(_, m)| m.iter().map(|(f, _)| f.filename.as_str()))
            .collect();
        for (file, parsed) in &parsed_video_files {
            if already_matched.contains(file.filename.as_str()) {
                continue;
            }
            let candidate = if looks_obfuscated(&file.filename) {
                if single_playable && !raw_title.is_empty() {
                    raw_title
                } else {
                    continue;
                }
            } else {
                file.filename.as_str()
            };
            if let Some(idx) =
                super::super::event_match::match_release_to_episode(candidate, &events)
            {
                let ep_id = episodes[idx].id;
                tracing::debug!(
                    id, season = season_number, info_hash = %info_hash,
                    episode = episodes[idx].episode_number,
                    episode_title = %episodes[idx].title,
                    candidate = %candidate,
                    "event-matched release to episode"
                );
                if let Some((_, m)) = episode_matches.iter_mut().find(|(ep, _)| ep.id == ep_id) {
                    m.push((*file, parsed.clone()));
                }
            }
        }
    }

    // Obfuscated-season-pack fallback: when no inner filename parses to a
    // recognisable S/E *and* the pack is exactly the right shape (one playable
    // video file per episode, all obfuscated), assign files to episodes in
    // NZB/sort order.
    //
    // Constraints kept tight on purpose: a partial match (e.g. 22 of 23
    // episodes match normally, 1 obfuscated file left over) is *not* covered —
    // that's safer left to the per-episode cascade than to a brittle
    // index-based guess. The release-title S/E vetting that ranked this pack
    // is what justifies trusting the order here.
    let no_normal_matches = episode_matches.iter().all(|(_, m)| m.is_empty());
    if no_normal_matches
        && !episodes.is_empty()
        && parsed_video_files.len() == episodes.len()
        && parsed_video_files
            .iter()
            .all(|(f, _)| looks_obfuscated(&f.filename))
    {
        let mut ordered: Vec<&(&DownloadFile, riven_rank::ParsedData)> =
            parsed_video_files.iter().collect();
        ordered.sort_by(|a, b| a.0.filename.cmp(&b.0.filename));
        let mut by_ep: Vec<(&MediaItem, Vec<(&DownloadFile, riven_rank::ParsedData)>)> =
            Vec::with_capacity(episodes.len());
        let mut episodes_sorted = episodes.iter().collect::<Vec<_>>();
        episodes_sorted.sort_by_key(|e| e.episode_number.unwrap_or(0));
        for (idx, ep) in episodes_sorted.iter().enumerate() {
            by_ep.push((
                *ep,
                vec![(ordered[idx].0, riven_rank::ParsedData::default())],
            ));
        }
        let example = ordered.first().map_or("", |(f, _)| f.filename.as_str());
        tracing::debug!(
            id, season = season_number, info_hash = %info_hash,
            title = %item.title,
            file_count = parsed_video_files.len(),
            episode_count = episodes.len(),
            example_filename = %example,
            "obfuscated season pack matched 1:1 to episodes by sort order"
        );
        episode_matches = by_ep;
    }

    let mut completed_episode_ids: Vec<i64> = Vec::new();

    for (ep, matched) in &episode_matches {
        let episode_number = ep.episode_number.unwrap_or(1);
        for (file, part) in select_episode_files(matched) {
            let path = episode_vfs_path(&show_name, season_number, episode_number, part, path_tag);
            match repo::create_media_entry(repo::MediaEntryInput {
                media_item_id: ep.id,
                path: &path,
                file_size: file.file_size as i64,
                original_filename: &file.filename,
                download_url: file.download_url.as_deref(),
                stream_url: file.stream_url.as_deref(),
                plugin: &dl.plugin_name,
                provider: dl.provider.as_deref(),
                stream_id,
                resolution: None,
                ranking_profile_name: profile_name,
                library_profiles: Some(&library_profiles_json),
                usenet_info_hash: file.usenet_info_hash.as_deref(),
                usenet_file_index: file.usenet_file_index,
            })
            .await
            {
                Ok(_) => {
                    completed_episode_ids.push(ep.id);
                }
                Err(e) => {
                    if is_item_deleted_fk_error(&e) {
                        tracing::debug!(ep_id = ep.id, "episode was deleted mid-persist, skipping");
                    } else {
                        tracing::error!(error = %e, ep_id = ep.id, "failed to create media entry for episode");
                    }
                }
            }
        }
    }

    if completed_episode_ids.is_empty() {
        tracing::warn!(
            id, season = season_number, info_hash = %info_hash,
            title = %item.title,
            "season pack matched episodes but no entries were persisted — blacklisting stream"
        );
        blacklist_stream(id, info_hash, &item.title).await;
        return SeasonPersistOutcome::Failed;
    }

    // The filesystem_entries inserts above already recomputed state for each
    // episode (via the repo layer), and the recompute cascade walked them up to
    // the season and show. Just sync the request state and read the now-current
    // season state.
    sync_item_request_state(item).await;

    let season_complete = repo::get_media_item(item.id)
        .await
        .ok()
        .flatten()
        .is_some_and(|i| i.state == MediaItemState::Completed);

    if !season_complete {
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        return SeasonPersistOutcome::Partial;
    }

    queue
        .filesystem_settings_revision
        .fetch_add(1, Ordering::SeqCst);

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

/// Persist a multi-season pack against a show.
///
/// Generalises [`persist_season`] across every requested (non-special) season:
/// each playable file in the download is matched to an episode by normal S/E
/// numbering and the matching episodes are filled. Multi-season packs are
/// always labelled `SxxExx`, so the obfuscated index-order fallback used by
/// `persist_season` is intentionally not applied here. Emits the success event
/// itself when at least one episode is persisted, so the caller should return
/// immediately after.
pub async fn persist_show(
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

    let seasons = match repo::list_seasons_excluding_specials(id).await {
        Ok(seasons) => seasons,
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

    let show_name = item.pretty_name();
    let metadata = metadata_from_show_context(hierarchy);
    let filesystem_settings = queue.filesystem_settings.read().await;
    let library_profiles =
        filesystem_settings.matching_profile_keys(&metadata, FilesystemContentType::Show);
    let library_profiles_json = library_profiles.into_json();
    drop(filesystem_settings);

    let parsed_video_files: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .filter(|f| is_persistable_video_file(&f.filename))
        .filter(|f| has_playable_url(f))
        .map(|f| (f, parse_file_path(&f.filename)))
        .collect();

    let mut completed_episode_ids: Vec<i64> = Vec::new();

    for season in &seasons {
        let season_number = season.season_number.unwrap_or(1);
        let episodes = match repo::list_episodes(season.id).await {
            Ok(eps) => eps,
            Err(e) => {
                tracing::error!(id, season_id = season.id, error = %e, "failed to load episodes for season");
                continue;
            }
        };

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
                let path =
                    episode_vfs_path(&show_name, season_number, episode_number, part, path_tag);
                match repo::create_media_entry(repo::MediaEntryInput {
                    media_item_id: ep.id,
                    path: &path,
                    file_size: file.file_size as i64,
                    original_filename: &file.filename,
                    download_url: file.download_url.as_deref(),
                    stream_url: file.stream_url.as_deref(),
                    plugin: &dl.plugin_name,
                    provider: dl.provider.as_deref(),
                    stream_id,
                    resolution: None,
                    ranking_profile_name: profile_name,
                    library_profiles: Some(&library_profiles_json),
                    usenet_info_hash: file.usenet_info_hash.as_deref(),
                    usenet_file_index: file.usenet_file_index,
                })
                .await
                {
                    Ok(_) => completed_episode_ids.push(ep.id),
                    Err(e) => {
                        if is_item_deleted_fk_error(&e) {
                            tracing::debug!(
                                ep_id = ep.id,
                                "episode was deleted mid-persist, skipping"
                            );
                        } else {
                            tracing::error!(error = %e, ep_id = ep.id, "failed to create media entry for episode");
                        }
                    }
                }
            }
        }
    }

    if completed_episode_ids.is_empty() {
        tracing::warn!(
            id, info_hash = %info_hash,
            title = %item.title,
            "show pack matched no episodes — blacklisting stream"
        );
        blacklist_stream(id, info_hash, &item.title).await;
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        return SeasonPersistOutcome::Failed;
    }

    sync_item_request_state(item).await;
    queue
        .filesystem_settings_revision
        .fetch_add(1, Ordering::SeqCst);

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
            plugin_name: dl.plugin_name,
            provider: dl.provider,
            duration_seconds: duration.as_secs_f64(),
        })
        .await;
    tracing::info!(
        id,
        episodes = completed_episode_ids.len(),
        duration_secs = duration.as_secs_f64(),
        "show pack download flow completed"
    );
    SeasonPersistOutcome::Complete
}
