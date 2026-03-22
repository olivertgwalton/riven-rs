use riven_core::events::{HookResponse, RivenEvent};
use riven_core::plugin::PluginRegistry;
use riven_core::types::*;
use riven_db::repo;
use tokio::sync::mpsc;

/// Run the index item flow.
/// Dispatches to indexer plugins (TMDB, TVDB), merges results, and persists.
pub async fn run(
    id: i64,
    registry: &PluginRegistry,
    db_pool: &sqlx::PgPool,
    event_tx: &mpsc::Sender<RivenEvent>,
) {
    let item = match repo::get_media_item(db_pool, id).await {
        Ok(Some(item)) => item,
        Ok(None) => {
            tracing::error!(id, "media item not found for indexing");
            return;
        }
        Err(e) => {
            tracing::error!(id, error = %e, "failed to fetch media item for indexing");
            return;
        }
    };

    let event = RivenEvent::MediaItemIndexRequested {
        id: item.id,
        item_type: item.item_type,
        imdb_id: item.imdb_id.clone(),
        tvdb_id: item.tvdb_id.clone(),
        tmdb_id: item.tmdb_id.clone(),
    };

    let results = registry.dispatch(&event).await;

    let mut merged = IndexedMediaItem::default();
    let mut got_response = false;

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::Index(indexed)) => {
                tracing::info!(plugin = plugin_name, id, "indexer responded");
                got_response = true;
                merged = merged.merge(indexed);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(plugin = plugin_name, id, error = %e, "indexer hook failed");
            }
        }
    }

    if !got_response {
        let _ = event_tx
            .send(RivenEvent::MediaItemIndexError {
                id,
                error: "no indexer plugin responded".into(),
            })
            .await;
        return;
    }

    // Persist indexed data
    if let Err(e) = repo::update_media_item_index(db_pool, id, &merged).await {
        let _ = event_tx
            .send(RivenEvent::MediaItemIndexError {
                id,
                error: e.to_string(),
            })
            .await;
        return;
    }

    // For shows, create seasons and episodes
    if item.item_type == MediaItemType::Show {
        if let Some(seasons) = &merged.seasons {
            // Gap 1: season filtering — only mark seasons/episodes as requested when
            // the item request explicitly lists them. An absent seasons list means
            // "all seasons requested".
            let requested_seasons: Option<Vec<i32>> = if let Some(req_id) = item.item_request_id {
                repo::get_item_request_by_id(db_pool, req_id)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|req| req.seasons)
                    .and_then(|s| serde_json::from_value(s).ok())
            } else {
                None
            };

            for season_data in seasons {
                // Season 0 (specials) is never marked as requested unless explicitly asked.
                let season_requested = if season_data.number == 0 {
                    requested_seasons
                        .as_ref()
                        .map(|s| s.contains(&0))
                        .unwrap_or(false)
                } else {
                    requested_seasons
                        .as_ref()
                        .map(|s| s.contains(&season_data.number))
                        .unwrap_or(true) // no filter → all requested
                };

                let season = match repo::create_season(
                    db_pool,
                    id,
                    season_data.number,
                    season_data.title.as_deref(),
                    season_data.tvdb_id.as_deref(),
                    season_data.number == 0,
                    item.item_request_id,
                    season_requested,
                )
                .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, season = season_data.number, "failed to create season");
                        continue;
                    }
                };

                for ep_data in &season_data.episodes {
                    if let Err(e) = repo::create_episode(
                        db_pool,
                        season.id,
                        ep_data.number,
                        ep_data.title.as_deref(),
                        ep_data.tvdb_id.as_deref(),
                        ep_data.aired_at,
                        ep_data.runtime,
                        ep_data.absolute_number,
                        item.item_request_id,
                        season_requested,
                        Some(season_data.number),
                    )
                    .await
                    {
                        tracing::error!(error = %e, episode = ep_data.number, "failed to create episode");
                    }
                }

                // After creating all episodes, cascade their states up through the
                // season so that fully-unreleased seasons get state = Unreleased
                // rather than staying Indexed.
                if let Ok(season_state) = repo::compute_state(db_pool, &season).await {
                    if season_state != season.state {
                        let _ = repo::update_media_item_state(db_pool, season.id, season_state).await;
                    }
                }
            }
        }

        // Cascade season states up to the show.
        if let Ok(Some(show_item)) = repo::get_media_item(db_pool, id).await {
            if let Ok(show_state) = repo::compute_state(db_pool, &show_item).await {
                if show_state != show_item.state {
                    let _ = repo::update_media_item_state(db_pool, id, show_state).await;
                }
            }
        }
    }

    let title = merged.title.clone().unwrap_or_else(|| item.title.clone());
    let _ = event_tx
        .send(RivenEvent::MediaItemIndexSuccess { id, title, item_type: item.item_type })
        .await;
    tracing::info!(id, "index flow completed");
}
