use std::collections::HashMap;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::plugin::PluginRegistry;
use riven_core::types::*;
use riven_db::repo;
use riven_rank::RankSettings;
use tokio::sync::mpsc;

/// Load rank settings from the database, falling back to defaults.
async fn load_rank_settings(db_pool: &sqlx::PgPool) -> RankSettings {
    match repo::get_setting(db_pool, "rank_settings").await {
        Ok(Some(value)) => serde_json::from_value(value).unwrap_or_default(),
        _ => RankSettings::default(),
    }
}

/// Run the scrape item flow.
/// Dispatches to scraper plugins, aggregates streams, ranks them, and persists.
pub async fn run(
    id: i64,
    event: &RivenEvent,
    registry: &PluginRegistry,
    db_pool: &sqlx::PgPool,
    event_tx: &mpsc::Sender<RivenEvent>,
) {
    tracing::info!(id, "running scrape flow");

    // Load the item once — used for the skip guard, title/aliases for ranking,
    // and enriching emitted events.
    let item = match repo::get_media_item(db_pool, id).await {
        Ok(Some(item)) => item,
        Ok(None) => {
            tracing::error!(id, "media item not found for scrape");
            return;
        }
        Err(e) => {
            tracing::error!(id, error = %e, "failed to load media item for scrape");
            return;
        }
    };

    if matches!(
        item.state,
        MediaItemState::Completed | MediaItemState::Ongoing | MediaItemState::Unreleased
    ) {
        tracing::debug!(id, state = ?item.state, "skipping scrape");
        return;
    }

    let results = registry.dispatch(event).await;

    let mut all_streams: HashMap<String, String> = HashMap::new();

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::Scrape(streams)) => {
                tracing::info!(
                    plugin = plugin_name,
                    count = streams.len(),
                    "scraper responded"
                );
                all_streams.extend(streams);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(plugin = plugin_name, error = %e, "scraper hook failed");
            }
        }
    }

    if all_streams.is_empty() {
        let _ = event_tx
            .send(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item.title.clone(),
                item_type: item.item_type,
            })
            .await;
        return;
    }

    let correct_title = item.title.clone();
    let item_title = item.title.clone();
    let item_type = item.item_type;
    let aliases: HashMap<String, Vec<String>> = item
        .aliases
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    // Load rank settings from database
    let rank_settings = load_rank_settings(db_pool).await;

    // Parse, rank, and persist streams
    let mut stream_count = 0;
    for (info_hash, title) in &all_streams {
        let (parsed_data, rank) = match riven_rank::rank_torrent(
            title,
            info_hash,
            &correct_title,
            &aliases,
            &rank_settings,
        ) {
            Ok(ranked) => {
                let parsed_data = serde_json::to_value(&ranked.data).ok();
                (parsed_data, Some(ranked.rank))
            }
            Err(e) => {
                tracing::debug!(
                    info_hash,
                    title,
                    error = %e,
                    "stream rejected by ranking"
                );
                // Still parse and store, but with no rank (won't be selected for download)
                let parsed_data = serde_json::to_value(riven_rank::parse(title)).ok();
                (parsed_data, None)
            }
        };

        match repo::upsert_stream(db_pool, info_hash, parsed_data, rank).await {
            Ok(stream) => {
                if let Err(e) = repo::link_stream_to_item(db_pool, id, stream.id).await {
                    tracing::error!(error = %e, "failed to link stream to item");
                }
                stream_count += 1;
            }
            Err(e) => {
                tracing::error!(error = %e, info_hash, "failed to upsert stream");
            }
        }
    }

    // Update item state
    if let Err(e) = repo::update_scraped(db_pool, id).await {
        tracing::error!(error = %e, "failed to update scraped timestamp");
    }

    if let Err(e) = repo::update_media_item_state(db_pool, id, MediaItemState::Scraped).await {
        tracing::error!(error = %e, "failed to update media item state");
    }

    let _ = event_tx
        .send(RivenEvent::MediaItemScrapeSuccess {
            id,
            title: item_title,
            item_type,
            stream_count,
        })
        .await;

    tracing::info!(id, stream_count, "scrape flow completed");
}
