use std::collections::HashMap;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;
use riven_rank::{ParsedData, RankSettings};

use crate::{JobQueue, ScrapeJob};
use super::load_item_or_log;

/// Validate that a parsed torrent is a plausible match for the given item.
/// Returns `Some(reason)` if the torrent should be skipped, `None` if it passes.
fn validate_parsed_data(item: &MediaItem, parsed: &ParsedData) -> Option<String> {
    let has_episodes = !parsed.episodes.is_empty();
    let has_seasons = !parsed.seasons.is_empty();

    match item.item_type {
        MediaItemType::Movie => {
            // Movies can be part of collections (S##-S##) or short-packs,
            // so we don't strictly reject if seasons/episodes are present.
            // Similarity and rank will handle the match.
        }
        MediaItemType::Episode => {
            if !has_episodes && !has_seasons {
                return Some("torrent has no season or episode info for episode item".into());
            }

            let ep_num = item.episode_number.unwrap_or(0);
            let abs_num = item.absolute_number;

            if has_episodes {
                let matches_relative = parsed.episodes.contains(&ep_num);
                let matches_absolute =
                    abs_num.map(|a| parsed.episodes.contains(&a)).unwrap_or(false);
                if !matches_relative && !matches_absolute {
                    return Some(format!(
                        "torrent episodes {:?} don't include episode {} (abs {:?})",
                        parsed.episodes, ep_num, abs_num
                    ));
                }
            }

            if has_seasons {
                let season_num = item.season_number.unwrap_or(0);
                if !parsed.seasons.contains(&season_num) {
                    return Some(format!(
                        "torrent seasons {:?} don't include season {}",
                        parsed.seasons, season_num
                    ));
                }
            }
        }
        MediaItemType::Season => {
            if !has_episodes && !has_seasons {
                return Some("torrent has no season or episode info for season item".into());
            }

            if has_seasons {
                let season_num = item.season_number.unwrap_or(1);
                if !parsed.seasons.contains(&season_num) {
                    return Some(format!(
                        "torrent seasons {:?} don't include season {}",
                        parsed.seasons, season_num
                    ));
                }
            }
        }
        MediaItemType::Show => {
            if !has_episodes && !has_seasons {
                return Some("torrent has no season or episode info for show item".into());
            }
        }
    }

    None
}

async fn load_rank_settings(db_pool: &sqlx::PgPool) -> RankSettings {
    match repo::get_setting(db_pool, "rank_settings").await {
        Ok(Some(value)) => serde_json::from_value(value).unwrap_or_default(),
        _ => RankSettings::default(),
    }
}

/// Run the scrape item flow.
/// Dispatches to scraper plugins, aggregates streams, ranks them, and persists.
pub async fn run(id: i64, job: &ScrapeJob, queue: &JobQueue) {
    tracing::debug!(id, "running scrape flow");
    let Some(item) = load_item_or_log(id, &queue.db_pool, "scrape").await else {
        return;
    };

    if matches!(item.state, MediaItemState::Completed | MediaItemState::Unreleased) {
        tracing::debug!(id, state = ?item.state, "skipping scrape");
        return;
    }

    let _ = repo::clear_blacklisted_streams(&queue.db_pool, id).await;

    let event = RivenEvent::MediaItemScrapeRequested {
        id,
        item_type: job.item_type,
        imdb_id: job.imdb_id.clone(),
        title: job.title.clone(),
        season: job.season,
        episode: job.episode,
    };


    let results = queue.registry.dispatch(&event).await;

    let mut all_streams: HashMap<String, String> = HashMap::new();

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::Scrape(streams)) => {
                tracing::debug!(
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

    let mut correct_title = item.title.clone();
    let mut aliases: HashMap<String, Vec<String>> = item
        .aliases
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    let mut show_title = None;

    if matches!(item.item_type, MediaItemType::Season | MediaItemType::Episode) {
        let mut current = item.clone();
        while let Some(parent_id) = current.parent_id {
            if let Ok(Some(parent)) = repo::get_media_item(&queue.db_pool, parent_id).await {
                current = parent;
                if current.item_type == MediaItemType::Show {
                    correct_title = current.title.clone();
                    show_title = Some(current.title.clone());
                    aliases = current
                        .aliases
                        .as_ref()
                        .and_then(|v| serde_json::from_value(v.clone()).ok())
                        .unwrap_or_default();
                    break;
                }
            } else {
                break;
            }
        }
    }

    if all_streams.is_empty() {
        tracing::info!(id, "no streams found by any scraper");
    }

    let item_type = item.item_type;

    let item_title = if item.item_type == MediaItemType::Season {
        if let Some(show_t) = show_title {
            format!("{} - {}", show_t, item.title)
        } else {
            item.title.clone()
        }
    } else {
        item.title.clone()
    };

    let rank_settings = load_rank_settings(&queue.db_pool).await;

    let mut stream_count = 0;
    for (info_hash, title) in &all_streams {
        let base_parsed = riven_rank::parse(title);

        if let Some(reason) = validate_parsed_data(&item, &base_parsed) {
            tracing::debug!(info_hash, title, reason, "torrent skipped by item validation");
            continue;
        }

        let (parsed_data, rank) = match riven_rank::rank_torrent(
            title,
            info_hash,
            &correct_title,
            &aliases,
            &rank_settings,
        ) {
            Ok(ranked) => {
                let parsed_data = serde_json::to_value(&ranked.data).ok();
                let rank = ranked.rank;
                let bitrate = ranked.data.bitrate.as_deref().unwrap_or("unknown");
                tracing::info!(id, info_hash, rank, bitrate, title, "stream ranked and linked");
                (parsed_data, Some(rank))
            }
            Err(e) => {
                tracing::debug!(
                    info_hash,
                    title,
                    error = %e,
                    "stream rejected by ranking — not linked to item"
                );
                let parsed_data = serde_json::to_value(&base_parsed).ok();
                let _ = repo::upsert_stream(&queue.db_pool, info_hash, parsed_data, None).await;
                continue;
            }
        };

        let stream = match repo::upsert_stream(&queue.db_pool, info_hash, parsed_data, rank).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, info_hash, "failed to upsert stream");
                continue;
            }
        };

        if let Err(e) = repo::link_stream_to_item(&queue.db_pool, id, stream.id).await {
            tracing::error!(error = %e, "failed to link stream to item");
        }
        stream_count += 1;
    }

    if let Err(e) = repo::update_scraped(&queue.db_pool, id).await {
        tracing::error!(error = %e, "failed to update scraped timestamp");
    }

    if let Err(e) =
        repo::update_media_item_state(&queue.db_pool, id, MediaItemState::Scraped).await
    {
        tracing::error!(error = %e, "failed to update media item state");
    }

    if all_streams.is_empty() {
        let _ = repo::increment_failed_attempts(&queue.db_pool, id).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type,
            })
            .await;
        queue.fan_out_download(id).await;
    } else {
        queue
            .notify(RivenEvent::MediaItemScrapeSuccess {
                id,
                title: item_title,
                item_type,
                stream_count,
            })
            .await;
    }

    tracing::info!(id, stream_count, "scrape flow completed");

    // Immediately queue download after successful scraping.
    queue.push_download_from_best_stream(id).await;
}
