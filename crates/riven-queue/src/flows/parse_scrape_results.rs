use std::collections::HashMap;

use futures::stream::{self, StreamExt};

use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::repo;

use super::load_item_or_log;
use crate::discovery::{
    load_active_profiles, load_dubbed_anime_only, load_fallback_rank_settings, rank_streams,
    ParseContext,
};
use crate::{JobQueue, ParseScrapeResultsJob};

/// Resolve the show title, year, and aliases for Season/Episode items by
/// walking up the parent chain. Returns (correct_title, parent_year, aliases,
/// show_title_for_format).
async fn resolve_parent_info(
    db_pool: &sqlx::PgPool,
    item: &riven_db::entities::MediaItem,
) -> (
    String,
    Option<i32>,
    HashMap<String, Vec<String>>,
    Option<String>,
) {
    let initial_aliases: HashMap<String, Vec<String>> = item
        .aliases
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    if !matches!(
        item.item_type,
        MediaItemType::Season | MediaItemType::Episode
    ) {
        return (item.title.clone(), None, initial_aliases, None);
    }

    let mut current = item.clone();
    loop {
        let Some(parent_id) = current.parent_id else {
            break;
        };
        let Ok(Some(parent)) = repo::get_media_item(db_pool, parent_id).await else {
            break;
        };
        current = parent;
        if current.item_type == MediaItemType::Show {
            let aliases = current
                .aliases
                .as_ref()
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            return (
                current.title.clone(),
                current.year,
                aliases,
                Some(current.title.clone()),
            );
        }
    }

    (item.title.clone(), None, initial_aliases, None)
}

/// Load season episodes (Season items) or show season numbers (Show items).
/// Returns (season_episodes, show_season_numbers, show_status).
async fn load_episode_or_season_data(
    db_pool: &sqlx::PgPool,
    item: &riven_db::entities::MediaItem,
) -> (Vec<(i32, Option<i32>)>, Vec<i32>, Option<ShowStatus>) {
    match item.item_type {
        MediaItemType::Season => {
            let eps = match repo::list_episodes(db_pool, item.id).await {
                Ok(eps) => eps
                    .into_iter()
                    .map(|e| (e.episode_number.unwrap_or(0), e.absolute_number))
                    .collect(),
                Err(e) => {
                    tracing::warn!(id = item.id, error = %e, "failed to load season episodes for validation");
                    vec![]
                }
            };
            (eps, vec![], item.show_status)
        }
        MediaItemType::Show => {
            let season_nums = match repo::list_seasons(db_pool, item.id).await {
                Ok(seasons) => seasons.iter().filter_map(|s| s.season_number).collect(),
                Err(e) => {
                    tracing::warn!(id = item.id, error = %e, "failed to load show seasons for validation");
                    vec![]
                }
            };
            (vec![], season_nums, item.show_status)
        }
        _ => (vec![], vec![], item.show_status),
    }
}

pub async fn run(id: i64, job: &ParseScrapeResultsJob, queue: &JobQueue) {
    tracing::debug!(id, "running parse-scrape-results flow");

    let Some(item) = load_item_or_log(id, &queue.db_pool, "parse-scrape-results").await else {
        return;
    };

    let processable = matches!(
        item.state,
        MediaItemState::Indexed
            | MediaItemState::Ongoing
            | MediaItemState::Scraped
            | MediaItemState::PartiallyCompleted
    );
    if !processable {
        tracing::debug!(id, state = ?item.state, "item not in processable state for scrape persist; skipping");
        return;
    }

    // Parallelise all DB loads that don't depend on each other.
    let (
        (correct_title, parent_year, aliases, show_title_for_format),
        (season_episodes, show_season_numbers, show_status),
        profiles,
        dubbed_anime_only,
    ) = tokio::join!(
        resolve_parent_info(&queue.db_pool, &item),
        load_episode_or_season_data(&queue.db_pool, &item),
        load_active_profiles(&queue.db_pool),
        load_dubbed_anime_only(&queue.db_pool),
    );

    let fallback_settings = if profiles.is_empty() {
        Some(load_fallback_rank_settings(&queue.db_pool).await)
    } else {
        None
    };

    let item_title = match (item.item_type, show_title_for_format.as_deref()) {
        (MediaItemType::Season, Some(show_t)) => format!("{show_t} - {}", item.title),
        _ => item.title.clone(),
    };
    let item_type = item.item_type;

    let ctx = ParseContext {
        item_type: item.item_type,
        season_number: item.season_number,
        episode_number: item.episode_number,
        absolute_number: item.absolute_number,
        item_year: item.year,
        parent_year,
        item_country: item.country.clone(),
        season_episodes,
        show_season_numbers,
        show_status,
        correct_title,
        aliases,
        profiles,
        fallback_settings,
        dubbed_anime_only,
    };

    let responses: Vec<HashMap<String, String>> = queue.flow_load_results("scrape", id).await;
    queue.clear_flow_results("scrape", id).await;

    let streams = responses
        .into_iter()
        .fold(HashMap::new(), |mut acc, streams| {
            acc.extend(streams);
            acc
        });

    // CPU-bound: parse + validate + rank — offloaded to the blocking thread pool.
    // Rayon parallelises across all CPU cores within a single job.
    let ranked = tokio::task::spawn_blocking(move || rank_streams(ctx, streams))
        .await
        .unwrap_or_default();

    // Async: persist stream entities in parallel, then link to this item.
    let stream_count = stream::iter(ranked)
        .map(|candidate| {
            let pool = queue.db_pool.clone();
            async move {
                let stream = match repo::upsert_stream(
                    &pool,
                    &candidate.info_hash,
                    candidate.parsed_data,
                    candidate.rank,
                )
                .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, info_hash = %candidate.info_hash, title = %candidate.title, "failed to upsert stream");
                        return false;
                    }
                };
                match repo::link_stream_to_item(&pool, id, stream.id).await {
                    Ok(_) => true,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to link stream to item");
                        false
                    }
                }
            }
        })
        .buffer_unordered(4)
        .filter(|ok| futures::future::ready(*ok))
        .count()
        .await;

    if let Err(e) = repo::update_scraped(&queue.db_pool, id).await {
        tracing::error!(error = %e, "failed to update scraped timestamp");
    }

    if let Err(e) = repo::refresh_state_cascade(&queue.db_pool, &item).await {
        tracing::error!(error = %e, "failed to refresh state after scrape");
    }

    tracing::info!(id, stream_count, "parse-scrape-results completed");

    if stream_count == 0 {
        let _ = repo::increment_failed_attempts(&queue.db_pool, id).await;
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item_title,
                item_type,
            })
            .await;
    } else {
        // Reset failed_attempts to 0 on success — mirrors riven-ts persist-scrape-results.ts:71.
        let _ = repo::reset_failed_attempts(&queue.db_pool, id).await;
        queue
            .notify(RivenEvent::MediaItemScrapeSuccess {
                id,
                title: item_title,
                item_type,
                stream_count,
            })
            .await;
        if job.auto_download {
            queue.push_download_from_best_stream(id).await;
        }
    }
}
