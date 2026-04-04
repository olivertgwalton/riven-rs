use std::collections::{HashMap, HashSet};

use futures::stream::{self, StreamExt};
use rayon::prelude::*;

use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::repo;
use riven_rank::RankSettings;

use super::{load_active_profiles, load_item_or_log};
use crate::{JobQueue, ParseScrapeResultsJob};

/// All data needed for the CPU-bound parse + validate + rank loop.
/// Must be Send + 'static so it can cross the spawn_blocking boundary.
struct ParseContext {
    item_type: MediaItemType,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_number: Option<i32>,
    item_year: Option<i32>,
    /// Year of the top-level show (for seasons/episodes).
    parent_year: Option<i32>,
    item_country: Option<String>,
    /// (episode_number, absolute_number) for every episode in the season.
    season_episodes: Vec<(i32, Option<i32>)>,
    /// Season numbers that belong to the show (for Show-level scrapes).
    show_season_numbers: Vec<i32>,
    show_status: Option<ShowStatus>,
    /// Show title used for title-similarity ranking.
    correct_title: String,
    aliases: HashMap<String, Vec<String>>,
    profiles: Vec<(String, RankSettings)>,
    fallback_settings: Option<RankSettings>,
    /// Mirror of riven-ts `settings.dubbedAnimeOnly`.
    dubbed_anime_only: bool,
}

fn year_candidates(year: i32) -> [i32; 3] {
    [year - 1, year, year + 1]
}

/// Returns `Some(reason)` to skip the torrent, `None` to keep it.
fn validate(ctx: &ParseContext, parsed: &riven_rank::ParsedData) -> Option<String> {
    let has_episodes = !parsed.episodes.is_empty();
    let has_seasons = !parsed.seasons.is_empty();

    // `parsed.anime` is proxy for `item.isAnime` — dubbed-anime only mode skips non-dubbed anime
    if ctx.dubbed_anime_only && parsed.anime && !parsed.dubbed {
        return Some("non-dubbed anime torrent (dubbed_anime_only=true)".into());
    }

    if !parsed.anime {
        if let (Some(pc), Some(ic)) = (parsed.country.as_deref(), ctx.item_country.as_deref()) {
            if !pc.eq_ignore_ascii_case(ic) {
                return Some(format!("incorrect country: {pc} vs {ic}"));
            }
        }
    }

    if let Some(py) = parsed.year {
        let mut candidates: HashSet<i32> = HashSet::new();
        if let Some(y) = ctx.item_year {
            candidates.extend(year_candidates(y));
        }
        if let Some(y) = ctx.parent_year {
            candidates.extend(year_candidates(y));
        }
        if !candidates.is_empty() && !candidates.contains(&py) {
            return Some(format!("incorrect year: {py}"));
        }
    }

    match ctx.item_type {
        MediaItemType::Movie => {
            if has_seasons || has_episodes {
                return Some("show torrent for movie".into());
            }
        }
        MediaItemType::Episode => {
            if !has_episodes && !has_seasons {
                return Some("no seasons or episodes for episode item".into());
            }
            if has_episodes {
                let ep_num = ctx.episode_number.unwrap_or(0);
                let matches_relative = parsed.episodes.contains(&ep_num);
                let matches_absolute = ctx
                    .absolute_number
                    .map(|a| parsed.episodes.contains(&a))
                    .unwrap_or(false);
                if !matches_relative && !matches_absolute {
                    return Some(format!(
                        "incorrect episode number for episode item: {:?} does not include ep {} (abs {:?})",
                        parsed.episodes, ep_num, ctx.absolute_number
                    ));
                }
            }
            if has_seasons {
                let season_num = ctx.season_number.unwrap_or(0);
                if !parsed.seasons.contains(&season_num) {
                    return Some(format!(
                        "incorrect season number for episode item: {:?} does not include season {}",
                        parsed.seasons, season_num
                    ));
                }
            }
        }
        MediaItemType::Season => {
            if !has_episodes && !has_seasons {
                return Some("no seasons or episodes for season item".into());
            }
            if !has_seasons {
                if has_episodes {
                    // No season info in torrent — check absolute episode range.
                    // e.g. "One Piece 0001-1000" has no S## tag, only episode numbers.
                    let abs_eps: HashSet<i32> = ctx
                        .season_episodes
                        .iter()
                        .filter_map(|(_, abs)| *abs)
                        .collect();
                    if !abs_eps.is_empty() {
                        let torrent_eps: HashSet<i32> = parsed.episodes.iter().copied().collect();
                        let intersection = abs_eps.intersection(&torrent_eps).count();
                        if intersection != abs_eps.len() {
                            return Some("incorrect absolute episode range for season item".into());
                        }
                    }
                }
            } else {
                let season_num = ctx.season_number.unwrap_or(1);
                if !parsed.seasons.contains(&season_num) {
                    return Some(format!(
                        "incorrect season number for season item: {:?} does not include season {}",
                        parsed.seasons, season_num
                    ));
                }
                if has_episodes {
                    // Torrent has both season and episode numbers — check relative coverage.
                    let rel_eps: HashSet<i32> =
                        ctx.season_episodes.iter().map(|(ep, _)| *ep).collect();
                    if !rel_eps.is_empty() {
                        let torrent_eps: HashSet<i32> = parsed.episodes.iter().copied().collect();
                        let intersection = rel_eps.intersection(&torrent_eps).count();
                        if intersection != rel_eps.len() {
                            return Some("incorrect episodes for season item".into());
                        }
                    }
                }
            }
        }
        MediaItemType::Show => {
            if !has_episodes && !has_seasons {
                return Some("no seasons or episodes for show item".into());
            }
            if has_seasons && !ctx.show_season_numbers.is_empty() {
                let show_seasons: HashSet<i32> = ctx.show_season_numbers.iter().copied().collect();
                let torrent_seasons: HashSet<i32> = parsed.seasons.iter().copied().collect();
                let intersection = show_seasons.intersection(&torrent_seasons).count();
                let expected = if ctx.show_status == Some(ShowStatus::Continuing) {
                    show_seasons.len().saturating_sub(1)
                } else {
                    show_seasons.len()
                };
                if intersection < expected {
                    return Some(format!(
                        "incorrect number of seasons for show: {intersection} < {expected}"
                    ));
                }
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

/// Load the `dubbed_anime_only` flag from the `general` settings key.
async fn load_dubbed_anime_only(db_pool: &sqlx::PgPool) -> bool {
    match repo::get_setting(db_pool, "general").await {
        Ok(Some(v)) => v
            .get("dubbed_anime_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        _ => false,
    }
}

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

pub async fn run(id: i64, _job: &ParseScrapeResultsJob, queue: &JobQueue) {
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
        Some(load_rank_settings(&queue.db_pool).await)
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
    let ranked: Vec<(String, Option<serde_json::Value>, Option<i64>)> =
        tokio::task::spawn_blocking(move || {
            streams
                .par_iter()
                .filter_map(|(info_hash, title)| {
                    let parsed = riven_rank::parse(title);

                    if let Some(reason) = validate(&ctx, &parsed) {
                        tracing::debug!(info_hash, title, reason, "torrent skipped");
                        return None;
                    }

                    let best = if let Some(ref settings) = ctx.fallback_settings {
                        riven_rank::rank_torrent(
                            title,
                            info_hash,
                            &ctx.correct_title,
                            &ctx.aliases,
                            settings,
                        )
                        .ok()
                    } else {
                        ctx.profiles
                            .iter()
                            .filter_map(|(_, settings)| {
                                riven_rank::rank_torrent(
                                    title,
                                    info_hash,
                                    &ctx.correct_title,
                                    &ctx.aliases,
                                    settings,
                                )
                                .ok()
                            })
                            .max_by_key(|r| r.rank)
                    };

                    let (parsed_value, rank) = match best {
                        Some(ranked) => {
                            let bitrate = ranked.data.bitrate.as_deref().unwrap_or("unknown");
                            tracing::info!(
                                info_hash,
                                rank = ranked.rank,
                                bitrate,
                                title,
                                "stream ranked"
                            );
                            (serde_json::to_value(&ranked.data).ok(), Some(ranked.rank))
                        }
                        None => {
                            tracing::debug!(
                                info_hash,
                                title,
                                "stream rejected by all ranking profiles"
                            );
                            (serde_json::to_value(&parsed).ok(), None)
                        }
                    };

                    Some((info_hash.clone(), parsed_value, rank))
                })
                .collect()
        })
        .await
        .unwrap_or_default();

    // Async: persist stream entities in parallel, then link to this item.
    let stream_count = stream::iter(ranked)
        .map(|(info_hash, parsed_data, rank)| {
            let pool = queue.db_pool.clone();
            async move {
                let stream = match repo::upsert_stream(&pool, &info_hash, parsed_data, rank).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, info_hash, "failed to upsert stream");
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
        queue.push_download_from_best_stream(id).await;
    }
}
