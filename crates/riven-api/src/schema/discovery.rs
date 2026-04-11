use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_graphql::{Error, Result};
use riven_core::events::{HookResponse, RivenEvent};
use riven_core::plugin::PluginRegistry;
use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;
use riven_queue::JobQueue;
use riven_queue::discovery::{
    ParseContext, load_active_profiles, load_dubbed_anime_only, load_fallback_rank_settings,
    rank_streams,
};
use riven_queue::indexing::apply_indexed_media_item;
use riven_queue::orchestrator::LibraryOrchestrator;

use super::types::DiscoveredStream;

pub struct DiscoveryTarget {
    pub item_type: MediaItemType,
    pub season_number: Option<i32>,
    pub scrape_title: String,
    pub parse_ctx: ParseContext,
}

pub async fn run_index_discovery(
    registry: &PluginRegistry,
    item_type: MediaItemType,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<IndexedMediaItem> {
    let event = RivenEvent::MediaItemIndexRequested {
        id: 0,
        item_type,
        imdb_id: imdb_id.map(ToOwned::to_owned),
        tvdb_id: tvdb_id.map(ToOwned::to_owned),
        tmdb_id: tmdb_id.map(ToOwned::to_owned),
    };

    let merged = registry
        .dispatch(&event)
        .await
        .into_iter()
        .filter_map(|(_, result)| match result {
            Ok(HookResponse::Index(indexed)) => Some(*indexed),
            _ => None,
        })
        .fold(IndexedMediaItem::default(), |acc, indexed| {
            acc.merge(indexed)
        });

    if merged.title.is_none()
        && merged.imdb_id.is_none()
        && merged.tmdb_id.is_none()
        && merged.tvdb_id.is_none()
    {
        return Err(Error::new("No indexer plugin returned metadata"));
    }

    Ok(merged)
}

pub async fn run_scrape_discovery(
    registry: &PluginRegistry,
    item_type: MediaItemType,
    imdb_id: Option<&str>,
    title: &str,
    season: Option<i32>,
    episode: Option<i32>,
) -> ScrapeResponse {
    let event = RivenEvent::MediaItemScrapeRequested {
        id: 0,
        item_type,
        imdb_id: imdb_id.map(ToOwned::to_owned),
        title: title.to_string(),
        season,
        episode,
    };

    registry
        .dispatch(&event)
        .await
        .into_iter()
        .fold(HashMap::new(), |mut acc, (_, result)| {
            if let Ok(HookResponse::Scrape(streams)) = result {
                acc.extend(streams);
            }
            acc
        })
}

pub fn build_discovery_targets(
    item_type: MediaItemType,
    requested_title: &str,
    indexed: &IndexedMediaItem,
    seasons: Option<&[i32]>,
    profiles: Vec<(String, riven_rank::RankSettings)>,
    fallback_settings: Option<riven_rank::RankSettings>,
    dubbed_anime_only: bool,
) -> Result<Vec<DiscoveryTarget>> {
    let correct_title = indexed
        .title
        .clone()
        .unwrap_or_else(|| requested_title.to_string());
    let aliases = indexed.aliases.clone().unwrap_or_default();

    match item_type {
        MediaItemType::Movie => Ok(vec![DiscoveryTarget {
            item_type: MediaItemType::Movie,
            season_number: None,
            scrape_title: correct_title.clone(),
            parse_ctx: ParseContext {
                item_type: MediaItemType::Movie,
                season_number: None,
                episode_number: None,
                absolute_number: None,
                item_year: indexed.year,
                parent_year: None,
                item_country: indexed.country.clone(),
                season_episodes: vec![],
                show_season_numbers: vec![],
                show_status: indexed.status,
                correct_title,
                aliases,
                profiles,
                fallback_settings,
                dubbed_anime_only,
            },
        }]),
        MediaItemType::Show => {
            let all_seasons = indexed.seasons.clone().unwrap_or_default();
            let selected: Vec<i32> = if let Some(numbers) = seasons {
                numbers.to_vec()
            } else {
                all_seasons
                    .iter()
                    .filter(|season| season.number > 0)
                    .map(|season| season.number)
                    .collect()
            };

            if selected.is_empty() {
                return Err(Error::new("Select at least one season to find streams"));
            }

            let mut targets = Vec::new();
            for number in selected {
                let season = all_seasons
                    .iter()
                    .find(|season| season.number == number)
                    .ok_or_else(|| {
                        Error::new(format!("Season {number} is not available from index data"))
                    })?;

                targets.push(DiscoveryTarget {
                    item_type: MediaItemType::Season,
                    season_number: Some(number),
                    scrape_title: correct_title.clone(),
                    parse_ctx: ParseContext {
                        item_type: MediaItemType::Season,
                        season_number: Some(number),
                        episode_number: None,
                        absolute_number: None,
                        item_year: None,
                        parent_year: indexed.year,
                        item_country: indexed.country.clone(),
                        season_episodes: season
                            .episodes
                            .iter()
                            .map(|episode| (episode.number, episode.absolute_number))
                            .collect(),
                        show_season_numbers: vec![],
                        show_status: indexed.status,
                        correct_title: correct_title.clone(),
                        aliases: aliases.clone(),
                        profiles: profiles.clone(),
                        fallback_settings: fallback_settings.clone(),
                        dubbed_anime_only,
                    },
                });
            }
            Ok(targets)
        }
        _ => Err(Error::new("Only Movie and Show discovery is supported")),
    }
}

pub async fn discover_streams(
    pool: &sqlx::PgPool,
    registry: &PluginRegistry,
    item_type: MediaItemType,
    title: &str,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    seasons: Option<&[i32]>,
    cached_only: bool,
) -> Result<Vec<DiscoveredStream>> {
    let indexed = run_index_discovery(registry, item_type, imdb_id, tmdb_id, tvdb_id).await?;
    let imdb_id = indexed.imdb_id.as_deref().or(imdb_id);

    let (profiles, dubbed_anime_only) =
        tokio::join!(load_active_profiles(pool), load_dubbed_anime_only(pool),);
    let fallback_settings = if profiles.is_empty() {
        Some(load_fallback_rank_settings(pool).await)
    } else {
        None
    };

    let targets = build_discovery_targets(
        item_type,
        title,
        &indexed,
        seasons,
        profiles,
        fallback_settings,
        dubbed_anime_only,
    )?;

    let mut discovered = Vec::new();
    for target in targets {
        let scraped = run_scrape_discovery(
            registry,
            target.item_type,
            imdb_id,
            &target.scrape_title,
            target.season_number,
            None,
        )
        .await;
        let ranked = tokio::task::spawn_blocking({
            let parse_ctx = target.parse_ctx.clone();
            move || rank_streams(parse_ctx, scraped)
        })
        .await
        .map_err(|_| Error::new("Failed to rank discovered streams"))?;

        discovered.extend(ranked.into_iter().map(|candidate| DiscoveredStream {
            key: format!(
                "{}:{}",
                target.season_number.unwrap_or(0),
                candidate.info_hash.to_lowercase()
            ),
            title: candidate.title,
            magnet: build_magnet_uri(&candidate.info_hash),
            info_hash: candidate.info_hash,
            parsed_data: candidate.parsed_data,
            rank: candidate.rank,
            file_size_bytes: None,
            is_cached: false,
            item_type: target.item_type,
            season_number: target.season_number,
        }));
    }

    apply_cache_status(registry, &mut discovered).await;

    if cached_only {
        discovered.retain(|stream| stream.is_cached);
    }

    discovered.sort_by(|a, b| {
        a.season_number
            .cmp(&b.season_number)
            .then_with(|| (b.rank.unwrap_or(-1)).cmp(&a.rank.unwrap_or(-1)))
            .then_with(|| a.info_hash.cmp(&b.info_hash))
    });

    Ok(discovered)
}

async fn apply_cache_status(registry: &PluginRegistry, streams: &mut [DiscoveredStream]) {
    if streams.is_empty() {
        return;
    }

    let hashes: Vec<String> = streams
        .iter()
        .map(|stream| stream.info_hash.to_lowercase())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let results = registry
        .dispatch(&RivenEvent::MediaItemDownloadCacheCheckRequested { hashes })
        .await;

    let mut cached_hashes = HashSet::new();
    let mut file_sizes: HashMap<String, i64> = HashMap::new();

    for (_, result) in results {
        let Ok(HookResponse::CacheCheck(cache_results)) = result else {
            continue;
        };

        for cache_result in cache_results {
            if matches!(
                cache_result.status,
                TorrentStatus::Cached | TorrentStatus::Downloaded
            ) {
                let hash = cache_result.hash.to_lowercase();
                cached_hashes.insert(hash.clone());
                let total_size = cache_result
                    .files
                    .iter()
                    .map(|file| file.size as i64)
                    .sum::<i64>();
                file_sizes
                    .entry(hash)
                    .and_modify(|size| *size = (*size).max(total_size))
                    .or_insert(total_size);
            }
        }
    }

    for stream in streams {
        let hash = stream.info_hash.to_lowercase();
        stream.is_cached = cached_hashes.contains(&hash);
        if stream.file_size_bytes.is_none() {
            stream.file_size_bytes = file_sizes.get(&hash).copied();
        }
    }
}

pub async fn ensure_download_target(
    pool: &sqlx::PgPool,
    registry: &PluginRegistry,
    queue: &Arc<JobQueue>,
    item_type: MediaItemType,
    title: &str,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    season_number: Option<i32>,
) -> Result<MediaItem> {
    let orchestrator = LibraryOrchestrator::new(queue.as_ref());

    match item_type {
        MediaItemType::Movie => {
            let outcome = orchestrator
                .upsert_requested_movie(title, imdb_id, tmdb_id, None, None)
                .await
                .map_err(Error::from)?;

            let needs_index = outcome.item.year.is_none() || outcome.item.imdb_id.is_none();
            if needs_index {
                let indexed =
                    run_index_discovery(registry, MediaItemType::Movie, imdb_id, tmdb_id, None)
                        .await?;
                apply_indexed_media_item(pool, &outcome.item, &indexed, None)
                    .await
                    .map_err(Error::from)?;
            }

            repo::get_media_item(pool, outcome.item.id)
                .await?
                .ok_or_else(|| Error::new("Movie not found after preparation"))
        }
        MediaItemType::Season => {
            let season_number =
                season_number.ok_or_else(|| Error::new("Season number is required"))?;
            let requested = [season_number];
            let outcome = orchestrator
                .upsert_requested_show(title, imdb_id, tvdb_id, None, None, Some(&requested))
                .await
                .map_err(Error::from)?;

            let mut needs_index = outcome.item.imdb_id.is_none();
            let existing_seasons = repo::list_seasons(pool, outcome.item.id).await?;
            let existing_season = existing_seasons
                .into_iter()
                .find(|season| season.season_number == Some(season_number));

            if existing_season.is_none() {
                needs_index = true;
            } else if let Some(ref season) = existing_season {
                needs_index = repo::list_episodes(pool, season.id).await?.is_empty();
            }

            if needs_index {
                let indexed =
                    run_index_discovery(registry, MediaItemType::Show, imdb_id, None, tvdb_id)
                        .await?;
                apply_indexed_media_item(pool, &outcome.item, &indexed, Some(&requested))
                    .await
                    .map_err(Error::from)?;
            }

            repo::list_seasons(pool, outcome.item.id)
                .await?
                .into_iter()
                .find(|season| season.season_number == Some(season_number))
                .ok_or_else(|| {
                    Error::new(format!(
                        "Season {season_number} not found after preparation"
                    ))
                })
        }
        _ => Err(Error::new("Only movie and season downloads are supported")),
    }
}
