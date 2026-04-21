use std::collections::HashMap;

use riven_core::events::RivenEvent;
use riven_core::types::{MediaItemType, ShowStatus};
use riven_db::entities::{MediaItem, MediaItemHierarchy};
use riven_db::repo;

use crate::JobQueue;
use crate::discovery::{ParseContext, load_active_profiles, load_dubbed_anime_only};

pub struct ShowContext {
    pub title: String,
    pub imdb_id: Option<String>,
}

pub struct ParseItemContext {
    pub item: MediaItem,
    pub item_title: String,
    pub item_type: MediaItemType,
    pub parse: ParseContext,
}

pub struct DownloadHierarchyContext {
    pub item: MediaItem,
    pub season_id: Option<i64>,
    pub season_number: Option<i32>,
    pub season_episodes: Vec<(i32, Option<i32>)>,
    pub show_id: Option<i64>,
    pub show_title: Option<String>,
    pub show_imdb_id: Option<String>,
    pub show_tvdb_id: Option<String>,
    pub show_year: Option<i32>,
    pub show_genres: Option<serde_json::Value>,
    pub show_network: Option<String>,
    pub show_rating: Option<f64>,
    pub show_content_rating: Option<riven_core::types::ContentRating>,
    pub show_language: Option<String>,
    pub show_country: Option<String>,
    pub show_is_anime: bool,
}

pub async fn load_media_item_hierarchy_or_log(
    db_pool: &sqlx::PgPool,
    id: i64,
    context: &str,
) -> Option<MediaItemHierarchy> {
    match repo::get_media_item_hierarchy(db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => {
            tracing::error!(id, "media item not found for {context}");
            None
        }
        Err(e) => {
            tracing::error!(id, error = %e, "failed to load media item for {context}");
            None
        }
    }
}

/// Load a media item by id, logging an error and returning `None` on failure.
pub async fn load_media_item_or_log(
    db_pool: &sqlx::PgPool,
    id: i64,
    context: &str,
) -> Option<MediaItem> {
    match repo::get_media_item(db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => {
            tracing::error!(id, "media item not found for {context}");
            None
        }
        Err(e) => {
            tracing::error!(id, error = %e, "failed to load media item for {context}");
            None
        }
    }
}

pub async fn load_media_item_or_download_error(
    queue: &JobQueue,
    id: i64,
    error_msg: &str,
) -> Option<MediaItem> {
    match load_media_item_or_log(&queue.db_pool, id, error_msg).await {
        Some(item) => Some(item),
        None => {
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: String::new(),
                    error: error_msg.into(),
                })
                .await;
            None
        }
    }
}

pub async fn load_requested_seasons(db_pool: &sqlx::PgPool, item: &MediaItem) -> Option<Vec<i32>> {
    let req_id = item.item_request_id?;
    repo::get_item_request_by_id(db_pool, req_id)
        .await
        .ok()
        .flatten()
        .and_then(|req| req.seasons)
        .and_then(|s| serde_json::from_value(s).ok())
}

pub async fn load_show_context(db_pool: &sqlx::PgPool, item: &MediaItem) -> ShowContext {
    let Some(hierarchy) =
        load_media_item_hierarchy_or_log(db_pool, item.id, "load show context").await
    else {
        return ShowContext {
            title: item.title.clone(),
            imdb_id: item.imdb_id.clone(),
        };
    };

    if let Some(show_title) = hierarchy.resolved_show_title {
        ShowContext {
            title: show_title,
            imdb_id: hierarchy.resolved_show_imdb_id,
        }
    } else {
        ShowContext {
            title: item.title.clone(),
            imdb_id: item.imdb_id.clone(),
        }
    }
}

pub async fn build_parse_item_context(db_pool: &sqlx::PgPool, item: MediaItem) -> ParseItemContext {
    let hierarchy =
        load_media_item_hierarchy_or_log(db_pool, item.id, "build parse item context").await;
    build_parse_item_context_with_hierarchy(db_pool, item, hierarchy.as_ref()).await
}

pub async fn build_parse_item_context_with_hierarchy(
    db_pool: &sqlx::PgPool,
    item: MediaItem,
    hierarchy: Option<&MediaItemHierarchy>,
) -> ParseItemContext {
    let (
        (correct_title, parent_year, aliases, show_title_for_format),
        (season_episodes, show_season_numbers, show_status),
        profiles,
        dubbed_anime_only,
    ) = tokio::join!(
        resolve_parent_info(&item, hierarchy),
        load_episode_or_season_data(db_pool, &item),
        load_active_profiles(db_pool),
        load_dubbed_anime_only(db_pool),
    );

    let item_title = match (item.item_type, show_title_for_format.as_deref()) {
        (MediaItemType::Season, Some(show_t)) => format!("{show_t} - {}", item.title),
        _ => item.title.clone(),
    };
    let item_type = item.item_type;

    let parse = ParseContext {
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
        dubbed_anime_only,
    };

    ParseItemContext {
        item,
        item_title,
        item_type,
        parse,
    }
}

pub async fn load_download_hierarchy_context(
    db_pool: &sqlx::PgPool,
    item: &MediaItem,
) -> DownloadHierarchyContext {
    let (hierarchy, (season_episodes, _, _)) = tokio::join!(
        load_media_item_hierarchy_or_log(db_pool, item.id, "load download hierarchy context"),
        load_episode_or_season_data(db_pool, item),
    );

    let default_show_id = match item.item_type {
        MediaItemType::Show => Some(item.id),
        MediaItemType::Season => item.parent_id,
        _ => None,
    };

    let default_show_title = matches!(item.item_type, MediaItemType::Show | MediaItemType::Movie)
        .then(|| item.title.clone());
    let default_show_imdb_id = matches!(item.item_type, MediaItemType::Show | MediaItemType::Movie)
        .then(|| item.imdb_id.clone())
        .flatten();

    let default_season_id = (item.item_type == MediaItemType::Season)
        .then_some(item.id)
        .or(if item.item_type == MediaItemType::Episode {
            item.parent_id
        } else {
            None
        });

    let default_season_number = match item.item_type {
        MediaItemType::Season | MediaItemType::Episode => item.season_number,
        _ => None,
    };

    DownloadHierarchyContext {
        item: item.clone(),
        season_id: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_season_id)
            .or(default_season_id),
        season_number: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_season_number)
            .or(default_season_number),
        season_episodes,
        show_id: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_id)
            .or(default_show_id),
        show_title: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_title.clone())
            .or(default_show_title),
        show_imdb_id: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_imdb_id.clone())
            .or(default_show_imdb_id),
        show_tvdb_id: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_tvdb_id.clone())
            .or_else(|| item.tvdb_id.clone()),
        show_year: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_year)
            .or(item.year),
        show_genres: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_genres.clone())
            .or_else(|| item.genres.clone()),
        show_network: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_network.clone())
            .or_else(|| item.network.clone()),
        show_rating: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_rating)
            .or(item.rating),
        show_content_rating: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_content_rating)
            .or(item.content_rating),
        show_language: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_language.clone())
            .or_else(|| item.language.clone()),
        show_country: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_country.clone())
            .or_else(|| item.country.clone()),
        show_is_anime: hierarchy
            .as_ref()
            .and_then(|h| h.resolved_show_is_anime)
            .unwrap_or(item.is_anime),
    }
}

/// Resolve the show title, year, and aliases for Season/Episode items by
/// walking up the parent chain. Returns (correct_title, parent_year, aliases,
/// show_title_for_format).
async fn resolve_parent_info(
    item: &MediaItem,
    hierarchy: Option<&MediaItemHierarchy>,
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

    if let Some(hierarchy) = hierarchy
        && let Some(show_title) = hierarchy.resolved_show_title.clone()
    {
        let aliases = hierarchy
            .resolved_show_aliases
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        return (
            show_title.clone(),
            hierarchy.resolved_show_year,
            aliases,
            Some(show_title),
        );
    }

    (item.title.clone(), None, initial_aliases, None)
}

/// Load season episodes (Season items) or show season numbers (Show items).
/// Returns (season_episodes, show_season_numbers, show_status).
async fn load_episode_or_season_data(
    db_pool: &sqlx::PgPool,
    item: &MediaItem,
) -> (Vec<(i32, Option<i32>)>, Vec<i32>, Option<ShowStatus>) {
    match item.item_type {
        MediaItemType::Season => {
            let eps = match repo::list_episodes(db_pool, item.id).await {
                Ok(eps) => eps
                    .into_iter()
                    .map(|e| (e.episode_number.unwrap_or(0), e.absolute_number))
                    .collect(),
                Err(e) => {
                    tracing::warn!(
                        id = item.id,
                        error = %e,
                        "failed to load season episodes for validation"
                    );
                    vec![]
                }
            };
            (eps, vec![], item.show_status)
        }
        MediaItemType::Show => {
            let season_nums = match repo::list_seasons(db_pool, item.id).await {
                Ok(seasons) => seasons.iter().filter_map(|s| s.season_number).collect(),
                Err(e) => {
                    tracing::warn!(
                        id = item.id,
                        error = %e,
                        "failed to load show seasons for validation"
                    );
                    vec![]
                }
            };
            (vec![], season_nums, item.show_status)
        }
        _ => (vec![], vec![], item.show_status),
    }
}
