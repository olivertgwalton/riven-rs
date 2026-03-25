use async_graphql::*;
use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::entities::*;
use riven_db::repo;
use riven_queue::{IndexJob, JobQueue, ScrapeJob};
use std::sync::Arc;

// ── Mutation root ──

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Update rank settings. Accepts a full or partial JSON object that will be
    /// merged with defaults. Returns the saved settings.
    async fn update_rank_settings(
        &self,
        ctx: &Context<'_>,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let _: riven_rank::RankSettings = serde_json::from_value(settings.clone())
            .map_err(|e| Error::new(format!("invalid rank settings: {e}")))?;

        let pool = ctx.data::<sqlx::PgPool>()?;
        repo::set_setting(pool, "rank_settings", settings.clone()).await?;
        Ok(settings)
    }

    /// Update all settings. Accepts a JSON object of key/value pairs.
    async fn update_all_settings(
        &self,
        ctx: &Context<'_>,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::set_all_settings(pool, settings).await?)
    }

    /// Reset items to Indexed state and clear failed_attempts.
    async fn reset_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::reset_items_by_ids(pool, ids).await? as i64)
    }

    /// Clear failed_attempts for items so they will be retried.
    async fn retry_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::retry_items_by_ids(pool, ids).await? as i64)
    }

    /// Remove items by ID.
    async fn remove_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let external_request_ids =
            repo::get_external_request_ids_for_items(pool, &ids).await.unwrap_or_default();

        let count = repo::delete_items_by_ids(pool, ids).await? as i64;

        if !external_request_ids.is_empty() {
            job_queue
                .notify(RivenEvent::MediaItemsDeleted { external_request_ids })
                .await;
        }

        Ok(count)
    }

    /// Pause items.
    async fn pause_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::pause_items_by_ids(pool, ids).await? as i64)
    }

    /// Unpause items (transitions back to Indexed).
    async fn unpause_items(&self, ctx: &Context<'_>, ids: Vec<i64>) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::unpause_items_by_ids(pool, ids).await? as i64)
    }

    /// Update settings for a specific plugin (stored under "plugin.{name}" key).
    /// Also re-validates the plugin with the new settings in-memory.
    /// Returns an object with the saved settings and the new valid status.
    async fn update_plugin_settings(
        &self,
        ctx: &Context<'_>,
        plugin: String,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        use riven_core::plugin::PluginRegistry;
        use std::sync::Arc;

        let pool = ctx.data::<sqlx::PgPool>()?;
        let key = format!("plugin.{plugin}");
        repo::set_setting(pool, &key, settings.clone()).await?;

        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let valid = registry.revalidate_plugin(&plugin, &settings).await;

        Ok(serde_json::json!({ "settings": settings, "valid": valid }))
    }

    /// Update general (non-plugin) settings and apply them to the live runtime config.
    async fn update_general_settings(
        &self,
        ctx: &Context<'_>,
        settings: serde_json::Value,
    ) -> Result<serde_json::Value> {
        use riven_core::downloader::DownloaderConfig;
        use tokio::sync::RwLock;

        let pool = ctx.data::<sqlx::PgPool>()?;
        repo::set_setting(pool, "general", settings.clone()).await?;

        let cfg = ctx.data::<std::sync::Arc<RwLock<DownloaderConfig>>>()?;
        let mut cfg = cfg.write().await;
        cfg.minimum_average_bitrate_movies = settings
            .get("minimum_average_bitrate_movies")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        cfg.minimum_average_bitrate_episodes = settings
            .get("minimum_average_bitrate_episodes")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        cfg.maximum_average_bitrate_movies = settings
            .get("maximum_average_bitrate_movies")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        cfg.maximum_average_bitrate_episodes = settings
            .get("maximum_average_bitrate_episodes")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        Ok(settings)
    }

    /// Trigger a scrape for an existing item.
    /// For shows, optionally provide season_numbers to scrape specific seasons.
    /// If season_numbers is omitted, all requested seasons in Indexed state are scraped.
    async fn scrape_item(
        &self,
        ctx: &Context<'_>,
        id: i64,
        season_numbers: Option<Vec<i32>>,
    ) -> Result<String> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let item = repo::get_media_item(pool, id)
            .await?
            .ok_or_else(|| Error::new("Item not found"))?;

        match item.item_type {
            MediaItemType::Movie | MediaItemType::Episode => {
                job_queue
                    .push_scrape(ScrapeJob {
                        id: item.id,
                        item_type: item.item_type,
                        imdb_id: item.imdb_id.clone(),
                        title: item.title.clone(),
                        season: item.season_number,
                        episode: item.episode_number,
                    })
                    .await;
            }
            MediaItemType::Season => {
                let show_imdb_id = if let Some(parent_id) = item.parent_id {
                    repo::get_media_item(pool, parent_id)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|s| s.imdb_id)
                } else {
                    None
                };
                job_queue
                    .push_scrape(ScrapeJob {
                        id: item.id,
                        item_type: item.item_type,
                        imdb_id: show_imdb_id,
                        title: item.title.clone(),
                        season: item.season_number,
                        episode: None,
                    })
                    .await;
            }
            MediaItemType::Show => {
                let show_imdb_id = item.imdb_id.clone();
                let seasons = repo::get_requested_seasons_for_show(pool, item.id).await?;
                for season in seasons {
                    if let Some(ref nums) = season_numbers {
                        if !season.season_number.map(|n| nums.contains(&n)).unwrap_or(false) {
                            continue;
                        }
                    }
                    job_queue
                        .push_scrape(ScrapeJob {
                            id: season.id,
                            item_type: season.item_type,
                            imdb_id: show_imdb_id.clone(),
                            title: season.title.clone(),
                            season: season.season_number,
                            episode: None,
                        })
                        .await;
                }
            }
        }

        Ok("Scrape queued".to_string())
    }

    /// Add a new media item to track and immediately queue it for indexing.
    /// For shows, `seasons` is an optional list of season numbers to request.
    /// If omitted, all non-special seasons are requested.
    async fn add_item(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        seasons: Option<Vec<i32>>,
    ) -> Result<MediaItem> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let request_type = match item_type {
            MediaItemType::Movie => ItemRequestType::Movie,
            MediaItemType::Show => ItemRequestType::Show,
            _ => return Err(Error::new("Only Movie and Show types can be added directly")),
        };

        let existing = repo::find_existing_media_item(
            pool,
            item_type,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
        )
        .await?;

        let request = repo::create_item_request(
            pool,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            request_type,
            None,
            None,
            seasons.as_deref(),
        )
        .await?;

        let (item, _was_created) = match item_type {
            MediaItemType::Movie => {
                repo::create_movie(pool, &title, imdb_id.as_deref(), tmdb_id.as_deref(), Some(request.id)).await?
            }
            MediaItemType::Show => {
                repo::create_show(pool, &title, imdb_id.as_deref(), tvdb_id.as_deref(), Some(request.id)).await?
            }
            _ => unreachable!(),
        };

        if existing.is_some() && item_type == MediaItemType::Show {
            if let Some(season_numbers) = seasons.as_deref() {
                if !season_numbers.is_empty() {
                    let _ = repo::mark_seasons_requested_and_get_episodes(
                        pool,
                        item.id,
                        season_numbers,
                    )
                    .await;
                }
            }

            // If the show has no imdb_id yet (e.g. created by Seerr with only tvdb_id
            // before indexing completed), re-index so the indexer fills in imdb_id and
            // then pushes scrape jobs with the correct ID.
            if item.imdb_id.is_none() {
                job_queue
                    .push_index(IndexJob {
                        id: item.id,
                        item_type,
                        imdb_id: item.imdb_id.clone(),
                        tmdb_id: item.tmdb_id.clone(),
                        tvdb_id: item.tvdb_id.clone(),
                    })
                    .await;
            } else {
                let show_imdb_id = item.imdb_id.clone();
                if let Ok(season_items) =
                    repo::get_requested_seasons_for_show(pool, item.id).await
                {
                    for season in season_items {
                        if season.state == riven_core::types::MediaItemState::Indexed {
                            job_queue
                                .push_scrape(ScrapeJob {
                                    id: season.id,
                                    item_type: season.item_type,
                                    imdb_id: show_imdb_id.clone(),
                                    title: season.title.clone(),
                                    season: season.season_number,
                                    episode: None,
                                })
                                .await;
                        }
                    }
                }
            }
        } else {
            job_queue
                .push_index(IndexJob {
                    id: item.id,
                    item_type,
                    imdb_id,
                    tmdb_id,
                    tvdb_id,
                })
                .await;
        }

        Ok(item)
    }
}
