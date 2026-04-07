use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDate, Utc};

use riven_core::types::*;
use riven_db::entities::{ItemRequest, MediaItem};
use riven_db::repo::{self, ItemRequestUpsertAction};

use crate::context::{load_media_item_or_log, load_show_context};
use crate::{IndexJob, JobQueue, ScrapeJob};

pub struct RequestedItemOutcome {
    pub item: MediaItem,
    pub action: ItemRequestUpsertAction,
}

pub struct LibraryOrchestrator<'a> {
    queue: &'a JobQueue,
}

impl<'a> LibraryOrchestrator<'a> {
    pub fn new(queue: &'a JobQueue) -> Self {
        Self { queue }
    }

    pub async fn upsert_requested_movie(
        &self,
        title: &str,
        imdb_id: Option<&str>,
        tmdb_id: Option<&str>,
        requested_by: Option<&str>,
        external_request_id: Option<&str>,
    ) -> Result<RequestedItemOutcome> {
        let request = repo::create_item_request(
            &self.queue.db_pool,
            imdb_id,
            tmdb_id,
            None,
            ItemRequestType::Movie,
            requested_by,
            external_request_id,
            None,
        )
        .await?;

        let (item, _) = repo::create_movie(
            &self.queue.db_pool,
            title,
            imdb_id,
            tmdb_id,
            Some(request.request.id),
        )
        .await?;

        Ok(RequestedItemOutcome {
            item,
            action: request.action,
        })
    }

    pub async fn upsert_requested_show(
        &self,
        title: &str,
        imdb_id: Option<&str>,
        tvdb_id: Option<&str>,
        requested_by: Option<&str>,
        external_request_id: Option<&str>,
        requested_seasons: Option<&[i32]>,
    ) -> Result<RequestedItemOutcome> {
        let request = repo::create_item_request(
            &self.queue.db_pool,
            imdb_id,
            None,
            tvdb_id,
            ItemRequestType::Show,
            requested_by,
            external_request_id,
            requested_seasons,
        )
        .await?;

        let (item, _) = repo::create_show(
            &self.queue.db_pool,
            title,
            imdb_id,
            tvdb_id,
            Some(request.request.id),
        )
        .await?;

        Ok(RequestedItemOutcome {
            item,
            action: request.action,
        })
    }

    pub async fn enqueue_after_request(
        &self,
        outcome: &RequestedItemOutcome,
        requested_seasons: Option<&[i32]>,
    ) {
        match outcome.item.item_type {
            MediaItemType::Movie => {
                if outcome.action == ItemRequestUpsertAction::Created {
                    self.queue
                        .push_index(IndexJob::from_item(&outcome.item))
                        .await;
                }
            }
            MediaItemType::Show => match outcome.action {
                ItemRequestUpsertAction::Created => {
                    self.queue
                        .push_index(IndexJob::from_item(&outcome.item))
                        .await;
                }
                ItemRequestUpsertAction::Updated => {
                    let requested_specific_seasons = requested_seasons
                        .map(|seasons| !seasons.is_empty())
                        .unwrap_or(false);

                    if outcome.item.imdb_id.is_none() || requested_specific_seasons {
                        self.queue
                            .push_index(IndexJob::from_item(&outcome.item))
                            .await;
                    } else {
                        self.queue_scrape_for_item(&outcome.item, requested_seasons, true)
                            .await;
                    }
                }
                ItemRequestUpsertAction::Unchanged => {}
            },
            _ => {}
        }
    }

    pub async fn retry_item_request(&self, request: &ItemRequest) {
        let item = match request.request_type {
            ItemRequestType::Movie => repo::find_existing_media_item(
                &self.queue.db_pool,
                MediaItemType::Movie,
                request.imdb_id.as_deref(),
                request.tmdb_id.as_deref(),
                None,
            )
            .await
            .ok()
            .flatten(),
            ItemRequestType::Show => repo::find_existing_media_item(
                &self.queue.db_pool,
                MediaItemType::Show,
                request.imdb_id.as_deref(),
                None,
                request.tvdb_id.as_deref(),
            )
            .await
            .ok()
            .flatten(),
        };

        if let Some(item) = item {
            self.queue.push_index(IndexJob::from_item(&item)).await;
        }
    }

    pub async fn sync_item_request_state(&self, item: &MediaItem) {
        let Some(request_id) = item.item_request_id else {
            return;
        };

        let request = match repo::get_item_request_by_id(&self.queue.db_pool, request_id).await {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(error) => {
                tracing::error!(
                    item_id = item.id,
                    request_id,
                    error = %error,
                    "failed to load item request"
                );
                return;
            }
        };

        let request_state = match repo::derive_item_request_state_for_request(
            &self.queue.db_pool,
            &request,
        )
        .await
        {
            Ok(state) => state,
            Err(error) => {
                tracing::error!(
                    item_id = item.id,
                    request_id,
                    error = %error,
                    "failed to derive item request state"
                );
                return;
            }
        };

        if let Err(error) =
            repo::update_item_request_state(&self.queue.db_pool, request_id, request_state).await
        {
            tracing::error!(
                item_id = item.id,
                request_id,
                error = %error,
                "failed to update item request state"
            );
        };
    }

    pub async fn enqueue_after_index(&self, item: &MediaItem, requested_seasons: Option<&[i32]>) {
        self.sync_item_request_state(item).await;
        let auto_download = item.is_requested;

        match item.state {
            MediaItemState::Unreleased => {
                self.schedule_reindex(item).await;
            }
            MediaItemState::Ongoing => {
                self.schedule_reindex(item).await;

                match item.item_type {
                    MediaItemType::Movie | MediaItemType::Episode => {
                        self.queue_scrape_for_item(item, None, auto_download).await;
                    }
                    MediaItemType::Show => {
                        self.queue_scrape_for_item(item, requested_seasons, auto_download)
                            .await;
                    }
                    _ => {}
                }
            }
            _ => {
                self.queue.clear_scheduled_index(item.id).await;

                match item.item_type {
                    MediaItemType::Movie | MediaItemType::Episode => {
                        self.queue_scrape_for_item(item, None, auto_download).await;
                    }
                    MediaItemType::Show => {
                        self.queue_scrape_for_item(item, requested_seasons, auto_download)
                            .await;
                    }
                    _ => {}
                }
            }
        }
    }

    pub async fn queue_scrape_for_item(
        &self,
        item: &MediaItem,
        season_numbers: Option<&[i32]>,
        auto_download: bool,
    ) {
        match item.item_type {
            MediaItemType::Movie => {
                let mut job = ScrapeJob::for_movie(item);
                job.auto_download = auto_download;
                self.queue.push_scrape(job).await;
            }
            MediaItemType::Show => {
                if let Some(seasons) = season_numbers {
                    let _ = repo::mark_seasons_requested_and_get_episodes(
                        &self.queue.db_pool,
                        item.id,
                        seasons,
                    )
                    .await;
                }

                match repo::get_requested_seasons_for_show(&self.queue.db_pool, item.id).await {
                    Ok(seasons) => {
                        for season in seasons.into_iter().filter(|season| {
                            season_numbers
                                .map(|numbers| {
                                    season
                                        .season_number
                                        .map(|number| numbers.contains(&number))
                                        .unwrap_or(false)
                                })
                                .unwrap_or(true)
                        }) {
                            let mut job = ScrapeJob::for_season(
                                &season,
                                item.title.clone(),
                                item.imdb_id.clone(),
                            );
                            job.auto_download = auto_download;
                            self.queue.push_scrape(job).await;
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            show_id = item.id,
                            error = %error,
                            "failed to fetch seasons for scrape"
                        );
                    }
                }
            }
            MediaItemType::Season => {
                let (show_title, show_imdb_id) = self.show_context(item).await;
                let mut job = ScrapeJob::for_season(item, show_title, show_imdb_id);
                job.auto_download = auto_download;
                self.queue.push_scrape(job).await;
            }
            MediaItemType::Episode => {
                let (show_title, show_imdb_id) = self.show_context(item).await;
                let mut job = ScrapeJob::for_episode(item, show_title, show_imdb_id);
                job.auto_download = auto_download;
                self.queue.push_scrape(job).await;
            }
        }
    }

    pub async fn queue_download_for_item(&self, item: &MediaItem) {
        match item.item_type {
            MediaItemType::Movie | MediaItemType::Season => {
                if !self.queue.push_download_from_best_stream(item.id).await {
                    let _ = repo::refresh_state_cascade(&self.queue.db_pool, item).await;
                }
            }
            MediaItemType::Show => {
                match repo::get_scraped_seasons_for_show(&self.queue.db_pool, item.id).await {
                    Ok(seasons) => {
                        for season in &seasons {
                            if !self.queue.push_download_from_best_stream(season.id).await {
                                let _ =
                                    repo::refresh_state_cascade(&self.queue.db_pool, season).await;
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            show_id = item.id,
                            error = %error,
                            "failed to fetch scraped seasons"
                        );
                    }
                }
            }
            _ => {}
        }
    }

    pub async fn fan_out_download_failure(&self, id: i64) {
        let Some(item) =
            load_media_item_or_log(&self.queue.db_pool, id, "fan out download failure").await
        else {
            return;
        };

        match item.item_type {
            MediaItemType::Show => {
                self.queue_scrape_for_item(&item, None, true).await;
            }
            MediaItemType::Season => {
                let (show_title, show_imdb_id) = self.show_context(&item).await;
                match repo::get_incomplete_episodes_for_season(&self.queue.db_pool, item.id).await {
                    Ok(episodes) => {
                        for episode in episodes {
                            self.queue
                                .push_scrape(ScrapeJob::for_episode(
                                    &episode,
                                    show_title.clone(),
                                    show_imdb_id.clone(),
                                ))
                                .await;
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            season_id = item.id,
                            error = %error,
                            "failed to fetch incomplete episodes"
                        );
                    }
                }
            }
            _ => {}
        }
    }

    async fn show_context(&self, item: &MediaItem) -> (String, Option<String>) {
        let ctx = load_show_context(&self.queue.db_pool, item).await;
        (ctx.title, ctx.imdb_id)
    }

    async fn schedule_reindex(&self, item: &MediaItem) {
        let run_at = self.next_reindex_at(item).await;
        self.queue
            .schedule_index_at(IndexJob::from_item(item), run_at)
            .await;
    }

    async fn next_reindex_at(&self, item: &MediaItem) -> DateTime<Utc> {
        let config = self.queue.reindex_config.read().await.clone();
        let offset_minutes = config.schedule_offset_minutes.min(i64::MAX as u64) as i64;
        let fallback_days = config.unknown_air_date_offset_days.min(i64::MAX as u64) as i64;

        let target_date = match item.item_type {
            MediaItemType::Show => {
                match repo::get_next_unreleased_air_date_for_show(&self.queue.db_pool, item.id)
                    .await
                {
                    Ok(Some(date)) => Some(date),
                    Ok(None) => {
                        if item.state == MediaItemState::Unreleased {
                            item.aired_at
                        } else {
                            None
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            show_id = item.id,
                            error = %error,
                            "failed to fetch next unreleased air date"
                        );
                        if item.state == MediaItemState::Unreleased {
                            item.aired_at
                        } else {
                            None
                        }
                    }
                }
            }
            _ => item.aired_at,
        };

        schedule_datetime(target_date, offset_minutes, fallback_days)
    }
}

fn schedule_datetime(
    target_date: Option<NaiveDate>,
    offset_minutes: i64,
    fallback_days: i64,
) -> DateTime<Utc> {
    match target_date {
        Some(date) => {
            let midnight = date
                .and_hms_opt(0, 0, 0)
                .expect("midnight should always be valid");
            DateTime::<Utc>::from_naive_utc_and_offset(midnight, Utc)
                + Duration::minutes(offset_minutes)
        }
        None => Utc::now() + Duration::days(fallback_days),
    }
}
