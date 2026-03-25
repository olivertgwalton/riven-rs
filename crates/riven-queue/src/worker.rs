use std::sync::Arc;
use std::time::Duration;

use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::{DownloadJob, IndexJob, JobQueue, ScrapeJob};

/// Periodic scheduler that triggers content service requests and retries stuck items.
/// Mirrors riven-ts: content service every 120 s, retryLibrary every 60 s.
/// Unreleased items are checked once per day.
pub struct Scheduler {
    db_pool: sqlx::PgPool,
    job_queue: Arc<JobQueue>,
    content_interval: Duration,
    retry_interval: Duration,
    unreleased_interval: Duration,
}

impl Scheduler {
    pub fn new(db_pool: sqlx::PgPool, job_queue: Arc<JobQueue>) -> Self {
        Self {
            db_pool,
            job_queue,
            content_interval: Duration::from_secs(120),
            retry_interval: Duration::from_secs(60),
            unreleased_interval: Duration::from_secs(86400),
        }
    }

    pub async fn run(self) {
        let mut content_tick = tokio::time::interval(self.content_interval);
        let mut retry_tick = tokio::time::interval(self.retry_interval);
        let mut unreleased_tick = tokio::time::interval(self.unreleased_interval);

        loop {
            tokio::select! {
                _ = content_tick.tick() => {
                    tracing::debug!("triggering content service request");
                    self.job_queue.push_content_service().await;
                }
                _ = retry_tick.tick() => {
                    self.retry_library().await;
                }
                _ = unreleased_tick.tick() => {
                    self.check_unreleased().await;
                }
            }
        }
    }

    async fn retry_library(&self) {
        // Movies indexed → scrape
        let movies_indexed = self
            .fetch_ready(&self.db_pool, MediaItemState::Indexed, MediaItemType::Movie, 50, "indexed movies")
            .await;
        for movie in movies_indexed {
            self.job_queue
                .push_scrape(ScrapeJob {
                    id: movie.id,
                    item_type: movie.item_type,
                    imdb_id: movie.imdb_id.clone(),
                    title: movie.title.clone(),
                    season: None,
                    episode: None,
                })
                .await;
        }

        // Movies scraped → download (or reset to Indexed if streams exhausted)
        let movies_scraped = self
            .fetch_ready(&self.db_pool, MediaItemState::Scraped, MediaItemType::Movie, 20, "scraped movies")
            .await;
        for movie in movies_scraped {
            if let Some(stream) = repo::get_best_stream(&self.db_pool, movie.id)
                .await
                .ok()
                .flatten()
            {
                let magnet = format!("magnet:?xt=urn:btih:{}", stream.info_hash);
                self.job_queue
                    .push_download(DownloadJob {
                        id: movie.id,
                        info_hash: stream.info_hash.clone(),
                        magnet,
                    })
                    .await;
            } else {
                // All streams blacklisted — reset to Indexed so re-scrape picks it up.
                tracing::info!(id = movie.id, "scraped movie has no remaining streams; resetting to indexed for re-scrape");
                let _ = repo::update_media_item_state(&self.db_pool, movie.id, MediaItemState::Indexed).await;
            }
        }

        // Shows indexed → scrape each requested season
        let shows_indexed = self
            .fetch_ready(&self.db_pool, MediaItemState::Indexed, MediaItemType::Show, 20, "indexed shows")
            .await;
        for show in shows_indexed {
            let show_imdb_id = show.imdb_id.clone();
            match repo::get_requested_seasons_for_show(&self.db_pool, show.id).await {
                Ok(seasons) => {
                    for season in seasons {
                        self.job_queue
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
                Err(e) => tracing::error!(error = %e, show_id = show.id, "failed to fetch seasons for retry"),
            }
        }

        // Shows scraped → download each requested scraped season (reset to Indexed if streams exhausted)
        let shows_scraped = self
            .fetch_ready(&self.db_pool, MediaItemState::Scraped, MediaItemType::Show, 20, "scraped shows")
            .await;
        for show in shows_scraped {
            match repo::get_scraped_seasons_for_show(&self.db_pool, show.id).await {
                Ok(seasons) => {
                    for season in seasons {
                        if let Some(stream) = repo::get_best_stream(&self.db_pool, season.id)
                            .await
                            .ok()
                            .flatten()
                        {
                            let magnet = format!("magnet:?xt=urn:btih:{}", stream.info_hash);
                            self.job_queue
                                .push_download(DownloadJob {
                                    id: season.id,
                                    info_hash: stream.info_hash.clone(),
                                    magnet,
                                })
                                .await;
                        } else {
                            // All streams blacklisted — reset season to Indexed for re-scrape.
                            tracing::info!(id = season.id, "scraped season has no remaining streams; resetting to indexed for re-scrape");
                            let _ = repo::update_media_item_state(&self.db_pool, season.id, MediaItemState::Indexed).await;
                        }
                    }
                }
                Err(e) => tracing::error!(error = %e, show_id = show.id, "failed to fetch scraped seasons for retry"),
            }
        }

        // PartiallyCompleted shows → re-scrape indexed seasons and re-download scraped/partial seasons
        self.retry_partial_shows().await;

        // Ongoing items → retry download
        self.retry_ongoing().await;
    }

    async fn retry_partial_shows(&self) {
        let shows = self
            .fetch_ready(&self.db_pool, MediaItemState::PartiallyCompleted, MediaItemType::Show, 20, "partially completed shows")
            .await;

        for show in shows {
            match repo::get_indexed_seasons_for_show(&self.db_pool, show.id).await {
                Ok(seasons) => {
                    for season in seasons {
                        self.job_queue
                            .push_scrape(ScrapeJob {
                                id: season.id,
                                item_type: season.item_type,
                                imdb_id: show.imdb_id.clone(),
                                title: season.title.clone(),
                                season: season.season_number,
                                episode: None,
                            })
                            .await;
                    }
                }
                Err(e) => tracing::error!(error = %e, show_id = show.id, "failed to fetch indexed seasons for partial retry"),
            }

            match repo::get_retryable_seasons_for_show(&self.db_pool, show.id).await {
                Ok(seasons) => {
                    for season in seasons {
                        if let Some(stream) =
                            repo::get_best_stream(&self.db_pool, season.id).await.ok().flatten()
                        {
                            let magnet = format!("magnet:?xt=urn:btih:{}", stream.info_hash);
                            self.job_queue
                                .push_download(DownloadJob {
                                    id: season.id,
                                    info_hash: stream.info_hash.clone(),
                                    magnet,
                                })
                                .await;
                        }
                    }
                }
                Err(e) => tracing::error!(error = %e, show_id = show.id, "failed to fetch retryable seasons for partial retry"),
            }
        }
    }

    async fn retry_ongoing(&self) {
        for item_type in [MediaItemType::Movie, MediaItemType::Season] {
            let items = match repo::get_stuck_ongoing_items(&self.db_pool, item_type, 10, 20).await {
                Ok(items) => items,
                Err(e) => {
                    tracing::error!(error = %e, "failed to fetch stuck ongoing items");
                    vec![]
                }
            };
            for item in items {
                if let Some(stream) = repo::get_best_stream(&self.db_pool, item.id)
                    .await
                    .ok()
                    .flatten()
                {
                    let magnet = format!("magnet:?xt=urn:btih:{}", stream.info_hash);
                    self.job_queue
                        .push_download(DownloadJob {
                            id: item.id,
                            info_hash: stream.info_hash.clone(),
                            magnet,
                        })
                        .await;
                } else if item.item_type == MediaItemType::Season {
                    if let Ok(computed) = repo::compute_state(&self.db_pool, &item).await {
                        if computed != MediaItemState::Ongoing {
                            let _ =
                                repo::update_media_item_state(&self.db_pool, item.id, computed)
                                    .await;
                        }
                    }
                } else {
                    let _ = repo::update_media_item_state(
                        &self.db_pool,
                        item.id,
                        MediaItemState::Scraped,
                    )
                    .await;
                }
            }
        }
    }

    async fn check_unreleased(&self) {
        match repo::transition_unreleased_aired(&self.db_pool).await {
            Ok(n) if n > 0 => tracing::info!(count = n, "transitioned unreleased items to indexed"),
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "failed to transition unreleased items"),
        }

        let shows = self
            .fetch_ready(&self.db_pool, MediaItemState::Ongoing, MediaItemType::Show, 50, "ongoing shows")
            .await;

        for show in shows {
            tracing::info!(id = show.id, title = %show.title, "re-indexing ongoing show for new episodes");
            self.job_queue
                .push_index(IndexJob {
                    id: show.id,
                    item_type: show.item_type,
                    imdb_id: show.imdb_id.clone(),
                    tvdb_id: show.tvdb_id.clone(),
                    tmdb_id: show.tmdb_id.clone(),
                })
                .await;
        }
    }

    async fn fetch_ready(
        &self,
        pool: &sqlx::PgPool,
        state: MediaItemState,
        item_type: MediaItemType,
        limit: i64,
        label: &str,
    ) -> Vec<MediaItem> {
        match repo::get_items_ready_for_processing(pool, state, item_type, limit).await {
            Ok(items) => items,
            Err(e) => {
                tracing::error!(error = %e, "failed to fetch {} for retry", label);
                vec![]
            }
        }
    }
}
