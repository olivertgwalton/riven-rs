use std::sync::Arc;
use std::time::Duration;

use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::{IndexJob, JobQueue, ScrapeJob};

/// Periodic scheduler: content service every 120 s, library retry every 60 s,
/// unreleased check once per day.
pub struct Scheduler {
    db_pool: sqlx::PgPool,
    job_queue: Arc<JobQueue>,
}

impl Scheduler {
    pub fn new(db_pool: sqlx::PgPool, job_queue: Arc<JobQueue>) -> Self {
        Self { db_pool, job_queue }
    }

    pub async fn run(self) {
        let mut content_tick    = tokio::time::interval(Duration::from_secs(120));
        let mut retry_tick      = tokio::time::interval(Duration::from_secs(60));
        let mut unreleased_tick = tokio::time::interval(Duration::from_secs(86400));

        loop {
            tokio::select! {
                _ = content_tick.tick()    => self.job_queue.push_content_service().await,
                _ = retry_tick.tick()      => self.retry_library().await,
                _ = unreleased_tick.tick() => self.check_unreleased().await,
            }
        }
    }

    async fn retry_library(&self) {
        // Indexed movies → re-index if fresh, else scrape
        for movie in self.fetch_ready(MediaItemState::Indexed, MediaItemType::Movie, 50, "indexed movies").await {
            if movie.indexed_at.is_none() {
                self.job_queue.push_index(IndexJob::from_item(&movie)).await;
            } else {
                self.job_queue.push_scrape(ScrapeJob::for_movie(&movie)).await;
            }
        }

        // Scraped movies → download or reset
        for movie in self.fetch_ready(MediaItemState::Scraped, MediaItemType::Movie, 20, "scraped movies").await {
            if !self.job_queue.push_download_from_best_stream(movie.id).await {
                tracing::info!(id = movie.id, "no remaining streams; resetting to indexed");
                let _ = repo::update_media_item_state(&self.db_pool, movie.id, MediaItemState::Indexed).await;
            }
        }

        // Indexed shows → re-index if fresh, else scrape requested seasons
        for show in self.fetch_ready(MediaItemState::Indexed, MediaItemType::Show, 20, "indexed shows").await {
            if show.indexed_at.is_none() {
                self.job_queue.push_index(IndexJob::from_item(&show)).await;
            } else {
                self.push_scrape_seasons_for_show(&show, "fetch seasons for retry").await;
            }
        }

        // Scraped shows → download each scraped season or reset
        for show in self.fetch_ready(MediaItemState::Scraped, MediaItemType::Show, 20, "scraped shows").await {
            match repo::get_scraped_seasons_for_show(&self.db_pool, show.id).await {
                Ok(seasons) => {
                    for season in seasons {
                        if !self.job_queue.push_download_from_best_stream(season.id).await {
                            tracing::info!(id = season.id, "no remaining streams; resetting to indexed");
                            let _ = repo::update_media_item_state(&self.db_pool, season.id, MediaItemState::Indexed).await;
                        }
                    }
                }
                Err(e) => tracing::error!(error = %e, show_id = show.id, "failed to fetch scraped seasons"),
            }
        }

        self.retry_partial_shows().await;
        self.retry_ongoing().await;
    }

    async fn retry_partial_shows(&self) {
        for show in self.fetch_ready(MediaItemState::PartiallyCompleted, MediaItemType::Show, 20, "partially completed shows").await {
            self.push_scrape_seasons_for_show_indexed(&show).await;

            match repo::get_retryable_seasons_for_show(&self.db_pool, show.id).await {
                Ok(seasons) => {
                    for season in seasons {
                        self.job_queue.push_download_from_best_stream(season.id).await;
                        // Also fan out to individually scrape any episodes still at Indexed
                        // within this season (e.g. double-episode finales that the pack
                        // download couldn't match).
                        self.job_queue.fan_out_download(season.id).await;
                    }
                }
                Err(e) => tracing::error!(error = %e, show_id = show.id, "failed to fetch retryable seasons"),
            }
        }
    }

    async fn retry_ongoing(&self) {
        for item_type in [MediaItemType::Movie, MediaItemType::Season] {
            let items = match repo::get_stuck_ongoing_items(&self.db_pool, item_type, 10, 20).await {
                Ok(items) => items,
                Err(e) => { tracing::error!(error = %e, "failed to fetch stuck ongoing items"); vec![] }
            };
            for item in items {
                if !self.job_queue.push_download_from_best_stream(item.id).await {
                    if item.item_type == MediaItemType::Season {
                        if let Ok(computed) = repo::compute_state(&self.db_pool, &item).await {
                            if computed != MediaItemState::Ongoing {
                                let _ = repo::update_media_item_state(&self.db_pool, item.id, computed).await;
                            }
                        }
                    } else {
                        let _ = repo::update_media_item_state(&self.db_pool, item.id, MediaItemState::Scraped).await;
                    }
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

        for show in self.fetch_ready(MediaItemState::Ongoing, MediaItemType::Show, 50, "ongoing shows").await {
            tracing::info!(id = show.id, title = %show.title, "re-indexing ongoing show for new episodes");
            self.job_queue.push_index(IndexJob::from_item(&show)).await;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    async fn fetch_ready(&self, state: MediaItemState, item_type: MediaItemType, limit: i64, label: &str) -> Vec<MediaItem> {
        match repo::get_items_ready_for_processing(&self.db_pool, state, item_type, limit).await {
            Ok(items) => items,
            Err(e) => { tracing::error!(error = %e, "failed to fetch {} for retry", label); vec![] }
        }
    }

    /// Push a ScrapeJob for every requested season of a show.
    async fn push_scrape_seasons_for_show(&self, show: &MediaItem, err_label: &str) {
        match repo::get_requested_seasons_for_show(&self.db_pool, show.id).await {
            Ok(seasons) => {
                for season in seasons {
                    self.job_queue.push_scrape(ScrapeJob::for_season(&season, show.title.clone(), show.imdb_id.clone())).await;
                }
            }
            Err(e) => tracing::error!(error = %e, show_id = show.id, "{err_label}"),
        }
    }

    /// Push a ScrapeJob for every indexed season of a show.
    async fn push_scrape_seasons_for_show_indexed(&self, show: &MediaItem) {
        match repo::get_indexed_seasons_for_show(&self.db_pool, show.id).await {
            Ok(seasons) => {
                for season in seasons {
                    self.job_queue.push_scrape(ScrapeJob::for_season(&season, show.title.clone(), show.imdb_id.clone())).await;
                }
            }
            Err(e) => tracing::error!(error = %e, show_id = show.id, "failed to fetch indexed seasons"),
        }
    }
}
