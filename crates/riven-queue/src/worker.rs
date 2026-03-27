use std::sync::Arc;
use std::time::Duration;

use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::{IndexJob, JobQueue, ScrapeJob};

/// Periodic scheduler.
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

    /// Retry pending top-level items.
    async fn retry_library(&self) {
        for item_type in [MediaItemType::Movie, MediaItemType::Show] {
            let items = match repo::get_pending_items_for_retry(&self.db_pool, item_type, 50).await {
                Ok(items) => items,
                Err(e) => { tracing::error!(error = %e, "failed to fetch pending items for retry"); vec![] }
            };

            for item in items {
                match item.state {
                    MediaItemState::Indexed if item.indexed_at.is_none() => {
                        self.job_queue.push_index(IndexJob::from_item(&item)).await;
                    }
                    MediaItemState::Indexed | MediaItemState::PartiallyCompleted => {
                        self.push_scrape(&item).await;
                    }
                    MediaItemState::Scraped => {
                        self.push_download(&item).await;
                    }
                    _ => {}
                }
            }
        }

        self.retry_ongoing().await;
    }

    /// Retry items stuck in Ongoing (partially completed with some unreleased episodes).
    async fn retry_ongoing(&self) {
        for item_type in [MediaItemType::Movie, MediaItemType::Season] {
            let items = match repo::get_stuck_ongoing_items(&self.db_pool, item_type, 10, 20).await {
                Ok(items) => items,
                Err(e) => { tracing::error!(error = %e, "failed to fetch stuck ongoing items"); vec![] }
            };
            for item in &items {
                if !self.job_queue.push_download_from_best_stream(item.id).await {
                    let _ = repo::refresh_state_cascade(&self.db_pool, item).await;
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

    async fn push_scrape(&self, item: &MediaItem) {
        match item.item_type {
            MediaItemType::Movie => {
                self.job_queue.push_scrape(ScrapeJob::for_movie(item)).await;
            }
            MediaItemType::Show => {
                match repo::get_requested_seasons_for_show(&self.db_pool, item.id).await {
                    Ok(seasons) => {
                        for season in seasons {
                            if season.state == MediaItemState::PartiallyCompleted {
                                // Season pack exists but some episodes missing — scrape episodes individually.
                                match repo::get_incomplete_episodes_for_season(&self.db_pool, season.id).await {
                                    Ok(episodes) => {
                                        for ep in episodes {
                                            self.job_queue.push_scrape(ScrapeJob {
                                                id: ep.id,
                                                item_type: ep.item_type,
                                                imdb_id: item.imdb_id.clone(),
                                                title: item.title.clone(),
                                                season: ep.season_number,
                                                episode: ep.episode_number,
                                            }).await;
                                        }
                                    }
                                    Err(e) => tracing::error!(error = %e, season_id = season.id, "failed to fetch incomplete episodes"),
                                }
                            } else {
                                self.job_queue.push_scrape(
                                    ScrapeJob::for_season(&season, item.title.clone(), item.imdb_id.clone())
                                ).await;
                            }
                        }
                    }
                    Err(e) => tracing::error!(error = %e, show_id = item.id, "failed to fetch seasons for scrape"),
                }
            }
            _ => {}
        }
    }

    /// Push download for movies directly; for shows, fan out to scraped seasons.
    async fn push_download(&self, item: &MediaItem) {
        match item.item_type {
            MediaItemType::Movie => {
                if !self.job_queue.push_download_from_best_stream(item.id).await {
                    let _ = repo::refresh_state_cascade(&self.db_pool, item).await;
                }
            }
            MediaItemType::Show => {
                match repo::get_scraped_seasons_for_show(&self.db_pool, item.id).await {
                    Ok(seasons) => {
                        for season in &seasons {
                            if !self.job_queue.push_download_from_best_stream(season.id).await {
                                let _ = repo::refresh_state_cascade(&self.db_pool, season).await;
                            }
                        }
                    }
                    Err(e) => tracing::error!(error = %e, show_id = item.id, "failed to fetch scraped seasons"),
                }
            }
            _ => {}
        }
    }
}
