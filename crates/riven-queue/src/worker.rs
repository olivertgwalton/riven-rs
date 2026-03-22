use std::time::Duration;

use tokio::sync::mpsc;

use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

/// Periodic scheduler that triggers content service requests and retries stuck items.
/// Mirrors riven-ts: content service every 120 s, retryLibrary every 60 s.
pub struct Scheduler {
    db_pool: sqlx::PgPool,
    event_tx: mpsc::Sender<RivenEvent>,
    content_interval: Duration,
    retry_interval: Duration,
}

impl Scheduler {
    pub fn new(
        db_pool: sqlx::PgPool,
        event_tx: mpsc::Sender<RivenEvent>,
    ) -> Self {
        Self {
            db_pool,
            event_tx,
            content_interval: Duration::from_secs(120), // 2 minutes (riven-ts: 120_000 ms)
            retry_interval: Duration::from_secs(60),    // 1 minute  (riven-ts: 60_000 ms)
        }
    }

    pub async fn run(self) {
        let mut content_tick = tokio::time::interval(self.content_interval);
        let mut retry_tick = tokio::time::interval(self.retry_interval);

        loop {
            tokio::select! {
                _ = content_tick.tick() => {
                    tracing::debug!("triggering content service request");
                    let _ = self.event_tx.send(RivenEvent::ContentServiceRequested).await;
                }
                _ = retry_tick.tick() => {
                    self.retry_library().await;
                }
            }
        }
    }

    /// Mirrors riven-ts retryLibrary.actor:
    ///   - Movies  indexed  → request scrape
    ///   - Movies  scraped  → request download
    ///   - Shows   indexed  → request scrape for each requested season
    ///   - Shows   scraped  → request download for each requested scraped season
    /// Also transitions unreleased items that have since aired back to indexed.
    async fn retry_library(&self) {
        // Transition unreleased items that have aired
        match repo::transition_unreleased_aired(&self.db_pool).await {
            Ok(n) if n > 0 => tracing::info!(count = n, "transitioned unreleased items to indexed"),
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "failed to transition unreleased items"),
        }

        // Movies indexed → scrape
        let movies_indexed = self
            .fetch_ready(&self.db_pool, MediaItemState::Indexed, MediaItemType::Movie, 50, "indexed movies")
            .await;
        for movie in movies_indexed {
            let _ = self
                .event_tx
                .send(RivenEvent::MediaItemScrapeRequested {
                    id: movie.id,
                    item_type: movie.item_type,
                    imdb_id: movie.imdb_id.clone(),
                    title: movie.title.clone(),
                    season: None,
                    episode: None,
                })
                .await;
        }

        // Movies scraped → download
        let movies_scraped = self
            .fetch_ready(&self.db_pool, MediaItemState::Scraped, MediaItemType::Movie, 20, "scraped movies")
            .await;
        for movie in movies_scraped {
            if let Some(stream) = repo::get_best_stream(&self.db_pool, movie.id).await.ok().flatten()
            {
                let magnet = format!("magnet:?xt=urn:btih:{}", stream.info_hash);
                let _ = self
                    .event_tx
                    .send(RivenEvent::MediaItemDownloadRequested {
                        id: movie.id,
                        info_hash: stream.info_hash.clone(),
                        magnet,
                    })
                    .await;
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
                        let _ = self
                            .event_tx
                            .send(RivenEvent::MediaItemScrapeRequested {
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

        // Shows scraped → download each requested scraped season
        let shows_scraped = self
            .fetch_ready(&self.db_pool, MediaItemState::Scraped, MediaItemType::Show, 20, "scraped shows")
            .await;
        for show in shows_scraped {
            match repo::get_scraped_seasons_for_show(&self.db_pool, show.id).await {
                Ok(seasons) => {
                    for season in seasons {
                        if let Some(stream) = repo::get_best_stream(&self.db_pool, season.id).await.ok().flatten()
                        {
                            let magnet = format!("magnet:?xt=urn:btih:{}", stream.info_hash);
                            let _ = self
                                .event_tx
                                .send(RivenEvent::MediaItemDownloadRequested {
                                    id: season.id,
                                    info_hash: stream.info_hash.clone(),
                                    magnet,
                                })
                                .await;
                        }
                    }
                }
                Err(e) => tracing::error!(error = %e, show_id = show.id, "failed to fetch scraped seasons for retry"),
            }
        }

        // Ongoing items → retry download (torrent added but files not yet confirmed)
        self.retry_ongoing().await;
    }

    /// Retry download for items in the Ongoing state.
    /// These had a download initiated but the session may have been lost (e.g. restart).
    async fn retry_ongoing(&self) {
        for item_type in [MediaItemType::Movie, MediaItemType::Season] {
            let items = self
                .fetch_ready(&self.db_pool, MediaItemState::Ongoing, item_type, 20, "ongoing items")
                .await;
            for item in items {
                if let Some(stream) = repo::get_best_stream(&self.db_pool, item.id).await.ok().flatten()
                {
                    let magnet = format!("magnet:?xt=urn:btih:{}", stream.info_hash);
                    let _ = self
                        .event_tx
                        .send(RivenEvent::MediaItemDownloadRequested {
                            id: item.id,
                            info_hash: stream.info_hash.clone(),
                            magnet,
                        })
                        .await;
                } else {
                    // No streams left — revert to Scraped so it can try scraping again
                    let _ = repo::update_media_item_state(&self.db_pool, item.id, MediaItemState::Scraped).await;
                }
            }
        }
    }

    /// Fetch items ready for processing, logging an error and returning an empty
    /// vec on failure so callers can always iterate without nested match arms.
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
