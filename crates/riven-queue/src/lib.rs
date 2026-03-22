pub mod flows;
pub mod worker;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::plugin::PluginRegistry;

/// File-size / bitrate filtering applied after a download response is received.
///
/// Minimum size is computed as `runtime_seconds × bitrate_kbps × 125` (bytes).
/// If either field is `None` or the item has no runtime, the check is skipped.
#[derive(Clone, Default)]
pub struct DownloaderConfig {
    /// Minimum average bitrate for movies (kbps). `None` = disabled.
    pub minimum_average_bitrate_movies: Option<u32>,
    /// Minimum average bitrate for episodes (kbps). `None` = disabled.
    pub minimum_average_bitrate_episodes: Option<u32>,
}

impl DownloaderConfig {
    /// Returns `true` if the file passes the movie bitrate gate.
    pub fn movie_passes(&self, file_size: u64, runtime_minutes: Option<i32>) -> bool {
        let Some(kbps) = self.minimum_average_bitrate_movies else {
            return true;
        };
        let Some(mins) = runtime_minutes else {
            return true; // can't compute without runtime
        };
        let min_bytes = mins as u64 * 60 * kbps as u64 * 125;
        file_size >= min_bytes
    }

    /// Returns `true` if the file passes the episode bitrate gate.
    pub fn episode_passes(&self, file_size: u64, runtime_minutes: Option<i32>) -> bool {
        let Some(kbps) = self.minimum_average_bitrate_episodes else {
            return true;
        };
        let Some(mins) = runtime_minutes else {
            return true;
        };
        let min_bytes = mins as u64 * 60 * kbps as u64 * 125;
        file_size >= min_bytes
    }
}

/// The main event bus that distributes events to the plugin system and flow workers.
pub struct EventBus {
    event_tx: mpsc::Sender<RivenEvent>,
}

impl EventBus {
    pub fn new(
        registry: Arc<PluginRegistry>,
        db_pool: sqlx::PgPool,
        downloader_config: DownloaderConfig,
    ) -> (Self, EventBusHandle) {
        let (event_tx, event_rx) = mpsc::channel(1024);
        let handle = EventBusHandle {
            event_rx,
            registry,
            db_pool,
            downloader_config,
        };
        (Self { event_tx }, handle)
    }

    pub async fn publish(&self, event: RivenEvent) -> Result<()> {
        self.event_tx.send(event).await?;
        Ok(())
    }

    pub fn publisher(&self) -> mpsc::Sender<RivenEvent> {
        self.event_tx.clone()
    }
}

pub struct EventBusHandle {
    event_rx: mpsc::Receiver<RivenEvent>,
    registry: Arc<PluginRegistry>,
    db_pool: sqlx::PgPool,
    downloader_config: DownloaderConfig,
}

impl EventBusHandle {
    /// Run the event loop, dispatching events to plugins and processing flows.
    pub async fn run(mut self, event_tx: mpsc::Sender<RivenEvent>) -> Result<()> {
        tracing::info!("event bus started");

        while let Some(event) = self.event_rx.recv().await {
            let event_type = event.event_type();
            tracing::debug!(?event_type, "processing event");

            // Dispatch to plugins
            let results = self.registry.dispatch(&event).await;

            for (plugin_name, result) in results {
                match result {
                    Ok(response) => {
                        // Process hook responses into follow-up events
                        if let Some(follow_up) = self
                            .process_hook_response(&event, plugin_name, response)
                            .await
                        {
                            if let Err(e) = event_tx.send(follow_up).await {
                                tracing::error!(error = %e, "failed to send follow-up event");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(plugin = plugin_name, error = %e, "plugin hook failed");
                    }
                }
            }

            // Run flow processors for certain events
            self.run_flow(&event, &event_tx).await;
        }

        Ok(())
    }

    async fn process_hook_response(
        &self,
        _event: &RivenEvent,
        _plugin_name: &str,
        _response: HookResponse,
    ) -> Option<RivenEvent> {
        // Hook responses generate follow-up events
        // The flow system handles aggregation
        None
    }

    async fn run_flow(&self, event: &RivenEvent, event_tx: &mpsc::Sender<RivenEvent>) {
        match event {
            RivenEvent::ContentServiceRequested => {
                flows::request_content::run(&self.registry, &self.db_pool, event_tx).await;
            }
            RivenEvent::MediaItemIndexRequested { id, .. } => {
                flows::index_item::run(*id, &self.registry, &self.db_pool, event_tx).await;
            }
            RivenEvent::MediaItemScrapeRequested { id, .. } => {
                flows::scrape_item::run(
                    *id,
                    event,
                    &self.registry,
                    &self.db_pool,
                    event_tx,
                )
                .await;
            }
            RivenEvent::MediaItemDownloadRequested { id, .. } => {
                flows::download_item::run(
                    *id,
                    event,
                    &self.registry,
                    &self.db_pool,
                    event_tx,
                    &self.downloader_config,
                )
                .await;
            }
            RivenEvent::MediaItemIndexSuccess { id, .. } => {
                // Immediately queue scraping after successful indexing
                if let Ok(Some(item)) = riven_db::repo::get_media_item(&self.db_pool, *id).await {
                    match item.item_type {
                        riven_core::types::MediaItemType::Movie | riven_core::types::MediaItemType::Episode => {
                            let _ = event_tx
                                .send(RivenEvent::MediaItemScrapeRequested {
                                    id: item.id,
                                    item_type: item.item_type,
                                    imdb_id: item.imdb_id.clone(),
                                    title: item.title.clone(),
                                    season: item.season_number,
                                    episode: item.episode_number,
                                })
                                .await;
                        }
                        riven_core::types::MediaItemType::Show => {
                            // After a show is indexed, queue scraping for each requested season
                            // (riven-ts: requestScrape → requestScrape.actor → fans out to requestedSeasons).
                            let show_imdb_id = item.imdb_id.clone();
                            if let Ok(seasons) = riven_db::repo::get_requested_seasons_for_show(&self.db_pool, item.id).await {
                                for season in seasons {
                                    let _ = event_tx
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
                        }
                        _ => {}
                    }
                }
            }
            RivenEvent::MediaItemScrapeSuccess { id, .. } => {
                // Immediately queue download after successful scraping
                if let Ok(Some(stream)) = riven_db::repo::get_best_stream(&self.db_pool, *id).await
                {
                    let magnet = format!("magnet:?xt=urn:btih:{}", stream.info_hash);
                    let _ = event_tx
                        .send(RivenEvent::MediaItemDownloadRequested {
                            id: *id,
                            info_hash: stream.info_hash.clone(),
                            magnet,
                        })
                        .await;
                }
            }

            RivenEvent::MediaItemScrapeErrorNoNewStreams { id, .. } => {
                // No streams found — fan out to lower-level scraping as fallback
                // (riven-ts: scrape.error.no-new-streams → fanOutDownload).
                self.fan_out_download(*id, event_tx).await;
            }

            RivenEvent::MediaItemDownloadPartialSuccess { id } => {
                // Stream was already blacklisted in the download flow.
                // Fan out to re-scrape at a lower level (riven-ts: download.partial-success → fanOutDownload).
                self.fan_out_download(*id, event_tx).await;
            }

            RivenEvent::MediaItemDownloadError { id, .. } => {
                // Transient failure — fan out to re-scrape (riven-ts: download.error → fanOutDownload).
                self.fan_out_download(*id, event_tx).await;
            }

            _ => {}
        }
    }

    /// Fan out to re-scrape at a lower level when scraping/downloading fails.
    /// Mirrors riven-ts fanOutDownload.actor:
    ///   Show   → request scrape for each requested season
    ///   Season → request scrape for each incomplete (indexed/scraped/ongoing) episode
    ///   Other  → no-op (item stays in current state; retryLibrary will pick it up)
    async fn fan_out_download(&self, id: i64, event_tx: &mpsc::Sender<RivenEvent>) {
        let item = match riven_db::repo::get_media_item(&self.db_pool, id).await {
            Ok(Some(item)) => item,
            _ => return,
        };

        match item.item_type {
            riven_core::types::MediaItemType::Show => {
                let show_imdb_id = item.imdb_id.clone();
                if let Ok(seasons) = riven_db::repo::get_requested_seasons_for_show(&self.db_pool, id).await {
                    for season in seasons {
                        let _ = event_tx
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
            }
            riven_core::types::MediaItemType::Season => {
                let show_imdb_id = if let Some(show_id) = item.parent_id {
                    riven_db::repo::get_media_item(&self.db_pool, show_id)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|s| s.imdb_id)
                } else {
                    None
                };
                if let Ok(episodes) = riven_db::repo::get_incomplete_episodes_for_season(&self.db_pool, id).await {
                    for ep in episodes {
                        let _ = event_tx
                            .send(RivenEvent::MediaItemScrapeRequested {
                                id: ep.id,
                                item_type: ep.item_type,
                                imdb_id: show_imdb_id.clone(),
                                title: ep.title.clone(),
                                season: ep.season_number,
                                episode: ep.episode_number,
                            })
                            .await;
                    }
                }
            }
            _ => {}
        }
    }
}
