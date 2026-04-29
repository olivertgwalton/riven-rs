//! Lifecycle helpers — request upsert, retry of a request, request-state sync.
//!
//! All event-driven orchestration lives in `main_orchestrator::MainOrchestrator`;
//! this module just holds the request-DB helpers callers (and
//! `MainOrchestrator`) compose.

use anyhow::Result;

use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::entities::{ItemRequest, MediaItem};
use riven_db::repo::{self, ItemRequestUpsertAction};

use crate::{IndexJob, JobQueue, ProcessMediaItemJob};

pub struct RequestedItemOutcome {
    pub item: MediaItem,
    pub request: ItemRequest,
    pub action: ItemRequestUpsertAction,
}

impl RequestedItemOutcome {
    pub fn lifecycle_event(&self, requested_seasons: Option<&[i32]>) -> Option<RivenEvent> {
        let requested_seasons = requested_seasons.map(|seasons| seasons.to_vec());
        match self.action {
            ItemRequestUpsertAction::Created => Some(RivenEvent::ItemRequestCreated {
                request_id: self.request.id,
                item_id: self.item.id,
                request_type: self.request.request_type,
                requested_seasons,
            }),
            ItemRequestUpsertAction::Updated => Some(RivenEvent::ItemRequestUpdated {
                request_id: self.request.id,
                item_id: self.item.id,
                request_type: self.request.request_type,
                requested_seasons,
            }),
            ItemRequestUpsertAction::Unchanged => None,
        }
    }
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
            request: request.request,
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
            request: request.request,
            action: request.action,
        })
    }

    pub async fn enqueue_after_request_action(
        &self,
        item: &MediaItem,
        action: ItemRequestUpsertAction,
        requested_seasons: Option<&[i32]>,
    ) {
        match item.item_type {
            MediaItemType::Movie if action == ItemRequestUpsertAction::Created => {
                self.queue.push_index(IndexJob::from_item(item)).await;
            }
            MediaItemType::Movie => {}
            MediaItemType::Show => match action {
                ItemRequestUpsertAction::Created => {
                    self.queue.push_index(IndexJob::from_item(item)).await;
                }
                ItemRequestUpsertAction::Updated => {
                    let requested_specific_seasons =
                        requested_seasons.is_some_and(|seasons| !seasons.is_empty());

                    if item.imdb_id.is_none() || requested_specific_seasons {
                        self.queue.push_index(IndexJob::from_item(item)).await;
                    } else {
                        // Already indexed and no new seasons added — kick the
                        // per-item state machine. ProcessMediaItem.handle_scrape
                        // for a Show fans out to its requested seasons.
                        self.queue
                            .push_process_media_item(ProcessMediaItemJob::new(item.id))
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
}
