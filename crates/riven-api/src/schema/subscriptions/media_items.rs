use async_graphql::{Context, Subscription};
use futures::stream::{self, Stream, StreamExt};
use riven_core::events::RivenEvent;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;
use riven_db::repo;
use std::sync::Arc;

use super::super::queries::MediaQuery;
use super::super::typed_items::Show;
use super::super::types::MediaItemStateTree;
use super::broadcast_stream;

fn event_item_id(event: &RivenEvent) -> Option<i64> {
    match event {
        RivenEvent::ItemRequestCreated { item_id: id, .. }
        | RivenEvent::ItemRequestUpdated { item_id: id, .. }
        | RivenEvent::MediaItemIndexRequested { id, .. }
        | RivenEvent::MediaItemIndexSuccess { id, .. }
        | RivenEvent::MediaItemIndexError { id, .. }
        | RivenEvent::MediaItemIndexErrorIncorrectState { id }
        | RivenEvent::MediaItemScrapeRequested { id, .. }
        | RivenEvent::MediaItemScrapeSuccess { id, .. }
        | RivenEvent::MediaItemScrapeError { id, .. }
        | RivenEvent::MediaItemScrapeErrorIncorrectState { id }
        | RivenEvent::MediaItemScrapeErrorNoNewStreams { id, .. }
        | RivenEvent::MediaItemDownloadRequested { id, .. }
        | RivenEvent::MediaItemDownloadError { id, .. }
        | RivenEvent::MediaItemDownloadErrorIncorrectState { id }
        | RivenEvent::MediaItemDownloadPartialSuccess { id }
        | RivenEvent::MediaItemDownloadSuccess { id, .. } => Some(*id),
        _ => None,
    }
}

async fn item_relates_to_target(item_id: i64, target_id: i64) -> bool {
    repo::is_item_descendant_of(item_id, target_id)
        .await
        .unwrap_or(false)
}

async fn should_emit_for_external_target(event: &RivenEvent, target: &MediaItem) -> bool {
    if let Some(event_id) = event_item_id(event) {
        return item_relates_to_target(event_id, target.id).await;
    }
    matches!(
        target.item_type,
        MediaItemType::Movie | MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode
    )
}

async fn wait_for_relevant_event(
    rx: &mut tokio::sync::broadcast::Receiver<RivenEvent>,
    target: &MediaItem,
) -> Option<()> {
    loop {
        let event = match rx.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
        };

        if should_emit_for_external_target(&event, target).await {
            while rx.try_recv().is_ok() {}
            return Some(());
        }
    }
}

fn event_stream<T>(
    rx: tokio::sync::broadcast::Receiver<RivenEvent>,
    select: impl Fn(RivenEvent) -> Option<T> + Copy,
) -> impl Stream<Item = async_graphql::Result<T>> {
    broadcast_stream(rx).filter_map(move |event| async move { select(event).map(Ok) })
}

#[derive(Clone)]
enum ExternalId {
    Tmdb(String),
    Tvdb(String),
}

impl ExternalId {
    async fn load_item(&self) -> anyhow::Result<Option<MediaItem>> {
        match self {
            Self::Tmdb(id) => repo::get_media_item_by_tmdb(id).await,
            Self::Tvdb(id) => repo::get_media_item_by_tvdb(id).await,
        }
    }

    async fn load_state(&self) -> async_graphql::Result<Option<MediaItemStateTree>> {
        let Some(item) = self.load_item().await? else {
            return Ok(None);
        };
        MediaQuery.media_item_state_tree_inner(item).await.map(Some)
    }
}

fn state_updates(
    rx: tokio::sync::broadcast::Receiver<RivenEvent>,
    external_id: ExternalId,
) -> impl Stream<Item = async_graphql::Result<Option<MediaItemStateTree>>> {
    stream::unfold((rx, external_id), |(mut rx, external_id)| async move {
        loop {
            let current_item = match external_id.load_item().await {
                Ok(item) => item,
                Err(error) => return Some((Err(error.into()), (rx, external_id))),
            };
            let item_existed = current_item.is_some();

            match current_item.as_ref() {
                Some(item) => wait_for_relevant_event(&mut rx, item).await?,
                None if rx.recv().await.is_err() => return None,
                None => {}
            }

            let next = external_id.load_state().await;
            if item_existed || !matches!(next, Ok(None)) {
                return Some((next, (rx, external_id)));
            }
        }
    })
}

#[derive(Default)]
pub struct MediaItemsSubscription;

#[Subscription]
impl MediaItemsSubscription {
    /// Fires when a show has been indexed (metadata and episode structure persisted).
    async fn show_indexed(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<Show>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(|event| async move {
                let RivenEvent::MediaItemIndexSuccess { id, item_type, .. } = event else {
                    return None;
                };
                if item_type != MediaItemType::Show {
                    return None;
                }
                match repo::get_media_item(id).await {
                    Ok(Some(item)) => Some(Ok(Show { item })),
                    Ok(None) => None,
                    Err(error) => Some(Err(error.into())),
                }
            }),
        )
    }

    /// Fires when a media item transitions to the scraped state.
    async fn item_scraped(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<i64>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(event_stream(
            queue.event_tx.subscribe(),
            |event| match event {
                RivenEvent::MediaItemScrapeSuccess { id, .. } => Some(id),
                _ => None,
            },
        ))
    }

    /// Fires when a media item transitions to the completed state.
    async fn item_downloaded(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<i64>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(event_stream(
            queue.event_tx.subscribe(),
            |event| match event {
                RivenEvent::MediaItemDownloadSuccess { id, .. } => Some(id),
                _ => None,
            },
        ))
    }

    /// Fires when a media item transitions to the failed state (scrape or download error).
    async fn item_failed(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<i64>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(event_stream(
            queue.event_tx.subscribe(),
            |event| match event {
                RivenEvent::MediaItemScrapeError { id, .. }
                | RivenEvent::MediaItemScrapeErrorNoNewStreams { id, .. }
                | RivenEvent::MediaItemDownloadError { id, .. }
                | RivenEvent::MediaItemDownloadPartialSuccess { id } => Some(id),
                _ => None,
            },
        ))
    }

    /// Fires when one or more media items are deleted.
    async fn items_deleted(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<Vec<i64>>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(event_stream(
            queue.event_tx.subscribe(),
            |event| match event {
                RivenEvent::MediaItemsDeleted { item_ids, .. } => Some(item_ids),
                _ => None,
            },
        ))
    }

    async fn media_item_state_updates_by_tmdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<Option<MediaItemStateTree>>>>
    {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(state_updates(
            queue.event_tx.subscribe(),
            ExternalId::Tmdb(tmdb_id),
        ))
    }

    async fn media_item_state_updates_by_tvdb(
        &self,
        ctx: &Context<'_>,
        tvdb_id: String,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<Option<MediaItemStateTree>>>>
    {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(state_updates(
            queue.event_tx.subscribe(),
            ExternalId::Tvdb(tvdb_id),
        ))
    }
}
