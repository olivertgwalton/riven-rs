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

async fn item_relates_to_target(pool: &sqlx::PgPool, item_id: i64, target_id: i64) -> bool {
    repo::is_item_descendant_of(pool, item_id, target_id)
        .await
        .unwrap_or(false)
}

async fn load_item_state_by_tmdb(
    pool: &sqlx::PgPool,
    tmdb_id: &str,
) -> async_graphql::Result<Option<MediaItemStateTree>> {
    let Some(item) = repo::get_media_item_by_tmdb(pool, tmdb_id).await? else {
        return Ok(None);
    };
    MediaQuery
        .media_item_state_tree_inner(pool, item)
        .await
        .map(Some)
}

async fn load_item_state_by_tvdb(
    pool: &sqlx::PgPool,
    tvdb_id: &str,
) -> async_graphql::Result<Option<MediaItemStateTree>> {
    let Some(item) = repo::get_media_item_by_tvdb(pool, tvdb_id).await? else {
        return Ok(None);
    };
    MediaQuery
        .media_item_state_tree_inner(pool, item)
        .await
        .map(Some)
}

async fn should_emit_for_external_target(
    pool: &sqlx::PgPool,
    event: &RivenEvent,
    target: &MediaItem,
) -> bool {
    if let Some(event_id) = event_item_id(event) {
        return item_relates_to_target(pool, event_id, target.id).await;
    }
    matches!(
        target.item_type,
        MediaItemType::Movie | MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode
    )
}

async fn wait_for_relevant_event(
    rx: &mut tokio::sync::broadcast::Receiver<RivenEvent>,
    pool: &sqlx::PgPool,
    target: &MediaItem,
) -> Option<()> {
    loop {
        let event = match rx.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
        };

        if should_emit_for_external_target(pool, &event, target).await {
            while rx.try_recv().is_ok() {}
            return Some(());
        }
    }
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
        let pool = ctx.data::<sqlx::PgPool>()?.clone();
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(move |event| {
                let pool = pool.clone();
                async move {
                    let RivenEvent::MediaItemIndexSuccess { id, item_type, .. } = event else {
                        return None;
                    };
                    if item_type != MediaItemType::Show {
                        return None;
                    }
                    match repo::get_media_item(&pool, id).await {
                        Ok(Some(item)) => Some(Ok(Show { item })),
                        Ok(None) => None,
                        Err(error) => Some(Err(error.into())),
                    }
                }
            }),
        )
    }

    /// Fires when a media item transitions to the scraped state.
    async fn item_scraped(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<i64>>> {
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(|event| async move {
                if let RivenEvent::MediaItemScrapeSuccess { id, .. } = event {
                    Some(Ok(id))
                } else {
                    None
                }
            }),
        )
    }

    /// Fires when a media item transitions to the completed state.
    async fn item_downloaded(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<i64>>> {
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(|event| async move {
                if let RivenEvent::MediaItemDownloadSuccess { id, .. } = event {
                    Some(Ok(id))
                } else {
                    None
                }
            }),
        )
    }

    /// Fires when a media item transitions to the failed state (scrape or download error).
    async fn item_failed(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<i64>>> {
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(|event| async move {
                match event {
                    RivenEvent::MediaItemScrapeError { id, .. }
                    | RivenEvent::MediaItemScrapeErrorNoNewStreams { id, .. }
                    | RivenEvent::MediaItemDownloadError { id, .. }
                    | RivenEvent::MediaItemDownloadPartialSuccess { id } => Some(Ok(id)),
                    _ => None,
                }
            }),
        )
    }

    /// Fires when one or more media items are deleted.
    async fn items_deleted(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<Vec<i64>>>> {
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(|event| async move {
                if let RivenEvent::MediaItemsDeleted { item_ids, .. } = event {
                    Some(Ok(item_ids))
                } else {
                    None
                }
            }),
        )
    }

    async fn media_item_state_updates_by_tmdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<Option<MediaItemStateTree>>>>
    {
        let pool = ctx.data::<sqlx::PgPool>()?.clone();
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(stream::unfold(
            (queue.event_tx.subscribe(), pool, tmdb_id),
            |(mut rx, pool, tmdb_id)| async move {
                loop {
                    let current_item = match repo::get_media_item_by_tmdb(&pool, &tmdb_id).await {
                        Ok(item) => item,
                        Err(error) => {
                            return Some((Err(error.into()), (rx, pool, tmdb_id)));
                        }
                    };

                    let Some(item) = current_item.as_ref() else {
                        if rx.recv().await.is_err() {
                            return None;
                        }
                        let next = match load_item_state_by_tmdb(&pool, &tmdb_id).await {
                            Ok(value) => value,
                            Err(error) => {
                                return Some((Err(error), (rx, pool, tmdb_id)));
                            }
                        };
                        if next.is_some() {
                            return Some((Ok(next), (rx, pool, tmdb_id)));
                        }
                        continue;
                    };

                    wait_for_relevant_event(&mut rx, &pool, item).await?;

                    let next = match load_item_state_by_tmdb(&pool, &tmdb_id).await {
                        Ok(value) => value,
                        Err(error) => {
                            return Some((Err(error), (rx, pool, tmdb_id)));
                        }
                    };

                    return Some((Ok(next), (rx, pool, tmdb_id)));
                }
            },
        ))
    }

    async fn media_item_state_updates_by_tvdb(
        &self,
        ctx: &Context<'_>,
        tvdb_id: String,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<Option<MediaItemStateTree>>>>
    {
        let pool = ctx.data::<sqlx::PgPool>()?.clone();
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(stream::unfold(
            (queue.event_tx.subscribe(), pool, tvdb_id),
            |(mut rx, pool, tvdb_id)| async move {
                loop {
                    let current_item = match repo::get_media_item_by_tvdb(&pool, &tvdb_id).await {
                        Ok(item) => item,
                        Err(error) => {
                            return Some((Err(error.into()), (rx, pool, tvdb_id)));
                        }
                    };

                    let Some(item) = current_item.as_ref() else {
                        if rx.recv().await.is_err() {
                            return None;
                        }
                        let next = match load_item_state_by_tvdb(&pool, &tvdb_id).await {
                            Ok(value) => value,
                            Err(error) => {
                                return Some((Err(error), (rx, pool, tvdb_id)));
                            }
                        };
                        if next.is_some() {
                            return Some((Ok(next), (rx, pool, tvdb_id)));
                        }
                        continue;
                    };

                    wait_for_relevant_event(&mut rx, &pool, item).await?;

                    let next = match load_item_state_by_tvdb(&pool, &tvdb_id).await {
                        Ok(value) => value,
                        Err(error) => {
                            return Some((Err(error), (rx, pool, tvdb_id)));
                        }
                    };

                    return Some((Ok(next), (rx, pool, tvdb_id)));
                }
            },
        ))
    }
}
