use async_graphql::{Context, Subscription};
use futures::stream::{self, Stream};
use riven_core::events::RivenEvent;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;
use riven_db::repo;
use std::sync::Arc;

use super::queries::CoreQuery;
use super::types::MediaItemStateTree;

#[derive(Default)]
pub struct SubscriptionRoot;

fn event_item_id(event: &RivenEvent) -> Option<i64> {
    match event {
        RivenEvent::MediaItemIndexRequested { id, .. }
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
    let mut current_id = Some(item_id);

    while let Some(id) = current_id {
        if id == target_id {
            return true;
        }

        current_id = match repo::get_media_item(pool, id).await {
            Ok(Some(item)) => item.parent_id,
            _ => None,
        };
    }

    false
}

async fn load_item_state_by_tmdb(
    pool: &sqlx::PgPool,
    tmdb_id: &str,
) -> async_graphql::Result<Option<MediaItemStateTree>> {
    let Some(item) = repo::get_media_item_by_tmdb(pool, tmdb_id).await? else {
        return Ok(None);
    };

    CoreQuery
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

    CoreQuery
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

    match event {
        RivenEvent::ItemRequestCreateSuccess { .. } => false,
        _ => matches!(
            target.item_type,
            MediaItemType::Movie
                | MediaItemType::Show
                | MediaItemType::Season
                | MediaItemType::Episode
        ),
    }
}

async fn wait_for_relevant_event(
    rx: &mut tokio::sync::broadcast::Receiver<String>,
    pool: &sqlx::PgPool,
    target: &MediaItem,
) -> Option<()> {
    loop {
        let raw = match rx.recv().await {
            Ok(raw) => raw,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
        };

        let Ok(event) = serde_json::from_str::<RivenEvent>(&raw) else {
            continue;
        };

        if should_emit_for_external_target(pool, &event, target).await {
            while let Ok(raw) = rx.try_recv() {
                let Ok(event) = serde_json::from_str::<RivenEvent>(&raw) else {
                    continue;
                };

                if should_emit_for_external_target(pool, &event, target).await {
                    continue;
                }
            }

            return Some(());
        }
    }
}

#[Subscription]
impl SubscriptionRoot {
    async fn media_item_state_updates_by_tmdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<Option<MediaItemStateTree>>>>
    {
        let pool = ctx.data::<sqlx::PgPool>()?.clone();
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(stream::unfold(
            (queue.notification_tx.subscribe(), pool, tmdb_id),
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
            (queue.notification_tx.subscribe(), pool, tvdb_id),
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
