use async_graphql::{Context, Subscription};
use futures::stream::{Stream, StreamExt};
use riven_core::events::RivenEvent;
use riven_core::types::ItemRequestType;
use riven_db::entities::ItemRequest;
use riven_db::repo;
use std::sync::Arc;

use super::broadcast_stream;

async fn load_item_request(request_id: i64) -> async_graphql::Result<Option<ItemRequest>> {
    repo::get_item_request_by_id(request_id)
        .await
        .map_err(Into::into)
}

fn request_stream(
    rx: tokio::sync::broadcast::Receiver<RivenEvent>,
    select_id: impl Fn(RivenEvent) -> Option<i64> + Copy,
) -> impl Stream<Item = async_graphql::Result<ItemRequest>> {
    broadcast_stream(rx).filter_map(move |event| async move {
        let request_id = select_id(event)?;
        load_item_request(request_id).await.transpose()
    })
}

#[derive(Default)]
pub struct RequestsSubscription;

#[Subscription]
impl RequestsSubscription {
    /// Fires when a new movie item request is created.
    async fn movie_requested(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<ItemRequest>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(request_stream(
            queue.event_tx.subscribe(),
            |event| match event {
                RivenEvent::ItemRequestCreated {
                    request_id,
                    request_type: ItemRequestType::Movie,
                    ..
                } => Some(request_id),
                _ => None,
            },
        ))
    }

    /// Fires when a new show item request is created.
    async fn show_requested(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<ItemRequest>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(request_stream(
            queue.event_tx.subscribe(),
            |event| match event {
                RivenEvent::ItemRequestCreated {
                    request_id,
                    request_type: ItemRequestType::Show,
                    ..
                } => Some(request_id),
                _ => None,
            },
        ))
    }

    /// Fires when an existing show item request is updated (e.g. new seasons added).
    async fn show_request_updated(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<ItemRequest>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        Ok(request_stream(
            queue.event_tx.subscribe(),
            |event| match event {
                RivenEvent::ItemRequestUpdated {
                    request_id,
                    request_type: ItemRequestType::Show,
                    ..
                } => Some(request_id),
                _ => None,
            },
        ))
    }
}
