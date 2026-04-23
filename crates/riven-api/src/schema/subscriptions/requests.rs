use async_graphql::{Context, Subscription};
use futures::stream::{Stream, StreamExt};
use riven_core::events::RivenEvent;
use riven_core::types::ItemRequestType;
use riven_db::entities::ItemRequest;
use riven_db::repo;
use std::sync::Arc;

use super::broadcast_stream;

async fn load_item_request(
    pool: &sqlx::PgPool,
    request_id: i64,
) -> async_graphql::Result<Option<ItemRequest>> {
    repo::get_item_request_by_id(pool, request_id)
        .await
        .map_err(Into::into)
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
        let pool = ctx.data::<sqlx::PgPool>()?.clone();
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(move |event| {
                let pool = pool.clone();
                async move {
                    let RivenEvent::ItemRequestCreated {
                        request_id,
                        request_type,
                        ..
                    } = event
                    else {
                        return None;
                    };
                    if request_type != ItemRequestType::Movie {
                        return None;
                    }
                    match load_item_request(&pool, request_id).await {
                        Ok(Some(request)) => Some(Ok(request)),
                        Ok(None) => None,
                        Err(error) => Some(Err(error)),
                    }
                }
            }),
        )
    }

    /// Fires when a new show item request is created.
    async fn show_requested(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<ItemRequest>>> {
        let pool = ctx.data::<sqlx::PgPool>()?.clone();
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(move |event| {
                let pool = pool.clone();
                async move {
                    let RivenEvent::ItemRequestCreated {
                        request_id,
                        request_type,
                        ..
                    } = event
                    else {
                        return None;
                    };
                    if request_type != ItemRequestType::Show {
                        return None;
                    }
                    match load_item_request(&pool, request_id).await {
                        Ok(Some(request)) => Some(Ok(request)),
                        Ok(None) => None,
                        Err(error) => Some(Err(error)),
                    }
                }
            }),
        )
    }

    /// Fires when an existing show item request is updated (e.g. new seasons added).
    async fn show_request_updated(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<ItemRequest>>> {
        let pool = ctx.data::<sqlx::PgPool>()?.clone();
        let queue = Arc::clone(ctx.data::<Arc<riven_queue::JobQueue>>()?);
        Ok(
            broadcast_stream(queue.event_tx.subscribe()).filter_map(move |event| {
                let pool = pool.clone();
                async move {
                    let RivenEvent::ItemRequestUpdated {
                        request_id,
                        request_type,
                        ..
                    } = event
                    else {
                        return None;
                    };
                    if request_type != ItemRequestType::Show {
                        return None;
                    }
                    match load_item_request(&pool, request_id).await {
                        Ok(Some(request)) => Some(Ok(request)),
                        Ok(None) => None,
                        Err(error) => Some(Err(error)),
                    }
                }
            }),
        )
    }
}
