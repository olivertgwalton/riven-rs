use async_graphql::{Context, SimpleObject, Subscription};
use futures::stream::{self, Stream, StreamExt};
use riven_core::events::RivenEvent;
use riven_core::types::{ItemRequestType, MediaItemType};
use riven_db::entities::{ItemRequest, MediaItem};
use riven_db::repo;
use std::sync::Arc;
use tokio::sync::broadcast;

use super::queries::CoreQuery;
use super::types::MediaItemStateTree;

// ── Notification shape ────────────────────────────────────────────────────────

/// Flat representation of a `RivenEvent` streamed to subscribers.
/// All fields mirror the JSON shape previously emitted over SSE.
#[derive(SimpleObject, Clone)]
pub struct RivenNotification {
    pub event_type: String,
    pub title: Option<String>,
    pub full_title: Option<String>,
    pub item_type: Option<String>,
    pub year: Option<i32>,
    pub imdb_id: Option<String>,
    pub tmdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub duration_seconds: Option<f64>,
    pub id: Option<i64>,
    pub stream_count: Option<i64>,
    pub count: Option<i64>,
    pub new_items: Option<i64>,
    pub error: Option<String>,
}

fn notification_from_json(v: &serde_json::Value) -> Option<RivenNotification> {
    let s = |key: &str| v.get(key).and_then(|x| x.as_str()).map(str::to_string);
    Some(RivenNotification {
        event_type: s("type")?,
        title: s("title"),
        full_title: s("full_title"),
        item_type: s("item_type"),
        year: v.get("year").and_then(|x| x.as_i64()).map(|x| x as i32),
        imdb_id: s("imdb_id"),
        tmdb_id: s("tmdb_id"),
        tvdb_id: s("tvdb_id"),
        duration_seconds: v.get("duration_seconds").and_then(|x| x.as_f64()),
        id: v.get("id").and_then(|x| x.as_i64()),
        stream_count: v.get("stream_count").and_then(|x| x.as_i64()),
        count: v.get("count").and_then(|x| x.as_i64()),
        new_items: v.get("new_items").and_then(|x| x.as_i64()),
        error: s("error"),
    })
}

fn broadcast_stream<T: Clone + Send + 'static>(
    rx: broadcast::Receiver<T>,
) -> impl Stream<Item = T> {
    stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(item) => return Some((item, rx)),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
}

#[derive(Default)]
pub struct SubscriptionRoot;

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

async fn load_item_request(
    pool: &sqlx::PgPool,
    request_id: i64,
) -> async_graphql::Result<Option<ItemRequest>> {
    repo::get_item_request_by_id(pool, request_id)
        .await
        .map_err(Into::into)
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
            // Drain any other queued events so a burst of activity produces
            // one emission (with a fresh DB query) rather than one per event.
            while rx.try_recv().is_ok() {}

            return Some(());
        }
    }
}

#[Subscription]
impl SubscriptionRoot {
    /// Stream of all UI-notable Riven events. Replaces the `/notifications/stream` SSE endpoint.
    async fn notifications(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<RivenNotification>>> {
        let queue = ctx.data::<Arc<riven_queue::JobQueue>>()?;
        let rx = queue.notification_tx.subscribe();
        Ok(broadcast_stream(rx).filter_map(|json| async move {
            let v: serde_json::Value = serde_json::from_str(&json).ok()?;
            notification_from_json(&v).map(Ok)
        }))
    }

    /// Stream of live log lines. Replaces the `/logs/stream` SSE endpoint.
    /// Each item is a JSON string matching `{ timestamp, level, message, target }`.
    async fn log_lines(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<String>>> {
        let log_tx = ctx.data::<broadcast::Sender<String>>()?;
        let rx = log_tx.subscribe();
        Ok(broadcast_stream(rx).map(Ok))
    }

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

    /// Fires when a show has been indexed (metadata and episode structure persisted).
    async fn show_indexed(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = async_graphql::Result<MediaItem>>> {
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
                        Ok(Some(item)) => Some(Ok(item)),
                        Ok(None) => None,
                        Err(error) => Some(Err(error.into())),
                    }
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
