use async_graphql::MergedSubscription;
use futures::stream::{self, Stream};
use tokio::sync::broadcast;

mod media_items;
mod notifications;
mod requests;

pub(super) fn broadcast_stream<T: Clone + Send + 'static>(
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

#[derive(MergedSubscription, Default)]
pub struct SubscriptionRoot(
    notifications::NotificationsSubscription,
    requests::RequestsSubscription,
    media_items::MediaItemsSubscription,
);
