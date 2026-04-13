use riven_db::entities::{ItemRequest, MediaItem};
use tokio::sync::broadcast;

/// Typed pub-sub events mirroring the riven-ts `pubSub` topics.
#[derive(Debug, Clone)]
pub enum PubSubEvent {
    ItemRequestCreated(ItemRequest),
    ItemRequestUpdated(ItemRequest),
    MediaItemIndexed(MediaItem),
}

/// In-process broadcast channel used by GraphQL subscriptions.
#[derive(Debug)]
pub struct PubSub {
    pub tx: broadcast::Sender<PubSubEvent>,
}

impl PubSub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PubSubEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: PubSubEvent) {
        let _ = self.tx.send(event);
    }
}
