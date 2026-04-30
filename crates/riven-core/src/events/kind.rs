use serde::{Deserialize, Serialize};

/// How an event reaches its plugin hooks. Picked at compile time per event so
/// adding a variant to `EventType` forces a corresponding `dispatch_strategy`
/// arm — without it, a new event would silently default to nothing useful
/// (broadcast queues that never get pushed to, or fan-in coordination with
/// no orchestrator on the other end).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchStrategy {
    /// Notification: every subscriber gets a fire-and-forget plugin-hook job.
    /// Producer calls `JobQueue::notify`.
    Broadcast,
    /// Orchestrator fans out per-plugin children, each plugin-hook job stores
    /// its result under the flow's `<prefix>` keys, and the last completion
    /// runs `finalize` inline. `prefix` namespaces the Redis flow keys.
    FanIn { prefix: &'static str },
    /// Caller invokes `registry.dispatch` / `dispatch_to_plugin` synchronously
    /// — no queue. Used when the caller needs the result in-process and the
    /// extra Redis round-trip would dominate the actual hook cost.
    Inline,
}

/// All event types in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    #[serde(rename = "riven.core.started")]
    CoreStarted,
    #[serde(rename = "riven.core.shutdown")]
    CoreShutdown,
    #[serde(rename = "riven.content-service.requested")]
    ContentServiceRequested,
    #[serde(rename = "riven.item-request.created")]
    ItemRequestCreated,
    #[serde(rename = "riven.item-request.updated")]
    ItemRequestUpdated,
    #[serde(rename = "riven.media-item.index.requested")]
    MediaItemIndexRequested,
    #[serde(rename = "riven.media-item.index.success")]
    MediaItemIndexSuccess,
    #[serde(rename = "riven.media-item.index.error")]
    MediaItemIndexError,
    #[serde(rename = "riven.media-item.index.error.incorrect-state")]
    MediaItemIndexErrorIncorrectState,
    #[serde(rename = "riven.media-item.scrape.requested")]
    MediaItemScrapeRequested,
    #[serde(rename = "riven.media-item.scrape.success")]
    MediaItemScrapeSuccess,
    #[serde(rename = "riven.media-item.scrape.error")]
    MediaItemScrapeError,
    #[serde(rename = "riven.media-item.scrape.error.incorrect-state")]
    MediaItemScrapeErrorIncorrectState,
    #[serde(rename = "riven.media-item.scrape.error.no-new-streams")]
    MediaItemScrapeErrorNoNewStreams,
    #[serde(rename = "riven.media-item.download.requested")]
    MediaItemDownloadRequested,
    #[serde(rename = "riven.media-item.download.cache-check-requested")]
    MediaItemDownloadCacheCheckRequested,
    #[serde(rename = "riven.media-item.download.error")]
    MediaItemDownloadError,
    #[serde(rename = "riven.media-item.download.error.incorrect-state")]
    MediaItemDownloadErrorIncorrectState,
    #[serde(rename = "riven.media-item.download.partial-success")]
    MediaItemDownloadPartialSuccess,
    #[serde(rename = "riven.media-item.download.provider-list-requested")]
    MediaItemDownloadProviderListRequested,
    #[serde(rename = "riven.media-item.download.success")]
    MediaItemDownloadSuccess,
    #[serde(rename = "riven.media-item.stream-link.requested")]
    MediaItemStreamLinkRequested,
    #[serde(rename = "riven.media-item.deleted")]
    MediaItemsDeleted,
    #[serde(rename = "riven.debrid.user-info.requested")]
    DebridUserInfoRequested,
    #[serde(rename = "riven.media-server.active-sessions.requested")]
    ActivePlaybackSessionsRequested,
}

impl EventType {
    /// Stable kebab-case identifier used for queue names and similar plumbing.
    /// Matches the `serde(rename = ...)` value (e.g. "riven.media-item.scrape.requested").
    pub const fn slug(self) -> &'static str {
        match self {
            Self::CoreStarted => "riven.core.started",
            Self::CoreShutdown => "riven.core.shutdown",
            Self::ContentServiceRequested => "riven.content-service.requested",
            Self::ItemRequestCreated => "riven.item-request.created",
            Self::ItemRequestUpdated => "riven.item-request.updated",
            Self::MediaItemIndexRequested => "riven.media-item.index.requested",
            Self::MediaItemIndexSuccess => "riven.media-item.index.success",
            Self::MediaItemIndexError => "riven.media-item.index.error",
            Self::MediaItemIndexErrorIncorrectState => "riven.media-item.index.error.incorrect-state",
            Self::MediaItemScrapeRequested => "riven.media-item.scrape.requested",
            Self::MediaItemScrapeSuccess => "riven.media-item.scrape.success",
            Self::MediaItemScrapeError => "riven.media-item.scrape.error",
            Self::MediaItemScrapeErrorIncorrectState => {
                "riven.media-item.scrape.error.incorrect-state"
            }
            Self::MediaItemScrapeErrorNoNewStreams => "riven.media-item.scrape.error.no-new-streams",
            Self::MediaItemDownloadRequested => "riven.media-item.download.requested",
            Self::MediaItemDownloadCacheCheckRequested => {
                "riven.media-item.download.cache-check-requested"
            }
            Self::MediaItemDownloadError => "riven.media-item.download.error",
            Self::MediaItemDownloadErrorIncorrectState => {
                "riven.media-item.download.error.incorrect-state"
            }
            Self::MediaItemDownloadPartialSuccess => "riven.media-item.download.partial-success",
            Self::MediaItemDownloadProviderListRequested => {
                "riven.media-item.download.provider-list-requested"
            }
            Self::MediaItemDownloadSuccess => "riven.media-item.download.success",
            Self::MediaItemStreamLinkRequested => "riven.media-item.stream-link.requested",
            Self::MediaItemsDeleted => "riven.media-item.deleted",
            Self::DebridUserInfoRequested => "riven.debrid.user-info.requested",
            Self::ActivePlaybackSessionsRequested => "riven.media-server.active-sessions.requested",
        }
    }

    pub const fn is_notable(self) -> bool {
        matches!(
            self,
            Self::MediaItemDownloadSuccess
                | Self::MediaItemScrapeSuccess
                | Self::MediaItemIndexSuccess
                | Self::MediaItemDownloadError
                | Self::MediaItemScrapeError
                | Self::MediaItemScrapeErrorNoNewStreams
                | Self::ItemRequestCreated
                | Self::ItemRequestUpdated
        )
    }

    pub const fn is_ui_streamed(self) -> bool {
        self.is_notable()
            || matches!(
                self,
                Self::MediaItemIndexRequested
                    | Self::MediaItemScrapeRequested
                    | Self::MediaItemDownloadRequested
                    | Self::MediaItemDownloadPartialSuccess
                    | Self::MediaItemsDeleted
            )
    }

    /// Compile-time mapping from event to its dispatch path. Every variant
    /// must be listed — the `match` is exhaustive, so a new event can't be
    /// added without picking a strategy.
    pub const fn dispatch_strategy(self) -> DispatchStrategy {
        use DispatchStrategy::*;
        match self {
            // ── Inline (synchronous request-response, no queue) ──────────
            Self::MediaItemDownloadRequested
            | Self::MediaItemDownloadCacheCheckRequested
            | Self::MediaItemDownloadProviderListRequested
            | Self::MediaItemStreamLinkRequested
            | Self::ActivePlaybackSessionsRequested
            | Self::DebridUserInfoRequested => Inline,

            // ── Fan-in (orchestrator fans out, finalize aggregates) ──────
            Self::MediaItemScrapeRequested => FanIn { prefix: "scrape" },
            Self::MediaItemIndexRequested => FanIn { prefix: "index" },
            Self::ContentServiceRequested => FanIn { prefix: "content" },

            // ── Broadcast (notifications) ────────────────────────────────
            Self::CoreStarted
            | Self::CoreShutdown
            | Self::ItemRequestCreated
            | Self::ItemRequestUpdated
            | Self::MediaItemIndexSuccess
            | Self::MediaItemIndexError
            | Self::MediaItemIndexErrorIncorrectState
            | Self::MediaItemScrapeSuccess
            | Self::MediaItemScrapeError
            | Self::MediaItemScrapeErrorIncorrectState
            | Self::MediaItemScrapeErrorNoNewStreams
            | Self::MediaItemDownloadError
            | Self::MediaItemDownloadErrorIncorrectState
            | Self::MediaItemDownloadPartialSuccess
            | Self::MediaItemDownloadSuccess
            | Self::MediaItemsDeleted => Broadcast,
        }
    }

}
