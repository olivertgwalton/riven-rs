use serde::{Deserialize, Serialize};

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
}
