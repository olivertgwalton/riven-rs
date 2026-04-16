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
