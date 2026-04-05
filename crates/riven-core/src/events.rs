use serde::{Deserialize, Serialize};

use crate::types::{
    ActivePlaybackSession, CacheCheckResult, ContentServiceResponse, DebridUserInfo,
    DownloadResult, IndexedMediaItem, MediaItemType, ProviderInfo, ScrapeResponse,
    StreamLinkResponse,
};

pub struct ScrapeRequest<'a> {
    pub id: i64,
    pub item_type: MediaItemType,
    pub imdb_id: Option<&'a str>,
    pub title: &'a str,
    pub season: Option<i32>,
    pub episode: Option<i32>,
}

pub struct IndexRequest<'a> {
    pub id: i64,
    pub item_type: MediaItemType,
    pub imdb_id: Option<&'a str>,
    pub tvdb_id: Option<&'a str>,
    pub tmdb_id: Option<&'a str>,
}

impl<'a> ScrapeRequest<'a> {
    pub fn season_or_1(&self) -> i32 {
        self.season.unwrap_or(1)
    }

    pub fn episode_or_1(&self) -> i32 {
        self.episode.unwrap_or(1)
    }
}

/// All event types in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    // Program lifecycle
    #[serde(rename = "riven.core.started")]
    CoreStarted,
    #[serde(rename = "riven.core.shutdown")]
    CoreShutdown,

    // Content services
    #[serde(rename = "riven.content-service.requested")]
    ContentServiceRequested,

    // Item request
    #[serde(rename = "riven.item-request.create.success")]
    ItemRequestCreateSuccess,
    #[serde(rename = "riven.item-request.create.error")]
    ItemRequestCreateError,
    #[serde(rename = "riven.item-request.create.error.conflict")]
    ItemRequestCreateErrorConflict,
    #[serde(rename = "riven.item-request.update.success")]
    ItemRequestUpdateSuccess,

    // Item indexing
    #[serde(rename = "riven.media-item.index.requested")]
    MediaItemIndexRequested,
    #[serde(rename = "riven.media-item.index.success")]
    MediaItemIndexSuccess,
    #[serde(rename = "riven.media-item.index.error")]
    MediaItemIndexError,
    #[serde(rename = "riven.media-item.index.error.incorrect-state")]
    MediaItemIndexErrorIncorrectState,

    // Item scraping
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

    // Item downloading
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

    // Item streaming
    #[serde(rename = "riven.media-item.stream-link.requested")]
    MediaItemStreamLinkRequested,

    // Item deletion
    #[serde(rename = "riven.media-item.deleted")]
    MediaItemsDeleted,

    // Debrid account info
    #[serde(rename = "riven.debrid.user-info.requested")]
    DebridUserInfoRequested,

    // Media server active sessions
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
                | Self::ItemRequestCreateSuccess
                | Self::ItemRequestCreateError
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
                    | Self::ItemRequestUpdateSuccess
                    | Self::MediaItemsDeleted
            )
    }
}

/// A concrete event with its payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RivenEvent {
    // Lifecycle
    #[serde(rename = "riven.core.started")]
    CoreStarted,
    #[serde(rename = "riven.core.shutdown")]
    CoreShutdown,

    // Content services
    #[serde(rename = "riven.content-service.requested")]
    ContentServiceRequested,

    // Item requests
    #[serde(rename = "riven.item-request.create.success")]
    ItemRequestCreateSuccess {
        count: usize,
        new_items: usize,
        updated_items: usize,
    },
    #[serde(rename = "riven.item-request.create.error")]
    ItemRequestCreateError { error: String },
    #[serde(rename = "riven.item-request.create.error.conflict")]
    ItemRequestCreateErrorConflict { imdb_id: Option<String> },
    #[serde(rename = "riven.item-request.update.success")]
    ItemRequestUpdateSuccess { id: i64 },

    // Indexing
    #[serde(rename = "riven.media-item.index.requested")]
    MediaItemIndexRequested {
        id: i64,
        item_type: MediaItemType,
        imdb_id: Option<String>,
        tvdb_id: Option<String>,
        tmdb_id: Option<String>,
    },
    #[serde(rename = "riven.media-item.index.success")]
    MediaItemIndexSuccess {
        id: i64,
        title: String,
        item_type: MediaItemType,
    },
    #[serde(rename = "riven.media-item.index.error")]
    MediaItemIndexError { id: i64, error: String },
    #[serde(rename = "riven.media-item.index.error.incorrect-state")]
    MediaItemIndexErrorIncorrectState { id: i64 },

    // Scraping
    #[serde(rename = "riven.media-item.scrape.requested")]
    MediaItemScrapeRequested {
        id: i64,
        item_type: MediaItemType,
        imdb_id: Option<String>,
        title: String,
        season: Option<i32>,
        episode: Option<i32>,
    },
    #[serde(rename = "riven.media-item.scrape.success")]
    MediaItemScrapeSuccess {
        id: i64,
        title: String,
        item_type: MediaItemType,
        stream_count: usize,
    },
    #[serde(rename = "riven.media-item.scrape.error")]
    MediaItemScrapeError {
        id: i64,
        title: String,
        error: String,
    },
    #[serde(rename = "riven.media-item.scrape.error.incorrect-state")]
    MediaItemScrapeErrorIncorrectState { id: i64 },
    #[serde(rename = "riven.media-item.scrape.error.no-new-streams")]
    MediaItemScrapeErrorNoNewStreams {
        id: i64,
        title: String,
        item_type: MediaItemType,
    },

    // Downloading
    #[serde(rename = "riven.media-item.download.requested")]
    MediaItemDownloadRequested {
        id: i64,
        info_hash: String,
        magnet: String,
    },
    #[serde(rename = "riven.media-item.download.cache-check-requested")]
    MediaItemDownloadCacheCheckRequested { hashes: Vec<String> },
    #[serde(rename = "riven.media-item.download.error")]
    MediaItemDownloadError {
        id: i64,
        title: String,
        error: String,
    },
    #[serde(rename = "riven.media-item.download.error.incorrect-state")]
    MediaItemDownloadErrorIncorrectState { id: i64 },
    #[serde(rename = "riven.media-item.download.partial-success")]
    MediaItemDownloadPartialSuccess { id: i64 },
    #[serde(rename = "riven.media-item.download.provider-list-requested")]
    MediaItemDownloadProviderListRequested,
    #[serde(rename = "riven.media-item.download.success")]
    MediaItemDownloadSuccess {
        id: i64,
        title: String,
        full_title: Option<String>,
        item_type: MediaItemType,
        year: Option<i32>,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        poster_path: Option<String>,
        plugin_name: String,
        provider: Option<String>,
        duration_seconds: f64,
    },

    // Streaming
    #[serde(rename = "riven.media-item.stream-link.requested")]
    MediaItemStreamLinkRequested { magnet: String, info_hash: String },

    // Deletion — carries the external content-service request IDs so plugins
    // (e.g. Seerr) can cancel/delete the corresponding requests.
    #[serde(rename = "riven.media-item.deleted")]
    MediaItemsDeleted { external_request_ids: Vec<String> },

    // Debrid account info
    #[serde(rename = "riven.debrid.user-info.requested")]
    DebridUserInfoRequested,

    // Media server active sessions
    #[serde(rename = "riven.media-server.active-sessions.requested")]
    ActivePlaybackSessionsRequested,
}

impl RivenEvent {
    pub fn index_request(&self) -> Option<IndexRequest<'_>> {
        match self {
            Self::MediaItemIndexRequested {
                id,
                item_type,
                imdb_id,
                tvdb_id,
                tmdb_id,
            } => Some(IndexRequest {
                id: *id,
                item_type: *item_type,
                imdb_id: imdb_id.as_deref(),
                tvdb_id: tvdb_id.as_deref(),
                tmdb_id: tmdb_id.as_deref(),
            }),
            _ => None,
        }
    }

    pub fn scrape_request(&self) -> Option<ScrapeRequest<'_>> {
        match self {
            Self::MediaItemScrapeRequested {
                id,
                item_type,
                imdb_id,
                title,
                season,
                episode,
            } => Some(ScrapeRequest {
                id: *id,
                item_type: *item_type,
                imdb_id: imdb_id.as_deref(),
                title,
                season: *season,
                episode: *episode,
            }),
            _ => None,
        }
    }

    /// Returns true for events that should be shown as UI notifications.
    pub fn is_notable(&self) -> bool {
        self.event_type().is_notable()
    }

    pub fn event_type(&self) -> EventType {
        match self {
            Self::CoreStarted => EventType::CoreStarted,
            Self::CoreShutdown => EventType::CoreShutdown,
            Self::ContentServiceRequested => EventType::ContentServiceRequested,
            Self::ItemRequestCreateSuccess { .. } => EventType::ItemRequestCreateSuccess,
            Self::ItemRequestCreateError { .. } => EventType::ItemRequestCreateError,
            Self::ItemRequestCreateErrorConflict { .. } => {
                EventType::ItemRequestCreateErrorConflict
            }
            Self::ItemRequestUpdateSuccess { .. } => EventType::ItemRequestUpdateSuccess,
            Self::MediaItemIndexRequested { .. } => EventType::MediaItemIndexRequested,
            Self::MediaItemIndexSuccess { .. } => EventType::MediaItemIndexSuccess,
            Self::MediaItemIndexError { .. } => EventType::MediaItemIndexError,
            Self::MediaItemIndexErrorIncorrectState { .. } => {
                EventType::MediaItemIndexErrorIncorrectState
            }
            Self::MediaItemScrapeRequested { .. } => EventType::MediaItemScrapeRequested,
            Self::MediaItemScrapeSuccess { .. } => EventType::MediaItemScrapeSuccess,
            Self::MediaItemScrapeError { .. } => EventType::MediaItemScrapeError,
            Self::MediaItemScrapeErrorIncorrectState { .. } => {
                EventType::MediaItemScrapeErrorIncorrectState
            }
            Self::MediaItemScrapeErrorNoNewStreams { .. } => {
                EventType::MediaItemScrapeErrorNoNewStreams
            }
            Self::MediaItemDownloadRequested { .. } => EventType::MediaItemDownloadRequested,
            Self::MediaItemDownloadCacheCheckRequested { .. } => {
                EventType::MediaItemDownloadCacheCheckRequested
            }
            Self::MediaItemDownloadError { .. } => EventType::MediaItemDownloadError,
            Self::MediaItemDownloadErrorIncorrectState { .. } => {
                EventType::MediaItemDownloadErrorIncorrectState
            }
            Self::MediaItemDownloadPartialSuccess { .. } => {
                EventType::MediaItemDownloadPartialSuccess
            }
            Self::MediaItemDownloadProviderListRequested => {
                EventType::MediaItemDownloadProviderListRequested
            }
            Self::MediaItemDownloadSuccess { .. } => EventType::MediaItemDownloadSuccess,
            Self::MediaItemStreamLinkRequested { .. } => EventType::MediaItemStreamLinkRequested,
            Self::MediaItemsDeleted { .. } => EventType::MediaItemsDeleted,
            Self::DebridUserInfoRequested => EventType::DebridUserInfoRequested,
            Self::ActivePlaybackSessionsRequested => EventType::ActivePlaybackSessionsRequested,
        }
    }
}

/// Typed responses that hooks can return.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HookResponse {
    ContentService(Box<ContentServiceResponse>),
    Index(Box<IndexedMediaItem>),
    Scrape(ScrapeResponse),
    Download(Box<DownloadResult>),
    /// The plugin reached the debrid store but the torrent is not available
    /// (not cached, rejected, etc.). The download flow should blacklist this
    /// stream and try the next best candidate rather than scheduling a retry.
    DownloadStreamUnavailable,
    CacheCheck(Vec<CacheCheckResult>),
    ProviderList(Vec<ProviderInfo>),
    StreamLink(StreamLinkResponse),
    UserInfo(Vec<DebridUserInfo>),
    ActivePlaybackSessions(Vec<ActivePlaybackSession>),
    Empty,
}
