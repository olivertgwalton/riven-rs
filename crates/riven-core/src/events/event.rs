use serde::{Deserialize, Serialize};

use super::{EventType, IndexRequest, ScrapeRequest};
use crate::types::{ItemRequestType, MediaItemType};

/// A concrete event with its payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RivenEvent {
    #[serde(rename = "riven.core.started")]
    CoreStarted,
    #[serde(rename = "riven.core.shutdown")]
    CoreShutdown,
    #[serde(rename = "riven.content-service.requested")]
    ContentServiceRequested,
    #[serde(rename = "riven.item-request.created")]
    ItemRequestCreated {
        request_id: i64,
        item_id: i64,
        request_type: ItemRequestType,
        requested_seasons: Option<Vec<i32>>,
    },
    #[serde(rename = "riven.item-request.updated")]
    ItemRequestUpdated {
        request_id: i64,
        item_id: i64,
        request_type: ItemRequestType,
        requested_seasons: Option<Vec<i32>>,
    },
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
    #[serde(rename = "riven.media-item.stream-link.requested")]
    MediaItemStreamLinkRequested {
        magnet: String,
        info_hash: String,
        provider: Option<String>,
        stream_base_url: Option<String>,
    },
    #[serde(rename = "riven.media-item.deleted")]
    MediaItemsDeleted {
        item_ids: Vec<i64>,
        external_request_ids: Vec<String>,
        deleted_paths: Vec<String>,
    },
    #[serde(rename = "riven.debrid.user-info.requested")]
    DebridUserInfoRequested,
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
            Self::ItemRequestCreated { .. } => EventType::ItemRequestCreated,
            Self::ItemRequestUpdated { .. } => EventType::ItemRequestUpdated,
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
