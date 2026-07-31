use serde::{Deserialize, Serialize};

use crate::types::{
    ActivePlaybackSession, CacheCheckResult, ContentServiceResponse, DebridUserInfo,
    DownloadResult, IndexedMediaItem, ProviderInfo, ScrapeResponse, StreamLinkResponse,
};

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
    /// The plugin reached the debrid store but it reported the torrent is
    /// permanently gone (a fatal HTTP status). The link-request consumer
    /// should blacklist this stream and re-download rather than retry.
    StreamLinkDead,
    UserInfo(Vec<DebridUserInfo>),
    ActivePlaybackSessions(Vec<ActivePlaybackSession>),
    /// One artwork image, already fetched from the media server with the
    /// credential riven holds. Answers [`RivenEvent::ArtworkRequested`].
    ///
    /// [`RivenEvent::ArtworkRequested`]: crate::events::RivenEvent::ArtworkRequested
    Artwork(Artwork),
    Empty,
}

/// Image bytes plus the content type the media server reported.
///
/// `content_type` is validated by the plugin before it gets here — riven serves
/// these bytes back to a browser, so an upstream that answered with
/// `text/html` must not be able to turn the artwork route into a
/// same-origin HTML injection point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artwork {
    pub content_type: String,
    pub bytes: Vec<u8>,
}
