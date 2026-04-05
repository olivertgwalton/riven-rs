use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Media item types ──

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "media_item_type", rename_all = "lowercase")]
pub enum MediaItemType {
    Movie,
    Show,
    Season,
    Episode,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "media_item_state", rename_all = "snake_case")]
#[graphql(rename_items = "PascalCase")]
pub enum MediaItemState {
    Indexed,
    Unreleased,
    Scraped,
    Ongoing,
    PartiallyCompleted,
    Completed,
    Paused,
    Failed,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "show_status", rename_all = "lowercase")]
pub enum ShowStatus {
    Continuing,
    Ended,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "content_rating")]
pub enum ContentRating {
    #[sqlx(rename = "G")]
    G,
    #[sqlx(rename = "PG")]
    Pg,
    #[serde(rename = "PG-13")]
    #[sqlx(rename = "PG-13")]
    Pg13,
    #[sqlx(rename = "R")]
    R,
    #[serde(rename = "NC-17")]
    #[sqlx(rename = "NC-17")]
    Nc17,
    #[graphql(name = "TV_Y")]
    #[sqlx(rename = "TV-Y")]
    TvY,
    #[graphql(name = "TV_Y7")]
    #[sqlx(rename = "TV-Y7")]
    TvY7,
    #[graphql(name = "TV_G")]
    #[sqlx(rename = "TV-G")]
    TvG,
    #[graphql(name = "TV_PG")]
    #[sqlx(rename = "TV-PG")]
    TvPg,
    #[graphql(name = "TV_14")]
    #[sqlx(rename = "TV-14")]
    Tv14,
    #[graphql(name = "TV_MA")]
    #[sqlx(rename = "TV-MA")]
    TvMa,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "filesystem_entry_type", rename_all = "lowercase")]
pub enum FileSystemEntryType {
    Media,
    Subtitle,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "item_request_type", rename_all = "lowercase")]
pub enum ItemRequestType {
    Movie,
    Show,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "item_request_state", rename_all = "lowercase")]
pub enum ItemRequestState {
    Requested,
    Completed,
    Failed,
    Ongoing,
    Unreleased,
}

// ── External IDs ──

#[derive(Debug, Clone, Default, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ExternalIds {
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
    pub external_request_id: Option<String>,
    pub requested_by: Option<String>,
    pub requested_seasons: Option<Vec<i32>>,
}

impl ExternalIds {
    /// Deduplication key for movies (prefers imdb_id, falls back to tmdb_id).
    pub fn movie_key(&self) -> String {
        self.imdb_id
            .as_ref()
            .or(self.tmdb_id.as_ref())
            .cloned()
            .unwrap_or_default()
    }

    /// Deduplication key for shows (prefers imdb_id, falls back to tvdb_id).
    pub fn show_key(&self) -> String {
        self.imdb_id
            .as_ref()
            .or(self.tvdb_id.as_ref())
            .cloned()
            .unwrap_or_default()
    }
}

// ── Content service response ──

#[derive(Debug, Clone, Default, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ContentServiceResponse {
    pub movies: Vec<ExternalIds>,
    pub shows: Vec<ExternalIds>,
}

// ── Scrape response ──

/// Maps info_hash -> torrent title
pub type ScrapeResponse = HashMap<String, String>;

// ── Download types ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadResult {
    pub info_hash: String,
    pub files: Vec<DownloadFile>,
    pub provider: Option<String>,
    pub plugin_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadFile {
    pub filename: String,
    pub file_size: u64,
    pub download_url: Option<String>,
    pub stream_url: Option<String>,
}

// ── Stream link ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamLinkResponse {
    pub link: String,
}

// ── Cache check ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TorrentStatus {
    Cached,
    Queued,
    Downloading,
    Processing,
    Downloaded,
    Uploading,
    Failed,
    Invalid,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheCheckResult {
    pub hash: String,
    pub status: TorrentStatus,
    pub files: Vec<CacheCheckFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheCheckFile {
    pub index: u32,
    pub name: String,
    pub size: u64,
}

// ── Provider list ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInfo {
    pub name: String,
    pub store: String,
}

// ── Debrid user info ──

#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DebridUserInfo {
    pub store: String,
    pub email: Option<String>,
    pub subscription_status: Option<String>,
    pub premium_until: Option<String>,
}

// ── Active playback sessions ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[serde(rename_all = "lowercase")]
pub enum PlaybackState {
    Playing,
    Paused,
    Buffering,
    Idle,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackMethod {
    DirectPlay,
    DirectStream,
    Transcode,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ActivePlaybackSession {
    pub server: String,
    pub user_name: Option<String>,
    pub parent_title: Option<String>,
    pub item_title: String,
    pub item_type: Option<String>,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub playback_state: PlaybackState,
    pub playback_method: PlaybackMethod,
    pub position_seconds: Option<i64>,
    pub duration_seconds: Option<i64>,
    pub device_name: Option<String>,
    pub client_name: Option<String>,
    pub image_url: Option<String>,
}

// ── Index response ──

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexedMediaItem {
    pub title: Option<String>,
    pub full_title: Option<String>,
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
    pub poster_path: Option<String>,
    pub year: Option<i32>,
    pub genres: Option<Vec<String>>,
    pub country: Option<String>,
    pub language: Option<String>,
    pub network: Option<String>,
    pub content_rating: Option<ContentRating>,
    pub runtime: Option<i32>,
    pub aliases: Option<HashMap<String, Vec<String>>>,
    pub aired_at: Option<chrono::NaiveDate>,
    pub status: Option<ShowStatus>,
    pub seasons: Option<Vec<IndexedSeason>>,
}

impl IndexedMediaItem {
    /// Merge another `IndexedMediaItem` into this one. Fields from `other` take
    /// precedence when present (non-None), otherwise the existing value is kept.
    pub fn merge(self, other: Self) -> Self {
        Self {
            title: other.title.or(self.title),
            full_title: other.full_title.or(self.full_title),
            imdb_id: other.imdb_id.or(self.imdb_id),
            tvdb_id: other.tvdb_id.or(self.tvdb_id),
            tmdb_id: other.tmdb_id.or(self.tmdb_id),
            poster_path: other.poster_path.or(self.poster_path),
            year: other.year.or(self.year),
            genres: other.genres.or(self.genres),
            country: other.country.or(self.country),
            language: other.language.or(self.language),
            network: other.network.or(self.network),
            content_rating: other.content_rating.or(self.content_rating),
            runtime: other.runtime.or(self.runtime),
            aliases: other.aliases.or(self.aliases),
            aired_at: other.aired_at.or(self.aired_at),
            status: other.status.or(self.status),
            seasons: other.seasons.or(self.seasons),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexedSeason {
    pub number: i32,
    pub title: Option<String>,
    pub tvdb_id: Option<String>,
    pub episodes: Vec<IndexedEpisode>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexedEpisode {
    pub number: i32,
    pub absolute_number: Option<i32>,
    pub title: Option<String>,
    pub tvdb_id: Option<String>,
    pub aired_at: Option<chrono::NaiveDate>,
    pub runtime: Option<i32>,
    pub poster_path: Option<String>,
    pub content_rating: Option<ContentRating>,
}
