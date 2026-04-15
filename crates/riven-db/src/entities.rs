use chrono::{DateTime, Datelike, NaiveDate, Utc};
use riven_core::settings::{FilesystemContentType, FilesystemItemMetadata};
use riven_core::types::*;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

// ── Media Item ──

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct MediaItem {
    pub id: i64,
    pub title: String,
    pub full_title: Option<String>,
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
    pub poster_path: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub indexed_at: Option<DateTime<Utc>>,
    pub scraped_at: Option<DateTime<Utc>>,
    pub scraped_times: i32,
    pub aliases: Option<serde_json::Value>,
    pub network: Option<String>,
    pub country: Option<String>,
    pub language: Option<String>,
    pub is_anime: bool,
    pub aired_at: Option<NaiveDate>,
    pub year: Option<i32>,
    pub genres: Option<serde_json::Value>,
    pub rating: Option<f64>,
    pub content_rating: Option<ContentRating>,
    pub state: MediaItemState,
    pub failed_attempts: i32,
    pub item_type: MediaItemType,
    pub is_requested: bool,
    // Show-specific
    pub show_status: Option<ShowStatus>,
    // Season-specific
    pub season_number: Option<i32>,
    pub is_special: Option<bool>,
    pub parent_id: Option<i64>,
    // Episode-specific
    pub episode_number: Option<i32>,
    pub absolute_number: Option<i32>,
    pub runtime: Option<i32>,
    // Item request FK
    pub item_request_id: Option<i64>,
    // Active stream FK
    pub active_stream_id: Option<i64>,
}

#[derive(Debug, Clone, FromRow)]
pub struct MediaItemHierarchy {
    #[sqlx(flatten)]
    pub item: MediaItem,
    pub resolved_season_id: Option<i64>,
    pub resolved_season_number: Option<i32>,
    pub resolved_show_id: Option<i64>,
    pub resolved_show_title: Option<String>,
    pub resolved_show_imdb_id: Option<String>,
    pub resolved_show_tvdb_id: Option<String>,
    pub resolved_show_year: Option<i32>,
    pub resolved_show_aliases: Option<serde_json::Value>,
    pub resolved_show_genres: Option<serde_json::Value>,
    pub resolved_show_network: Option<String>,
    pub resolved_show_rating: Option<f64>,
    pub resolved_show_content_rating: Option<ContentRating>,
    pub resolved_show_language: Option<String>,
    pub resolved_show_country: Option<String>,
    pub resolved_show_is_anime: Option<bool>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct MediaItemListRow {
    #[sqlx(flatten)]
    #[graphql(flatten)]
    pub item: MediaItem,
    pub show_id: Option<i64>,
    pub show_title: Option<String>,
    pub show_tmdb_id: Option<String>,
    pub show_tvdb_id: Option<String>,
    pub show_poster_path: Option<String>,
}

impl MediaItem {
    pub fn filesystem_metadata(&self) -> FilesystemItemMetadata {
        build_filesystem_metadata(
            self.genres.as_ref(),
            self.network.clone(),
            self.content_rating,
            self.language.clone(),
            self.country.clone(),
            self.aired_at.map(|date| date.year()).or(self.year),
            self.rating,
            self.is_anime,
        )
    }

    pub fn pretty_name(&self) -> String {
        let year_str = self.year.map(|y| format!(" ({y})")).unwrap_or_default();
        let id_str = match self.item_type {
            MediaItemType::Movie => self
                .tmdb_id
                .as_ref()
                .map(|id| format!(" {{tmdb-{id}}}"))
                .unwrap_or_default(),
            _ => self
                .tvdb_id
                .as_ref()
                .map(|id| format!(" {{tvdb-{id}}}"))
                .unwrap_or_default(),
        };
        format!("{}{year_str}{id_str}", self.title)
    }
}

// ── Calendar Row ──

/// Lightweight projection used by the calendar GraphQL query.
/// Resolves the ancestor show title in a single SQL JOIN rather than N+1 lookups.
#[derive(Debug, Clone, FromRow)]
pub struct CalendarRow {
    pub id: i64,
    pub item_type: MediaItemType,
    pub state: MediaItemState,
    pub title: String,
    /// Resolved show title: for episodes/seasons this walks to the top-level show;
    /// for movies it is the movie title.
    pub show_title: String,
    pub aired_at: Option<NaiveDate>,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub tmdb_id: Option<String>,
    pub tvdb_id: Option<String>,
}

// ── Filesystem Entry ──

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct FileSystemEntry {
    pub id: i64,
    pub file_size: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub media_item_id: i64,
    pub entry_type: FileSystemEntryType,
    pub path: String,
    // Media entry fields
    pub original_filename: Option<String>,
    pub download_url: Option<String>,
    pub stream_url: Option<String>,
    pub plugin: Option<String>,
    pub provider: Option<String>,
    pub provider_download_id: Option<String>,
    pub library_profiles: Option<serde_json::Value>,
    pub media_metadata: Option<serde_json::Value>,
    // Subtitle entry fields
    pub language: Option<String>,
    pub parent_original_filename: Option<String>,
    pub subtitle_content: Option<String>,
    pub file_hash: Option<String>,
    pub video_file_size: Option<i64>,
    pub opensubtitles_id: Option<String>,
    // Multi-version tracking
    pub stream_id: Option<i64>,
    pub resolution: Option<String>,
    pub ranking_profile_name: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
pub struct FilesystemProfileEntryCandidate {
    pub id: i64,
    pub library_profiles: Option<serde_json::Value>,
    pub content_type: String,
    pub genres: Option<serde_json::Value>,
    pub network: Option<String>,
    pub content_rating: Option<ContentRating>,
    pub language: Option<String>,
    pub country: Option<String>,
    pub year: Option<i32>,
    pub rating: Option<f64>,
    pub is_anime: bool,
}

#[derive(Debug, Clone, FromRow)]
pub struct VfsDirName {
    pub name: Option<String>,
    pub library_profiles: Option<serde_json::Value>,
}

#[derive(Debug, Clone, FromRow)]
pub struct VfsFileName {
    pub name: Option<String>,
    pub library_profiles: Option<serde_json::Value>,
}

impl FilesystemProfileEntryCandidate {
    pub fn filesystem_content_type(&self) -> FilesystemContentType {
        match self.content_type.as_str() {
            "movie" => FilesystemContentType::Movie,
            _ => FilesystemContentType::Show,
        }
    }

    pub fn filesystem_metadata(&self) -> FilesystemItemMetadata {
        build_filesystem_metadata(
            self.genres.as_ref(),
            self.network.clone(),
            self.content_rating,
            self.language.clone(),
            self.country.clone(),
            self.year,
            self.rating,
            self.is_anime,
        )
    }
}

fn build_filesystem_metadata(
    genres: Option<&serde_json::Value>,
    network: Option<String>,
    content_rating: Option<ContentRating>,
    language: Option<String>,
    country: Option<String>,
    year: Option<i32>,
    rating: Option<f64>,
    is_anime: bool,
) -> FilesystemItemMetadata {
    FilesystemItemMetadata {
        genres: lowercase_json_strings(genres),
        network,
        content_rating,
        language,
        country,
        year,
        rating,
        is_anime,
    }
}

fn lowercase_json_strings(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_ascii_lowercase)
        .collect()
}

impl FileSystemEntry {
    pub fn base_directory(&self) -> &str {
        if self.path.starts_with("/movies") {
            "movies"
        } else {
            "shows"
        }
    }

    pub fn vfs_filename(&self, pretty_name: &str) -> String {
        let ext = self
            .original_filename
            .as_ref()
            .and_then(|f| f.rsplit('.').next())
            .unwrap_or("mkv");
        format!("{pretty_name}.{ext}")
    }
}

// ── Stream ──

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct Stream {
    pub id: i64,
    pub info_hash: String,
    pub magnet: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    #[sqlx(json)]
    pub parsed_data: Option<serde_json::Value>,
    pub rank: Option<i64>,
    /// Actual file size in bytes, recorded after the first download attempt.
    /// `None` means the size is not yet known (stream has never been tried).
    pub file_size_bytes: Option<i64>,
}

// ── Item Request ──

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ItemRequest {
    pub id: i64,
    pub imdb_id: Option<String>,
    pub tmdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub request_type: ItemRequestType,
    pub requested_by: Option<String>,
    pub external_request_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub state: ItemRequestState,
    pub seasons: Option<serde_json::Value>,
}

// ── Media Item with relations (for VFS readdir) ──

#[derive(Debug, Clone)]
pub struct MovieWithEntries {
    pub item: MediaItem,
    pub entries: Vec<FileSystemEntry>,
}

#[derive(Debug, Clone)]
pub struct ShowWithSeasons {
    pub item: MediaItem,
    pub seasons: Vec<SeasonWithEpisodes>,
}

#[derive(Debug, Clone)]
pub struct SeasonWithEpisodes {
    pub item: MediaItem,
    pub episodes: Vec<EpisodeWithEntries>,
}

#[derive(Debug, Clone)]
pub struct EpisodeWithEntries {
    pub item: MediaItem,
    pub entries: Vec<FileSystemEntry>,
}
