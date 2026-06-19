use chrono::NaiveDate;
use riven_core::entities::helpers::build_filesystem_metadata;
use riven_core::settings::{FilesystemContentType, FilesystemItemMetadata};
use riven_core::types::*;
use serde::{Deserialize, Serialize};

pub use riven_core::entities::{
    filesystem_entries::Model as FileSystemEntry, item_requests::Model as ItemRequest,
    media_items::Model as MediaItem, streams::Model as Stream,
};

#[derive(Debug, Clone, sea_orm::FromQueryResult)]
pub struct MediaItemHierarchy {
    #[sea_orm(nested)]
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

#[derive(
    Debug, Clone, Serialize, Deserialize, async_graphql::SimpleObject, sea_orm::FromQueryResult,
)]
pub struct MediaItemListRow {
    #[graphql(flatten)]
    #[sea_orm(nested)]
    pub item: MediaItem,
    pub show_id: Option<i64>,
    pub show_title: Option<String>,
    pub show_tmdb_id: Option<String>,
    pub show_tvdb_id: Option<String>,
    pub show_poster_path: Option<String>,
}

/// Lightweight projection used by the calendar GraphQL query.
/// Resolves the ancestor show title in a single SQL JOIN rather than N+1 lookups.
#[derive(Debug, Clone, sea_orm::FromQueryResult)]
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

#[derive(Debug, Clone, sea_orm::FromQueryResult)]
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

#[derive(Debug, Clone, sea_orm::FromQueryResult)]
pub struct VfsDirName {
    pub name: Option<String>,
    pub library_profiles: Option<serde_json::Value>,
}

#[derive(Debug, Clone, sea_orm::FromQueryResult)]
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
