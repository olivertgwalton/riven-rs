use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Default, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ContentServiceResponse {
    pub movies: Vec<ExternalIds>,
    pub shows: Vec<ExternalIds>,
}
