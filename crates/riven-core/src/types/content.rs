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

    /// Merges another entry for the same key into this one, in place.
    ///
    /// Used when two separate upstream requests (e.g. two partial-season Seerr
    /// requests for the same show) resolve to the same dedup key: without this,
    /// whichever request is processed second is silently dropped.
    pub fn merge(&mut self, other: ExternalIds) {
        self.imdb_id = self.imdb_id.take().or(other.imdb_id);
        self.tvdb_id = self.tvdb_id.take().or(other.tvdb_id);
        self.tmdb_id = self.tmdb_id.take().or(other.tmdb_id);
        self.external_request_id = self.external_request_id.take().or(other.external_request_id);
        self.requested_by = self.requested_by.take().or(other.requested_by);

        self.requested_seasons = match (self.requested_seasons.take(), other.requested_seasons) {
            (Some(mut a), Some(b)) => {
                a.extend(b);
                a.sort_unstable();
                a.dedup();
                Some(a)
            }
            (a, b) => a.or(b),
        };
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ContentServiceResponse {
    pub movies: Vec<ExternalIds>,
    pub shows: Vec<ExternalIds>,
}
