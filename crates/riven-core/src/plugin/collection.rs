use std::collections::HashMap;

use crate::events::HookResponse;
use crate::types::{ContentServiceResponse, ExternalIds};

#[derive(Default)]
pub struct ContentCollection {
    movies: HashMap<String, ExternalIds>,
    shows: HashMap<String, ExternalIds>,
}

impl ContentCollection {
    pub fn insert_movie(&mut self, ids: ExternalIds) {
        self.movies.entry(ids.movie_key()).or_insert(ids);
    }

    pub fn insert_show(&mut self, ids: ExternalIds) {
        self.shows.entry(ids.show_key()).or_insert(ids);
    }

    pub fn movie_count(&self) -> usize {
        self.movies.len()
    }

    pub fn show_count(&self) -> usize {
        self.shows.len()
    }

    pub fn into_response(self) -> ContentServiceResponse {
        ContentServiceResponse {
            movies: self.movies.into_values().collect(),
            shows: self.shows.into_values().collect(),
        }
    }

    pub fn into_hook_response(self) -> HookResponse {
        HookResponse::ContentService(Box::new(self.into_response()))
    }
}
