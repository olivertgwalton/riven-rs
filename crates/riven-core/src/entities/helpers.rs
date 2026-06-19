//! Shared helpers for entity Model impls.

use crate::settings::FilesystemItemMetadata;
use crate::types::ContentRating;

pub fn build_filesystem_metadata(
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
