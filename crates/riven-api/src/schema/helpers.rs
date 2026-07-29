use async_graphql::{Error, Result};

/// Normalise a user-supplied lookup key (trim + lowercase).
pub(super) fn key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

/// Validate and canonicalise a `mediaType` argument to `"movie"` or `"tv"`.
pub(super) fn required_media_type(value: Option<&str>) -> Result<&'static str> {
    match value.map(key).as_deref() {
        Some("movie") => Ok("movie"),
        Some("tv") => Ok("tv"),
        Some(other) => Err(Error::new(format!("Unsupported mediaType: {other}"))),
        None => Err(Error::new("mediaType is required")),
    }
}

/// Parse a numeric ID argument, producing a friendly error on failure.
pub(super) fn parse_id<T>(id: &str, label: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    id.trim()
        .parse()
        .map_err(|_e| Error::new(format!("{label} ID must be numeric")))
}

pub(super) fn episode_lookup_keys(item: &riven_db::entities::MediaItem) -> Vec<String> {
    item.absolute_number
        .map(|number| format!("abs:{number}"))
        .into_iter()
        .chain(
            item.season_number
                .zip(item.episode_number)
                .map(|(season, episode)| format!("{season}:{episode}")),
        )
        .collect()
}

pub use riven_rank::derive_media_metadata;
