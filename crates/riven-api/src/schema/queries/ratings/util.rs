use async_graphql::{Context, Error, Result};
use riven_core::http::HttpClient;

use super::RatingScore;

pub(super) fn key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

pub(super) fn required_media_type(value: Option<&str>) -> Result<&'static str> {
    match value.map(key).as_deref() {
        Some("movie") => Ok("movie"),
        Some("tv") => Ok("tv"),
        Some(other) => Err(Error::new(format!("Unsupported mediaType: {other}"))),
        None => Err(Error::new("mediaType is required")),
    }
}

pub(super) fn parse_id<T>(id: &str, label: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    id.trim()
        .parse()
        .map_err(|_| Error::new(format!("{label} ID must be numeric")))
}

pub(super) fn optional_http(ctx: &Context<'_>, operation: &str) -> Option<HttpClient> {
    match ctx.data::<HttpClient>() {
        Ok(http) => Some(http.clone()),
        Err(error) => {
            tracing::warn!(
                ?error,
                operation,
                "HTTP client missing from GraphQL context"
            );
            None
        }
    }
}

pub(super) fn score_item(
    name: impl Into<String>,
    image: impl Into<String>,
    score: impl Into<String>,
    url: impl Into<String>,
) -> RatingScore {
    RatingScore {
        name: name.into(),
        image: Some(image.into()),
        score: score.into(),
        url: Some(url.into()),
    }
}

pub(super) fn decimal(value: f64) -> String {
    let rounded = (value * 10.0).round() / 10.0;
    if rounded.fract().abs() < f64::EPSILON {
        format!("{}", rounded as i64)
    } else {
        format!("{rounded:.1}")
    }
}
