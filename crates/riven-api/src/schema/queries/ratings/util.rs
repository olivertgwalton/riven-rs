use async_graphql::Context;
use riven_core::http::HttpClient;

use super::RatingScore;

pub(super) use crate::schema::helpers::{key, parse_id, required_media_type};

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
