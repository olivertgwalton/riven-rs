use async_graphql::Context;
use riven_core::http::profiles::HttpServiceProfile;
use serde::Deserialize;

use super::RatingScore;
use super::util::{decimal, optional_http, score_item};

const RADARR_IMDB_URL: &str = "https://api.radarr.video/v1/movie/imdb";
pub(super) const RADARR: HttpServiceProfile = HttpServiceProfile::new("radarr_public");

pub(super) async fn rating(
    ctx: &Context<'_>,
    imdb_id: Option<&str>,
    media_type: &str,
) -> Option<RatingScore> {
    if media_type != "movie" {
        return None;
    }

    let imdb_id = imdb_id?;
    let http = optional_http(ctx, "IMDb rating lookup")?;
    let movies: Vec<RadarrImdbResponse> = match http
        .get_json(RADARR, format!("radarr:imdb:{imdb_id}"), |client| {
            client
                .get(format!("{RADARR_IMDB_URL}/{imdb_id}"))
                .header("Accept", "application/json")
        })
        .await
    {
        Ok(movies) => movies,
        Err(error) => {
            tracing::warn!(%error, imdb_id, "IMDb rating lookup failed");
            return None;
        }
    };

    let rating = movies
        .into_iter()
        .find(|movie| movie.imdb_id == imdb_id)?
        .movie_ratings?
        .imdb?
        .value;

    Some(score_item(
        "imdb",
        "imdb.svg",
        decimal(rating),
        format!("https://www.imdb.com/title/{imdb_id}/"),
    ))
}

#[derive(Deserialize)]
struct RadarrImdbResponse {
    #[serde(rename = "ImdbId")]
    imdb_id: String,
    #[serde(rename = "MovieRatings")]
    movie_ratings: Option<RadarrMovieRatings>,
}

#[derive(Deserialize)]
struct RadarrMovieRatings {
    #[serde(rename = "Imdb")]
    imdb: Option<RadarrRating>,
}

#[derive(Deserialize)]
struct RadarrRating {
    #[serde(rename = "Value")]
    value: f64,
}
