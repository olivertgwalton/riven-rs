mod imdb;
mod rotten_tomatoes;
mod tmdb;
mod util;

#[cfg(test)]
mod tests;

use async_graphql::{Context, Error, Object, Result, SimpleObject};

use super::anilist::fetch_anilist_rating;
use util::{decimal, parse_id, required_media_type, score_item};

#[derive(Default)]
pub struct CoreRatingsQuery;

#[derive(SimpleObject, Clone)]
pub struct RatingScore {
    pub name: String,
    pub image: Option<String>,
    pub score: String,
    pub url: Option<String>,
}

#[derive(SimpleObject)]
pub struct RatingsResponse {
    pub scores: Vec<RatingScore>,
    pub tmdb_id: Option<i64>,
    pub anilist_id: Option<i64>,
    pub media_type: Option<String>,
    pub imdb_id: Option<String>,
}

#[Object]
impl CoreRatingsQuery {
    async fn ratings(
        &self,
        ctx: &Context<'_>,
        indexer: String,
        id: String,
        media_type: Option<String>,
    ) -> Result<RatingsResponse> {
        match util::key(&indexer).as_str() {
            "anilist" => anilist_ratings(ctx, parse_id(&id, "AniList")?).await,
            "tmdb" => tmdb_ratings(ctx, parse_id(&id, "TMDB")?, media_type.as_deref()).await,
            other => Err(Error::new(format!("Ratings are not supported for {other}"))),
        }
    }
}

async fn anilist_ratings(ctx: &Context<'_>, anilist_id: i32) -> Result<RatingsResponse> {
    let rating = fetch_anilist_rating(ctx, anilist_id).await?;
    let mut scores = Vec::new();

    if let Some(score) = rating.score.filter(|score| *score > 0.0) {
        scores.push(score_item(
            "anilist",
            "anilist.svg",
            decimal(score),
            format!("https://anilist.co/anime/{anilist_id}"),
        ));
    }

    Ok(RatingsResponse {
        scores,
        tmdb_id: None,
        anilist_id: Some(i64::from(anilist_id)),
        media_type: Some("anime".to_owned()),
        imdb_id: None,
    })
}

async fn tmdb_ratings(
    ctx: &Context<'_>,
    tmdb_id: i64,
    media_type: Option<&str>,
) -> Result<RatingsResponse> {
    let media_type = required_media_type(media_type)?;
    let details = tmdb::rating_details(ctx, media_type, tmdb_id).await?;
    let mut scores = Vec::with_capacity(4);

    if let Some(vote_average) = details.vote_average.filter(|score| *score > 0.0) {
        scores.push(score_item(
            "tmdb",
            "tmdb.svg",
            format!("{}%", (vote_average * 10.0).round() as i64),
            format!("https://www.themoviedb.org/{media_type}/{tmdb_id}"),
        ));
    }

    let imdb_id = details.imdb_id();
    let (imdb_score, rt_scores) = tokio::join!(
        imdb::rating(ctx, imdb_id.as_deref(), media_type),
        rotten_tomatoes::scores(ctx, media_type, details.title(), details.year())
    );

    scores.extend(imdb_score);
    scores.extend(rt_scores);

    Ok(RatingsResponse {
        scores,
        tmdb_id: Some(tmdb_id),
        anilist_id: None,
        media_type: Some(media_type.to_owned()),
        imdb_id,
    })
}
