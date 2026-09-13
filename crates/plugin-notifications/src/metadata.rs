use super::*;

pub(crate) async fn fetch_tmdb_overview(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    payload: &NotificationPayload,
) -> Option<String> {
    let tmdb_id = payload.tmdb_id.as_deref()?;
    let media_type = if payload.item_type == MediaItemType::Movie {
        "movie"
    } else {
        "tv"
    };
    let url = format!("{TMDB_BASE_URL}/{media_type}/{tmdb_id}");
    tracing::debug!(target_url = %url, tmdb_id, "fetching tmdb overview for notification");
    let resp = match http
        .get_json::<TmdbOverviewResponse, _>(TMDB_PROFILE, url.clone(), |client| {
            client.get(&url).bearer_auth(api_key)
        })
        .await
    {
        Ok(resp) => resp,
        Err(error) => {
            tracing::warn!(error = %error, target_url = %url, tmdb_id, "failed to fetch tmdb overview for notification");
            return None;
        }
    };
    resp.overview.filter(|s| !s.is_empty())
}

#[derive(Deserialize)]
struct TmdbOverviewResponse {
    overview: Option<String>,
}
