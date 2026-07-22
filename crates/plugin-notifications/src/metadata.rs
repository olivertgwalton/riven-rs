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

pub(crate) async fn fetch_tvdb_slug(
    http: &riven_core::http::HttpClient,
    tvdb_id: &str,
) -> Option<String> {
    let login_url = format!("{TVDB_BASE_URL}/login");
    tracing::debug!(target_url = %login_url, tvdb_id, "logging in to tvdb for notification slug lookup");
    let login: TvdbResponse<TvdbLoginData> = match http
        .send(TVDB_PROFILE, |client| {
            client
                .post(format!("{TVDB_BASE_URL}/login"))
                .json(&serde_json::json!({ "apikey": TVDB_DEFAULT_API_KEY }))
        })
        .await
    {
        Ok(resp) => match resp.json().await {
            Ok(json) => json,
            Err(error) => {
                tracing::warn!(error = %error, target_url = %login_url, tvdb_id, "failed to decode tvdb login response");
                return None;
            }
        },
        Err(error) => {
            tracing::warn!(error = %error, target_url = %login_url, tvdb_id, "failed to login to tvdb for notification slug lookup");
            return None;
        }
    };

    let token = login.data.token;
    let series_url = format!("{TVDB_BASE_URL}/series/{tvdb_id}");
    tracing::debug!(target_url = %series_url, tvdb_id, "fetching tvdb slug for notification");

    let resp: TvdbResponse<TvdbSeriesSlug> = match http
        .get_json(TVDB_PROFILE, series_url.clone(), |client| {
            client.get(&series_url).bearer_auth(&token)
        })
        .await
    {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!(error = %error, target_url = %series_url, tvdb_id, "failed to fetch tvdb slug for notification");
            return None;
        }
    };

    resp.data.slug
}

#[derive(Deserialize)]
struct TvdbResponse<T> {
    data: T,
}

#[derive(Deserialize)]
struct TvdbLoginData {
    token: String,
}

#[derive(Deserialize)]
struct TvdbSeriesSlug {
    slug: Option<String>,
}

#[derive(Deserialize)]
struct TmdbOverviewResponse {
    overview: Option<String>,
}
