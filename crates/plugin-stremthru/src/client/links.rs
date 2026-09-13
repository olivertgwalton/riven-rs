use super::*;

/// Outcome of a stream-link generation attempt against a single store.
pub enum GeneratedLink {
    /// The store minted a fresh stream URL.
    Link(String),
    /// The store reported the torrent is permanently gone (fatal HTTP status).
    /// Distinct from a transient error — the caller should blacklist, not retry.
    Dead,
}

pub async fn generate_link(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
    magnet: &str,
) -> anyhow::Result<GeneratedLink> {
    let kind = if magnet.contains("/store/newz/") {
        "newz"
    } else {
        "torz"
    };
    let url = format!("{base_url}v0/store/{kind}/link/generate");
    tracing::debug!(store, kind, url = %url, "generating stremthru link");
    let response = send_store(http, redis, store, None, |client| {
        store_headers(client.post(&url), store, api_key)
            .json(&serde_json::json!({ "link": magnet }))
    })
    .await?;
    let status = response.status();
    if !status.is_success() {
        if riven_core::stream_link::is_fatal_status_code(status.as_u16()) {
            tracing::warn!(store, %status, "store reports torrent is dead");
            return Ok(GeneratedLink::Dead);
        }
        let body = response.text().unwrap_or_default();
        anyhow::bail!("store rejected link generation: HTTP {} - {}", status, body);
    }

    let text = response.text()?;
    let resp: StremthruResponse<StremthruLink> = serde_json::from_str(&text)
        .map_err(|error| anyhow::anyhow!("invalid generate-link response: {error}; body={text}"))?;

    Ok(GeneratedLink::Link(
        resp.data
            .ok_or_else(|| anyhow::anyhow!("{}", describe_empty_link_response(&text)))?
            .link,
    ))
}

pub(super) fn describe_empty_link_response(body: &str) -> String {
    let error = StremthruErrorResponse::parse(body).error;
    match (error.code.as_str(), error.message.as_str()) {
        ("", "") => format!("store returned no link data; body={body}"),
        (code, "") => format!("store returned no link data: {code}; body={body}"),
        ("", message) => format!("store returned no link data: {message}"),
        (code, message) => format!("store returned no link data: {code} - {message}"),
    }
}
