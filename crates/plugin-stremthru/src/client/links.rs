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
    let response = match send_store(http, redis, store, |client| {
        client
            .post(&url)
            .store_headers(store, api_key)
            .json(&serde_json::json!({ "link": magnet }))
    })
    .await?
    {
        StoreSend::Ok(response) => response,
        StoreSend::Rejected { status, body } => {
            if riven_core::stream_link::is_fatal_status_code(status.as_u16()) {
                tracing::warn!(store, %status, "store reports torrent is dead");
                return Ok(GeneratedLink::Dead);
            }
            anyhow::bail!("store rejected link generation: HTTP {} - {}", status, body);
        }
    };

    let text = response.text().await?;
    let resp: StremthruResponse<StremthruLink> = serde_json::from_str(&text)
        .map_err(|error| anyhow::anyhow!("invalid generate-link response: {error}; body={text}"))?;

    Ok(GeneratedLink::Link(
        resp.data
            .ok_or_else(|| anyhow::anyhow!("{}", describe_empty_link_response(&text)))?
            .link,
    ))
}

pub(super) fn describe_empty_link_response(body: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(value) => {
            let code = value
                .pointer("/error/code")
                .and_then(serde_json::Value::as_str);
            let message = value
                .pointer("/error/message")
                .and_then(serde_json::Value::as_str);

            match (code, message) {
                (Some(code), Some(message)) => {
                    format!("store returned no link data: {code} - {message}")
                }
                (Some(code), None) => format!("store returned no link data: {code}; body={body}"),
                (None, Some(message)) => format!("store returned no link data: {message}"),
                (None, None) => format!("store returned no link data; body={body}"),
            }
        }
        Err(_) => format!("store returned no link data; body={body}"),
    }
}
