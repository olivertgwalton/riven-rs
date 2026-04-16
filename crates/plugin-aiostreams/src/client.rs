use std::collections::HashMap;

use reqwest::StatusCode;
use riven_core::events::ScrapeRequest;
use riven_core::http::{HttpClient, profiles};
use riven_core::types::MediaItemType;

use crate::models::AioStreamsResponse;

pub async fn validate_search(
    http: &HttpClient,
    base_url: &str,
    uuid: &str,
    password: &str,
) -> anyhow::Result<bool> {
    let url = format!("{base_url}/api/v1/search");
    let resp = http
        .send(profiles::AIOSTREAMS, |client| {
            client.get(&url).basic_auth(uuid, Some(password)).query(&[
                ("type", "movie"),
                ("id", "tt0111161"),
                ("requiredFields", "infoHash"),
            ])
        })
        .await;

    match resp {
        Ok(resp) => {
            if !resp.status().is_success() {
                return Ok(false);
            }
            let payload: AioStreamsResponse = match resp.json().await {
                Ok(payload) => payload,
                Err(_) => return Ok(false),
            };
            Ok(payload.success && payload.data.is_some())
        }
        Err(_) => Ok(false),
    }
}

pub async fn scrape(
    http: &HttpClient,
    base_url: &str,
    uuid: &str,
    password: &str,
    request: &ScrapeRequest<'_>,
) -> anyhow::Result<HashMap<String, String>> {
    let Some(id) = request_identifier(request) else {
        tracing::debug!(
            title = request.title,
            "aiostreams scrape skipped: missing identifier"
        );
        return Ok(HashMap::new());
    };

    let url = format!("{base_url}/api/v1/search");
    let media_type = request_media_type(request.item_type);

    tracing::debug!(
        url = %url,
        id,
        media_type,
        title = request.title,
        "requesting aiostreams search"
    );
    let http_resp = http
        .send_data(
            profiles::AIOSTREAMS,
            Some(format!("{url}:{media_type}:{id}")),
            |client| {
                client.get(&url).basic_auth(uuid, Some(password)).query(&[
                    ("type", media_type),
                    ("id", id.as_str()),
                    ("requiredFields", "infoHash"),
                    ("format", "true"),
                ])
            },
        )
        .await?;

    let status = http_resp.status();
    if !status.is_success() {
        let body = http_resp.text().unwrap_or_default();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            anyhow::bail!("aiostreams authentication failed: HTTP {status}");
        }
        anyhow::bail!(
            "aiostreams returned HTTP {status}: {}",
            body.chars().take(200).collect::<String>()
        );
    }

    let resp: AioStreamsResponse = http_resp
        .json()
        .map_err(|e| anyhow::anyhow!("aiostreams response parse error for {url}: {e}"))?;

    if !resp.success {
        let message = resp.error_message().unwrap_or("unknown aiostreams error");
        anyhow::bail!("aiostreams search failed: {message}");
    }

    let mut results = HashMap::new();
    let mut raw_result_count = 0usize;
    let mut missing_info_hash_count = 0usize;
    let mut missing_title_count = 0usize;
    if let Some(data) = resp.data {
        for stream in data.results {
            raw_result_count += 1;
            let Some(info_hash) = stream.info_hash else {
                missing_info_hash_count += 1;
                continue;
            };
            let title = stream
                .folder_name
                .or(stream.filename)
                .or(stream.name)
                .or_else(|| stream.description.and_then(first_description_line))
                .unwrap_or_default()
                .trim()
                .to_string();

            if title.is_empty() {
                missing_title_count += 1;
                continue;
            }

            results.insert(info_hash.to_lowercase(), title);
        }
    }

    tracing::info!(
        count = results.len(),
        raw_result_count,
        missing_info_hash_count,
        missing_title_count,
        id,
        title = request.title,
        "aiostreams scrape complete"
    );
    Ok(results)
}

fn request_media_type(item_type: MediaItemType) -> &'static str {
    match item_type {
        MediaItemType::Movie => "movie",
        MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode => "series",
    }
}

fn request_identifier(request: &ScrapeRequest<'_>) -> Option<String> {
    let imdb_id = request.imdb_id?.to_string();
    Some(match request.item_type {
        MediaItemType::Movie => imdb_id,
        MediaItemType::Show => format!("{imdb_id}:1:1"),
        MediaItemType::Season => format!("{imdb_id}:{}", request.season_or_1()),
        MediaItemType::Episode => {
            format!(
                "{imdb_id}:{}:{}",
                request.season_or_1(),
                request.episode_or_1()
            )
        }
    })
}

fn first_description_line(description: String) -> Option<String> {
    description
        .lines()
        .next()
        .map(str::trim)
        .map(|line| line.trim_start_matches(|c: char| !c.is_alphanumeric()))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests;
