use super::*;

pub async fn scrape_torznab(
    http: &HttpClient,
    base_url: &str,
    req: &ScrapeRequest<'_>,
) -> anyhow::Result<riven_core::types::ScrapeResponse> {
    let url = format!("{base_url}v0/torznab/api");

    let mut params: Vec<(&str, String)> = vec![("o", "json".to_string())];

    match req.item_type {
        MediaItemType::Movie => {
            params.push(("t", "movie".to_string()));
            params.push(("cat", "2000".to_string()));
        }
        _ => {
            params.push(("t", "tvsearch".to_string()));
            params.push(("cat", "5000".to_string()));
            params.push(("season", req.season_or_1().to_string()));
            if let Some(ep) = req.episode {
                params.push(("ep", ep.to_string()));
            }
        }
    }

    if let Some(imdb_id) = req.imdb_id {
        params.push(("imdbid", imdb_id.to_string()));
    } else {
        params.push(("q", req.title.to_string()));
    }

    tracing::debug!(
        url = %url,
        imdb_id = req.imdb_id,
        title = req.title,
        season = req.season,
        episode = req.episode,
        "requesting stremthru torznab scrape"
    );

    let dedupe_params = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let response = http
        .send_data(PROFILE, Some(format!("{url}?{dedupe_params}")), |client| {
            client.get(&url).query(&params)
        })
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        anyhow::bail!("torznab request rejected: HTTP {} - {}", status, body);
    }

    let text = response.text()?;
    let resp: StremthruTorznabResponse = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid torznab response: {e}; body={text}"))?;

    let mut results = riven_core::types::ScrapeResponse::new();
    for item in resp.channel.items {
        let Some(info_hash) = item.attr.iter().find_map(|a| {
            if a.attributes.name == "infohash" {
                Some(a.attributes.value.to_lowercase())
            } else {
                None
            }
        }) else {
            continue;
        };
        if info_hash.is_empty() || item.title.is_empty() {
            continue;
        }
        let file_size_bytes = item.size.or_else(|| {
            item.attr.iter().find_map(|a| {
                if a.attributes.name == "size" {
                    a.attributes.value.parse::<u64>().ok()
                } else {
                    None
                }
            })
        });
        let entry = match file_size_bytes {
            Some(size) => riven_core::types::ScrapeEntry::with_size(item.title, size),
            None => riven_core::types::ScrapeEntry::new(item.title),
        };
        results.insert(info_hash, entry);
    }

    tracing::debug!(
        count = results.len(),
        imdb_id = req.imdb_id,
        title = req.title,
        "torznab scrape complete"
    );
    Ok(results)
}
