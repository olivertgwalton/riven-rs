use super::*;

pub async fn fetch_user_info(
    http: &HttpClient,
    redis: &redis::aio::ConnectionManager,
    base_url: &str,
    store: &str,
    api_key: &str,
) -> anyhow::Result<riven_core::types::DebridUserInfo> {
    let url = format!("{base_url}v0/store/user");
    let response = send_store_data(http, redis, store, format!("{store}:{url}"), |client| {
        client.get(&url).store_headers(store, api_key)
    })
    .await?;
    if !response.status().is_success() {
        anyhow::bail!(
            "store user request rejected: HTTP {} - {}",
            response.status(),
            response.text().unwrap_or_default()
        );
    }
    let resp: StremthruResponse<StremthruUser> = response.json()?;
    let user = resp
        .data
        .ok_or_else(|| anyhow::anyhow!("store returned no user data"))?;

    let extra = fetch_debrid_extra(http, store, api_key)
        .await
        .inspect_err(|e| tracing::debug!(store, error = %e, "could not fetch debrid extra info"))
        .ok()
        .unwrap_or_default();

    Ok(riven_core::types::DebridUserInfo {
        store: store.to_string(),
        email: user.email,
        username: extra.username,
        subscription_status: user.subscription_status,
        premium_until: extra.premium_until,
        cooldown_until: extra.cooldown_until,
        total_downloaded_bytes: extra.total_downloaded_bytes,
        points: extra.points,
    })
}

#[derive(Default)]
struct DebridExtra {
    premium_until: Option<String>,
    cooldown_until: Option<String>,
    total_downloaded_bytes: Option<i64>,
    username: Option<String>,
    points: Option<i64>,
}

async fn fetch_debrid_extra(
    http: &HttpClient,
    store: &str,
    api_key: &str,
) -> anyhow::Result<DebridExtra> {
    if store == "torbox" {
        let body: serde_json::Value = http
            .get_json(
                debrid_service(store),
                format!("{store}:https://api.torbox.app/v1/api/user/me"),
                |client| {
                    client
                        .get("https://api.torbox.app/v1/api/user/me")
                        .header("Authorization", format!("Bearer {api_key}"))
                },
            )
            .await?;
        let data = &body["data"];
        return Ok(DebridExtra {
            premium_until: data["premium_expires_at"].as_str().map(str::to_owned),
            cooldown_until: data["cooldown_until"].as_str().map(str::to_owned),
            total_downloaded_bytes: data["total_downloaded"].as_i64(),
            ..Default::default()
        });
    }

    if store == "realdebrid" {
        let body: serde_json::Value = http
            .get_json(
                debrid_service(store),
                format!("{store}:https://api.real-debrid.com/rest/1.0/user"),
                |client| {
                    client
                        .get("https://api.real-debrid.com/rest/1.0/user")
                        .header("Authorization", format!("Bearer {api_key}"))
                },
            )
            .await?;
        return Ok(DebridExtra {
            premium_until: body["expiration"].as_str().map(str::to_owned),
            username: body["username"].as_str().map(str::to_owned),
            points: body["points"].as_i64(),
            ..Default::default()
        });
    }

    let (url, bearer, pointer, is_unix): (String, Option<String>, &str, bool) = match store {
        "alldebrid" => (
            "https://api.alldebrid.com/v4/user".into(),
            Some(format!("Bearer {api_key}")),
            "/data/user/premiumUntil",
            true,
        ),
        "debridlink" => (
            "https://debrid-link.com/api/v2/account/infos".into(),
            Some(format!("Bearer {api_key}")),
            "/value/accountExpirationDate",
            true,
        ),
        "premiumize" => (
            format!("https://www.premiumize.me/api/account/info?apikey={api_key}"),
            None,
            "/premium_until",
            true,
        ),
        _ => return Ok(DebridExtra::default()),
    };

    let body: serde_json::Value = http
        .get_json(debrid_service(store), format!("{store}:{url}"), |client| {
            let request = client.get(&url);
            if let Some(token) = bearer.clone() {
                request.header("Authorization", token)
            } else {
                request
            }
        })
        .await?;

    let premium_until = body.pointer(pointer).and_then(|v| {
        if is_unix {
            v.as_i64()
                .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        } else {
            v.as_str().map(str::to_owned)
        }
    });

    Ok(DebridExtra {
        premium_until,
        ..Default::default()
    })
}
