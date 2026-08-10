use super::*;

pub(crate) async fn dispatch_webhooks(
    ctx: &PluginContext,
    urls: &[String],
    payload: &NotificationPayload,
    detailed: bool,
) {
    for url_str in urls {
        match parse_notification_url(url_str) {
            Some(NotificationService::Discord {
                webhook_id,
                webhook_token,
            }) => {
                if let Err(error) =
                    send_discord(&ctx.http, &webhook_id, &webhook_token, payload, detailed).await
                {
                    // Never log `url_str` here — it's the full discord.com
                    // webhook URL, which is itself a bearer credential.
                    tracing::error!(error = %error, service = "discord", "failed to send discord notification");
                }
            }
            Some(NotificationService::Pushbullet { access_token }) => {
                if let Err(error) = send_pushbullet(&ctx.http, &access_token, payload).await {
                    // Never log `url_str` here — it embeds the Pushbullet
                    // access token, which grants full account access.
                    tracing::error!(error = %error, service = "pushbullet", "failed to send pushbullet notification");
                }
            }
            Some(NotificationService::Json { url }) => {
                if let Err(error) = send_json_webhook(&ctx.http, &url, payload).await {
                    // Generic webhook URLs commonly embed a secret path
                    // segment too, so this omits `url_str`/`url` as well.
                    tracing::error!(error = %error, service = "json", "failed to send json notification");
                }
            }
            None => {
                tracing::warn!(url = url_str, "unsupported notification URL scheme");
            }
        }
    }
}

pub(crate) enum NotificationService {
    Discord {
        webhook_id: String,
        webhook_token: String,
    },
    Pushbullet {
        access_token: String,
    },
    Json {
        url: String,
    },
}

pub(crate) fn parse_notification_url(url: &str) -> Option<NotificationService> {
    if let Some(rest) = url
        .strip_prefix("discord://")
        .or_else(|| url.strip_prefix("https://discord.com/api/webhooks/"))
    {
        let (webhook_id, webhook_token) = rest.split_once('/')?;
        Some(NotificationService::Discord {
            webhook_id: webhook_id.to_string(),
            webhook_token: webhook_token.to_string(),
        })
    } else if let Some(rest) = url.strip_prefix("pbul://") {
        // Apprise's pushbullet scheme also allows `pbul://token/#channel`,
        // `.../DEVICE_ID`, `.../email@address` for targeted delivery — not
        // supported here, so a trailing path segment is dropped rather than
        // treated as part of the token, matching a plain-token-only client.
        // A missing/empty token (`pbul://`, `pbul:///device-id`) is rejected
        // outright rather than reaching send_pushbullet with an empty
        // Access-Token header.
        let token = rest.split('/').next().filter(|token| !token.is_empty())?;
        Some(NotificationService::Pushbullet {
            access_token: token.to_string(),
        })
    } else if let Some(rest) = url.strip_prefix("json://") {
        Some(NotificationService::Json {
            url: format!("http://{rest}"),
        })
    } else {
        url.strip_prefix("jsons://")
            .map(|rest| NotificationService::Json {
                url: format!("https://{rest}"),
            })
    }
}

async fn send_discord(
    http: &riven_core::http::HttpClient,
    webhook_id: &str,
    webhook_token: &str,
    payload: &NotificationPayload,
    detailed: bool,
) -> anyhow::Result<()> {
    let url = format!("https://discord.com/api/webhooks/{webhook_id}/{webhook_token}");
    let body = if detailed {
        build_detailed_embed(payload)
    } else {
        build_simple_embed(payload)
    };
    tracing::debug!(
        webhook_id,
        title = %payload.full_title,
        "sending discord notification webhook"
    );
    http.send(profiles::DISCORD_WEBHOOK, |client| {
        client.post(&url).json(&body)
    })
    .await?
    .error_for_status()?;
    Ok(())
}

pub(crate) fn build_simple_embed(payload: &NotificationPayload) -> serde_json::Value {
    let duration_str = format_duration(payload.duration_seconds);

    let mut fields = vec![
        serde_json::json!({ "name": "Type", "value": format!("{:?}", payload.item_type), "inline": true }),
        serde_json::json!({ "name": "Downloader", "value": &payload.downloader, "inline": true }),
        serde_json::json!({ "name": "Duration", "value": duration_str, "inline": true }),
    ];

    if let Some(ref provider) = payload.provider {
        fields.push(serde_json::json!({ "name": "Provider", "value": provider, "inline": true }));
    }

    if let Some(year) = payload.year {
        fields
            .push(serde_json::json!({ "name": "Year", "value": year.to_string(), "inline": true }));
    }

    let mut embed = serde_json::json!({
        "title": format!("Downloaded: {}", payload.full_title),
        "color": 0x2ecc71,
        "fields": fields,
        "timestamp": &payload.timestamp,
    });

    if let Some(ref poster) = payload.poster_path {
        embed["thumbnail"] = serde_json::json!({ "url": poster });
    }

    serde_json::json!({ "embeds": [embed] })
}

fn build_detailed_embed(payload: &NotificationPayload) -> serde_json::Value {
    let media_label = if payload.is_anime {
        "Anime"
    } else {
        match payload.item_type {
            MediaItemType::Movie => "Movie",
            MediaItemType::Show => "Show",
            MediaItemType::Season => "Season",
            MediaItemType::Episode => "Episode",
        }
    };

    let color: u32 = if payload.is_anime {
        0x9B59B6
    } else {
        match payload.item_type {
            MediaItemType::Movie => 0xE67E22,
            MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode => 0x3498DB,
        }
    };

    let title = match payload.year {
        Some(year) => format!("{} ({})", payload.full_title, year),
        None => payload.full_title.clone(),
    };

    let description = match &payload.overview {
        Some(overview) => {
            let truncated = if overview.len() > 500 {
                &overview[..500]
            } else {
                overview.as_str()
            };
            format!("**✅ {media_label} Completed Successfully**\n\n{truncated}")
        }
        None => format!("**✅ {media_label} Completed Successfully**"),
    };

    let mut fields = vec![];

    if let Some(rating) = payload.rating {
        fields.push(serde_json::json!({
            "name": "⭐ Rating",
            "value": format!("{:.2} / 10", rating),
            "inline": false,
        }));
    }

    fields.push(serde_json::json!({
        "name": "⏱ Completion Time",
        "value": format_duration(payload.duration_seconds),
        "inline": false,
    }));

    fields.push(serde_json::json!({
        "name": "Downloader",
        "value": &payload.downloader,
        "inline": true,
    }));

    if let Some(ref provider) = payload.provider {
        fields.push(serde_json::json!({
            "name": "Provider",
            "value": provider,
            "inline": true,
        }));
    }

    let mut links = vec![];
    if let Some(ref tmdb_id) = payload.tmdb_id {
        let path = if payload.item_type == MediaItemType::Movie {
            "movie"
        } else {
            "tv"
        };
        links.push(format!(
            "[TMDB](https://www.themoviedb.org/{path}/{tmdb_id})"
        ));
    }
    if let Some(ref imdb_id) = payload.imdb_id {
        links.push(format!("[IMDB](https://www.imdb.com/title/{imdb_id})"));
    }
    if let Some(ref tvdb_slug) = payload.tvdb_slug {
        links.push(format!("[TVDB](https://thetvdb.com/series/{tvdb_slug})"));
    }
    if !links.is_empty() {
        fields.push(serde_json::json!({
            "name": "Links",
            "value": links.join(" • "),
            "inline": false,
        }));
    }

    let mut embed = serde_json::json!({
        "title": title,
        "description": description,
        "color": color,
        "fields": fields,
        "timestamp": &payload.timestamp,
        "footer": { "text": "Riven" },
    });

    if let Some(ref poster) = payload.poster_path {
        embed["image"] = serde_json::json!({ "url": poster });
    }

    serde_json::json!({ "embeds": [embed] })
}

async fn send_pushbullet(
    http: &riven_core::http::HttpClient,
    access_token: &str,
    payload: &NotificationPayload,
) -> anyhow::Result<()> {
    let title = format!("Downloaded: {}", payload.full_title);
    let body = build_pushbullet_body(payload);
    tracing::debug!(
        title = %payload.full_title,
        "sending pushbullet notification"
    );
    http.send(profiles::PUSHBULLET, |client| {
        client
            .post("https://api.pushbullet.com/v2/pushes")
            .header("Access-Token", access_token)
            .json(&serde_json::json!({
                "type": "note",
                "title": title,
                "body": body,
            }))
    })
    .await?
    .error_for_status()?;
    Ok(())
}

/// Pushbullet notes are plain text — no embed support — so this condenses
/// the same core fields `build_simple_embed` shows into one line.
pub(crate) fn build_pushbullet_body(payload: &NotificationPayload) -> String {
    let mut parts = vec![format!("{:?}", payload.item_type)];
    if let Some(year) = payload.year {
        parts.push(year.to_string());
    }
    parts.push(format!("via {}", payload.downloader));
    if let Some(ref provider) = payload.provider {
        parts.push(provider.clone());
    }
    parts.push(format!("in {}", format_duration(payload.duration_seconds)));
    parts.join(" • ")
}

async fn send_json_webhook(
    http: &riven_core::http::HttpClient,
    url: &str,
    payload: &NotificationPayload,
) -> anyhow::Result<()> {
    tracing::debug!(
        target_url = %url,
        title = %payload.full_title,
        "sending json notification webhook"
    );
    http.send(profiles::WEBHOOK_JSON, |client| {
        client.post(url).json(payload)
    })
    .await?
    .error_for_status()?;
    Ok(())
}

pub(crate) fn format_duration(seconds: f64) -> String {
    let total = u64::try_from(seconds.round().max(0.0) as i64).unwrap_or(0);
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let secs = total % 60;

    if hours > 0 {
        format!("{hours}h {minutes}m {secs}s")
    } else if minutes > 0 {
        format!("{minutes}m {secs}s")
    } else {
        format!("{seconds:.1}s")
    }
}
