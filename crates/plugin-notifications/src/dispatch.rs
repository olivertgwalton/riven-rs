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
                    tracing::error!(error = %error, url = url_str, "failed to send discord notification");
                }
            }
            Some(NotificationService::Json { url }) => {
                if let Err(error) = send_json_webhook(&ctx.http, &url, payload).await {
                    tracing::error!(error = %error, url = url_str, "failed to send json notification");
                }
            }
            Some(NotificationService::Ntfy {
                base_url,
                topic,
                auth,
                priority,
                tags,
            }) => {
                if let Err(error) = send_ntfy(
                    &ctx.http,
                    &base_url,
                    &topic,
                    &auth,
                    priority.as_deref(),
                    tags.as_deref(),
                    payload,
                )
                .await
                {
                    tracing::error!(error = %error, url = url_str, "failed to send ntfy notification");
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
    Json {
        url: String,
    },
    Ntfy {
        base_url: String,
        topic: String,
        auth: NtfyAuth,
        priority: Option<String>,
        tags: Option<String>,
    },
}

pub(crate) enum NtfyAuth {
    None,
    Basic { user: String, password: String },
    Token(String),
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
    } else if let Some(rest) = url.strip_prefix("json://") {
        Some(NotificationService::Json {
            url: format!("http://{rest}"),
        })
    } else if let Some(rest) = url.strip_prefix("jsons://") {
        Some(NotificationService::Json {
            url: format!("https://{rest}"),
        })
    } else if url.starts_with("ntfy://") || url.starts_with("ntfys://") {
        parse_ntfy_url(url)
    } else {
        None
    }
}

/// Apprise's ntfy scheme: `ntfy://topic` (public ntfy.sh), `ntfy://host/topic`
/// or `ntfys://host/topic` (self-hosted, http/https respectively), each
/// optionally prefixed with `user:pass@` (Basic auth) or `token@` (Bearer
/// auth). Only a single topic is supported — Apprise's `{topic1}/{topic2}`
/// multi-topic form is not, since without an explicit host there is no way
/// to tell a second topic segment apart from a hostname.
fn parse_ntfy_url(url: &str) -> Option<NotificationService> {
    let (secure_scheme, rest) = if let Some(rest) = url.strip_prefix("ntfys://") {
        (true, rest)
    } else {
        (false, url.strip_prefix("ntfy://")?)
    };

    let (rest, query) = match rest.split_once('?') {
        Some((path, q)) => (path, Some(q)),
        None => (rest, None),
    };

    let (auth_part, after_auth) = match rest.split_once('@') {
        Some((a, b)) => (Some(a), b),
        None => (None, rest),
    };

    let (base_url, topic) = match after_auth.split_once('/') {
        Some((host, topic)) if !host.is_empty() && !topic.is_empty() => {
            // ntfy.sh itself has no plaintext endpoint, so an explicit
            // `ntfy://ntfy.sh/topic` still needs to be forced to https.
            let scheme = if secure_scheme || host.eq_ignore_ascii_case("ntfy.sh") {
                "https"
            } else {
                "http"
            };
            (format!("{scheme}://{host}"), topic.to_string())
        }
        // A `/` is present but the host or topic on one side of it is empty
        // (e.g. `myhost/` or `/topic`) — not a valid self-hosted target.
        Some(_) => return None,
        None if !after_auth.is_empty() => ("https://ntfy.sh".to_string(), after_auth.to_string()),
        None => return None,
    };

    let auth = match auth_part {
        None => NtfyAuth::None,
        Some(a) => match a.split_once(':') {
            Some((user, password)) => NtfyAuth::Basic {
                user: user.to_string(),
                password: password.to_string(),
            },
            None => NtfyAuth::Token(a.to_string()),
        },
    };

    let mut priority = None;
    let mut tags = None;
    for pair in query.into_iter().flat_map(|q| q.split('&')) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "priority" if matches!(value, "max" | "high" | "low" | "min") => {
                priority = Some(value.to_string());
            }
            "tags" if !value.is_empty() => {
                tags = Some(value.to_string());
            }
            _ => {}
        }
    }

    Some(NotificationService::Ntfy {
        base_url,
        topic,
        auth,
        priority,
        tags,
    })
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

async fn send_ntfy(
    http: &riven_core::http::HttpClient,
    base_url: &str,
    topic: &str,
    auth: &NtfyAuth,
    priority: Option<&str>,
    tags: Option<&str>,
    payload: &NotificationPayload,
) -> anyhow::Result<()> {
    let url = format!("{base_url}/{topic}");
    let body = build_ntfy_body(payload);
    tracing::debug!(
        topic,
        title = %payload.full_title,
        "sending ntfy notification"
    );
    http.send(profiles::NTFY, |client| {
        let mut request = client.post(&url).json(&body);
        request = match auth {
            NtfyAuth::None => request,
            NtfyAuth::Basic { user, password } => request.basic_auth(user, Some(password)),
            NtfyAuth::Token(token) => request.bearer_auth(token),
        };
        if let Some(priority) = priority {
            request = request.header("X-Priority", priority);
        }
        if let Some(tags) = tags {
            request = request.header("X-Tags", tags);
        }
        request
    })
    .await?
    .error_for_status()?;
    Ok(())
}

pub(crate) fn build_ntfy_body(payload: &NotificationPayload) -> serde_json::Value {
    let mut body = serde_json::json!({
        "title": format!("Downloaded: {}", payload.full_title),
        "message": build_ntfy_message(payload),
    });
    if let Some(ref poster) = payload.poster_path {
        body["attach"] = serde_json::json!(poster);
    }
    body
}

/// ntfy's `message` field is plain text — no embed support — so this
/// condenses the same core fields `build_simple_embed` shows into one line.
fn build_ntfy_message(payload: &NotificationPayload) -> String {
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
