use async_trait::async_trait;
use chrono::Utc;
use riven_db::repo;
use serde::{Deserialize, Serialize};

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

const TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";
const TVDB_BASE_URL: &str = "https://api4.thetvdb.com/v4";
const TVDB_DEFAULT_API_KEY: &str = "6be85335-5c4f-4d8d-b945-d3ed0eb8cdce";

#[derive(Default)]
pub struct NotificationsPlugin;

register_plugin!(NotificationsPlugin);

#[async_trait]
impl Plugin for NotificationsPlugin {
    fn name(&self) -> &'static str {
        "notifications"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemDownloadSuccess]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        let urls = settings.get_list("urls");
        Ok(!urls.is_empty())
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("urls", "Webhook URLs", "textarea")
                .required()
                .with_placeholder("https://discord.com/api/webhooks/...")
                .with_description(
                    "Comma-separated webhook URLs. Supports Discord and generic JSON endpoints.",
                ),
            SettingField::new("detailed", "Detailed Embeds", "boolean").with_description(
                "Show rich Discord embeds with overview, rating, and external links.",
            ),
            SettingField::new("tmdb_api_key", "TMDB API Key", "password").with_description(
                "Optional. Required for overview text in detailed Discord embeds.",
            ),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::MediaItemDownloadSuccess {
                id,
                title,
                full_title,
                item_type,
                year,
                imdb_id,
                tmdb_id,
                poster_path,
                plugin_name,
                provider,
                duration_seconds,
            } => {
                let urls = ctx.settings.get_list("urls");
                let detailed = ctx.settings.get_bool("detailed");

                let mut payload = NotificationPayload {
                    event: "riven.media-item.download.success".to_string(),
                    title: title.clone(),
                    full_title: full_title.clone().unwrap_or_else(|| title.clone()),
                    item_type: *item_type,
                    year: *year,
                    imdb_id: imdb_id.clone(),
                    tmdb_id: tmdb_id.clone(),
                    tvdb_id: None,
                    poster_path: poster_path.clone(),
                    downloader: plugin_name.clone(),
                    provider: provider.clone(),
                    duration_seconds: *duration_seconds,
                    timestamp: Utc::now().to_rfc3339(),
                    is_anime: false,
                    rating: None,
                    overview: None,
                    tvdb_slug: None,
                };

                if !rewrite_for_request_root(ctx, *id, &mut payload).await? {
                    return Ok(HookResponse::Empty);
                }

                if detailed {
                    if let Some(api_key) = ctx.settings.get("tmdb_api_key") {
                        payload.overview =
                            fetch_tmdb_overview(&ctx.http_client, api_key, &payload).await;
                    }
                    if let Some(ref tvdb_id) = payload.tvdb_id.clone() {
                        payload.tvdb_slug = fetch_tvdb_slug(&ctx.http_client, tvdb_id).await;
                    }
                }

                dispatch_webhooks(ctx, &urls, &payload, detailed).await;

                Ok(HookResponse::Empty)
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}

async fn rewrite_for_request_root(
    ctx: &PluginContext,
    item_id: i64,
    payload: &mut NotificationPayload,
) -> anyhow::Result<bool> {
    let Some(item) = repo::get_media_item(&ctx.db_pool, item_id).await? else {
        return Ok(true);
    };

    payload.is_anime = item.is_anime;
    payload.rating = item.rating;
    payload.tvdb_id = item.tvdb_id.clone();

    let Some(request_id) = item.item_request_id else {
        return Ok(true);
    };
    let Some(request) = repo::get_item_request_by_id(&ctx.db_pool, request_id).await? else {
        return Ok(false);
    };
    if request.state != ItemRequestState::Completed {
        return Ok(false);
    }
    let Some(root_item) = repo::get_request_root_item(&ctx.db_pool, request_id).await? else {
        return Ok(false);
    };
    if !mark_request_notification_sent(ctx, request_id).await? {
        return Ok(false);
    }

    payload.title = root_item.title.clone();
    payload.full_title = root_item
        .full_title
        .clone()
        .unwrap_or_else(|| root_item.title.clone());
    payload.item_type = root_item.item_type;
    payload.year = root_item.year;
    payload.imdb_id = root_item.imdb_id.clone();
    payload.tmdb_id = root_item.tmdb_id.clone();
    payload.tvdb_id = root_item.tvdb_id.clone();
    payload.poster_path = root_item.poster_path.clone();
    payload.is_anime = root_item.is_anime;
    payload.rating = root_item.rating;
    payload.duration_seconds = request
        .completed_at
        .unwrap_or_else(Utc::now)
        .signed_duration_since(request.created_at)
        .to_std()
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(payload.duration_seconds);
    Ok(true)
}

async fn fetch_tmdb_overview(
    client: &reqwest::Client,
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
    let resp = client.get(&url).bearer_auth(api_key).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let json: TmdbOverviewResponse = resp.json().await.ok()?;
    json.overview.filter(|s| !s.is_empty())
}

async fn dispatch_webhooks(
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
                if let Err(error) = send_discord(
                    &ctx.http_client,
                    &webhook_id,
                    &webhook_token,
                    payload,
                    detailed,
                )
                .await
                {
                    tracing::error!(error = %error, url = url_str, "failed to send discord notification");
                }
            }
            Some(NotificationService::Json { url }) => {
                if let Err(error) = send_json_webhook(&ctx.http_client, &url, payload).await {
                    tracing::error!(error = %error, url = url_str, "failed to send json notification");
                }
            }
            None => {
                tracing::warn!(url = url_str, "unsupported notification URL scheme");
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct NotificationPayload {
    event: String,
    title: String,
    full_title: String,
    item_type: MediaItemType,
    year: Option<i32>,
    imdb_id: Option<String>,
    tmdb_id: Option<String>,
    tvdb_id: Option<String>,
    poster_path: Option<String>,
    downloader: String,
    provider: Option<String>,
    duration_seconds: f64,
    timestamp: String,
    is_anime: bool,
    rating: Option<f64>,
    overview: Option<String>,
    #[serde(skip)]
    tvdb_slug: Option<String>,
}

async fn mark_request_notification_sent(
    ctx: &PluginContext,
    request_id: i64,
) -> anyhow::Result<bool> {
    let key = format!("riven:notifications:request-complete:{request_id}");
    let mut conn = ctx.redis.clone();
    let result: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(60 * 60 * 24 * 30)
        .query_async(&mut conn)
        .await?;
    Ok(result.is_some())
}

enum NotificationService {
    Discord {
        webhook_id: String,
        webhook_token: String,
    },
    Json {
        url: String,
    },
}

fn parse_notification_url(url: &str) -> Option<NotificationService> {
    if url.starts_with("discord://") {
        let rest = url.strip_prefix("discord://")?;
        let (webhook_id, webhook_token) = rest.split_once('/')?;
        Some(NotificationService::Discord {
            webhook_id: webhook_id.to_string(),
            webhook_token: webhook_token.to_string(),
        })
    } else if url.starts_with("https://discord.com/api/webhooks/") {
        let rest = url.strip_prefix("https://discord.com/api/webhooks/")?;
        let (webhook_id, webhook_token) = rest.split_once('/')?;
        Some(NotificationService::Discord {
            webhook_id: webhook_id.to_string(),
            webhook_token: webhook_token.to_string(),
        })
    } else if url.starts_with("json://") {
        let rest = url.strip_prefix("json://")?;
        Some(NotificationService::Json {
            url: format!("http://{rest}"),
        })
    } else if url.starts_with("jsons://") {
        let rest = url.strip_prefix("jsons://")?;
        Some(NotificationService::Json {
            url: format!("https://{rest}"),
        })
    } else {
        None
    }
}

async fn send_discord(
    client: &reqwest::Client,
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
    client
        .post(&url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn build_simple_embed(payload: &NotificationPayload) -> serde_json::Value {
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
    client: &reqwest::Client,
    url: &str,
    payload: &NotificationPayload,
) -> anyhow::Result<()> {
    client
        .post(url)
        .json(payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn format_duration(seconds: f64) -> String {
    let total = seconds.round().max(0.0) as u64;
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

async fn fetch_tvdb_slug(client: &reqwest::Client, tvdb_id: &str) -> Option<String> {
    let login: TvdbResponse<TvdbLoginData> = client
        .post(format!("{TVDB_BASE_URL}/login"))
        .json(&serde_json::json!({ "apikey": TVDB_DEFAULT_API_KEY }))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let token = login.data.token;

    let resp: TvdbResponse<TvdbSeriesSlug> = client
        .get(format!("{TVDB_BASE_URL}/series/{tvdb_id}"))
        .bearer_auth(&token)
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

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
