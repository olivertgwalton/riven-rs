use async_trait::async_trait;
use chrono::Utc;
use riven_db::repo;
use serde::Serialize;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

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

                let mut payload = NotificationPayload {
                    event: "riven.media-item.download.success".to_string(),
                    title: title.clone(),
                    full_title: full_title.clone().unwrap_or_else(|| title.clone()),
                    item_type: *item_type,
                    year: *year,
                    imdb_id: imdb_id.clone(),
                    tmdb_id: tmdb_id.clone(),
                    poster_path: poster_path.clone(),
                    downloader: plugin_name.clone(),
                    provider: provider.clone(),
                    duration_seconds: *duration_seconds,
                    timestamp: Utc::now().to_rfc3339(),
                };

                if !rewrite_for_request_root(ctx, *id, &mut payload).await? {
                    return Ok(HookResponse::Empty);
                }

                dispatch_webhooks(ctx, &urls, &payload).await;

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
    payload.poster_path = root_item.poster_path.clone();
    payload.duration_seconds = request
        .completed_at
        .unwrap_or_else(Utc::now)
        .signed_duration_since(request.created_at)
        .to_std()
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(payload.duration_seconds);
    Ok(true)
}

async fn dispatch_webhooks(ctx: &PluginContext, urls: &[String], payload: &NotificationPayload) {
    for url_str in urls {
        match parse_notification_url(url_str) {
            Some(NotificationService::Discord {
                webhook_id,
                webhook_token,
            }) => {
                if let Err(error) =
                    send_discord(&ctx.http_client, &webhook_id, &webhook_token, payload).await
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
    poster_path: Option<String>,
    downloader: String,
    provider: Option<String>,
    duration_seconds: f64,
    timestamp: String,
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
) -> anyhow::Result<()> {
    let url = format!("https://discord.com/api/webhooks/{webhook_id}/{webhook_token}");

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

    let body = serde_json::json!({ "embeds": [embed] });

    client
        .post(&url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;

    Ok(())
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
