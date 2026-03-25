use async_trait::async_trait;
use chrono::Utc;
use serde::Serialize;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use riven_core::register_plugin;

#[derive(Default)]
pub struct NotificationsPlugin;

register_plugin!(NotificationsPlugin);

#[async_trait]
impl Plugin for NotificationsPlugin {
    fn name(&self) -> &'static str {
        "notifications"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
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
                .with_description("Comma-separated webhook URLs. Supports Discord and generic JSON endpoints."),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::MediaItemDownloadSuccess {
                id: _,
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

                let payload = NotificationPayload {
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

                for url_str in &urls {
                    match parse_notification_url(url_str) {
                        Some(NotificationService::Discord { webhook_id, webhook_token }) => {
                            if let Err(e) = send_discord(&ctx.http_client, &webhook_id, &webhook_token, &payload).await {
                                tracing::error!(error = %e, url = url_str, "failed to send discord notification");
                            }
                        }
                        Some(NotificationService::Json { url }) => {
                            if let Err(e) = send_json_webhook(&ctx.http_client, &url, &payload).await {
                                tracing::error!(error = %e, url = url_str, "failed to send json notification");
                            }
                        }
                        None => {
                            tracing::warn!(url = url_str, "unsupported notification URL scheme");
                        }
                    }
                }

                Ok(HookResponse::Empty)
            }
            _ => Ok(HookResponse::Empty),
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
        // discord://webhookId/webhookToken
        let rest = url.strip_prefix("discord://")?;
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
    let url = format!(
        "https://discord.com/api/webhooks/{webhook_id}/{webhook_token}"
    );

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
        fields.push(serde_json::json!({ "name": "Year", "value": year.to_string(), "inline": true }));
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
        .await?;

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
        .await?;
    Ok(())
}

fn format_duration(seconds: f64) -> String {
    let total = seconds as u64;
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let secs = total % 60;

    if hours > 0 {
        format!("{hours}h {minutes}m {secs}s")
    } else if minutes > 0 {
        format!("{minutes}m {secs}s")
    } else {
        format!("{secs}s")
    }
}
