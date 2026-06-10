//! Outbound webhook bus.
//!
//! Subscribes to every notable state transition (`EventType::NOTABLE`) and
//! POSTs a signed JSON envelope to each configured URL. Unlike the
//! `notifications` plugin — which renders one Discord-flavoured payload on
//! download success — this emits the raw `RivenEvent` for the whole notable
//! stream, so self-hosters can wire Riven into Home Assistant, ntfy, n8n, or
//! their own automation.
//!
//! Delivery is at-least-once: the shared HTTP client retries transient
//! failures per its profile, and a delivery that still fails is recorded in a
//! capped Redis dead-letter list rather than silently dropped. Consumers
//! should dedupe on the envelope `id`; there is no ordering guarantee across
//! events (broadcast hook jobs run concurrently).

use async_trait::async_trait;
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use serde_json::json;
use sha2::Sha256;
use ulid::Ulid;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::http::profiles;
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_db::repo;

type HmacSha256 = Hmac<Sha256>;

/// Redis list holding the most recent failed deliveries (newest first).
const DEAD_LETTER_KEY: &str = "riven:webhooks:dead";
/// Cap on retained dead-letter entries — a diagnostic tail, not a queue.
const DEAD_LETTER_MAX: isize = 100;

#[derive(Default)]
pub struct WebhooksPlugin;

register_plugin!(WebhooksPlugin);

#[async_trait]
impl Plugin for WebhooksPlugin {
    fn name(&self) -> &'static str {
        "webhooks"
    }

    fn subscribed_events(&self) -> &[EventType] {
        EventType::NOTABLE
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(!settings.get_list("urls").is_empty())
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("urls", "Webhook URLs", "textarea")
                .required()
                .with_placeholder("https://example.com/hook, https://ntfy.sh/riven")
                .with_description(
                    "Comma-separated endpoints. Each receives a POST with the signed event \
                     envelope as a JSON body.",
                ),
            SettingField::new("events", "Event Filter", "textarea")
                .with_placeholder(
                    "riven.media-item.download.success, riven.media-item.download.error",
                )
                .with_description(
                    "Optional comma-separated event slugs to deliver. Leave empty to send every \
                     notable event (downloads, scrapes, index results, request changes).",
                ),
            SettingField::new("secret", "Signing Secret", "password").with_description(
                "Optional HMAC-SHA256 key. When set, each request carries an \
                 `X-Riven-Signature: sha256=<hex>` header over the raw body so consumers can \
                 verify authenticity.",
            ),
            SettingField::new("enrich", "Enrich Payload", "boolean").with_description(
                "Attach the full media item (title, artwork, IDs, rating, state) under `item`. \
                 Adds a database lookup per delivery.",
            ),
        ]
    }

    async fn on_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let urls = ctx.settings.get_list("urls");
        if urls.is_empty() {
            return Ok(HookResponse::Empty);
        }

        let slug = event.event_type().slug();

        let filter = ctx.settings.get_list("events");
        if !should_deliver(&filter, slug) {
            return Ok(HookResponse::Empty);
        }

        let delivery_id = Ulid::new().to_string();
        let media_item_id = event.media_item_id();
        let item = if ctx.settings.get_bool("enrich") {
            enrich(ctx, media_item_id).await
        } else {
            None
        };

        let envelope = json!({
            "id": delivery_id,
            "event": slug,
            "timestamp": Utc::now().to_rfc3339(),
            "media_item_id": media_item_id,
            "data": event,
            "item": item,
        });

        // Serialize once: the exact bytes are what we sign and what we send, so
        // a consumer's HMAC check sees the same body we hashed.
        let body = match serde_json::to_vec(&envelope) {
            Ok(body) => body,
            Err(error) => {
                tracing::error!(error = %error, event = slug, "failed to serialize webhook envelope");
                return Ok(HookResponse::Empty);
            }
        };

        let signature = ctx.settings.get("secret").map(|secret| sign(secret, &body));

        for url in &urls {
            deliver(ctx, url, &body, slug, &delivery_id, signature.as_deref()).await;
        }

        Ok(HookResponse::Empty)
    }
}

async fn deliver(
    ctx: &PluginContext,
    url: &str,
    body: &[u8],
    slug: &str,
    delivery_id: &str,
    signature: Option<&str>,
) {
    tracing::debug!(target_url = %url, event = slug, delivery_id, "sending webhook");
    let result = ctx
        .http
        .send(profiles::WEBHOOK_JSON, |client| {
            let mut req = client
                .post(url)
                .header("content-type", "application/json")
                .header("user-agent", "Riven-Webhooks/1")
                .header("x-riven-event", slug)
                .header("x-riven-delivery", delivery_id)
                .body(body.to_vec());
            if let Some(sig) = signature {
                req = req.header("x-riven-signature", format!("sha256={sig}"));
            }
            req
        })
        .await;

    match result {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let status = resp.status();
            tracing::warn!(target_url = %url, event = slug, delivery_id, %status, "webhook rejected");
            dead_letter(ctx, url, slug, delivery_id, &format!("http {status}")).await;
        }
        Err(error) => {
            tracing::error!(target_url = %url, event = slug, delivery_id, error = %error, "webhook delivery failed");
            dead_letter(ctx, url, slug, delivery_id, &error.to_string()).await;
        }
    }
}

/// Record a failed delivery in a capped Redis list so failures stay visible
/// (for a future "webhook deliveries" view) without unbounded growth.
async fn dead_letter(ctx: &PluginContext, url: &str, slug: &str, delivery_id: &str, reason: &str) {
    let record = json!({
        "url": url,
        "event": slug,
        "id": delivery_id,
        "reason": reason,
        "at": Utc::now().to_rfc3339(),
    })
    .to_string();

    let mut conn = ctx.redis.clone();
    let result: redis::RedisResult<()> = redis::pipe()
        .lpush(DEAD_LETTER_KEY, record)
        .ignore()
        .ltrim(DEAD_LETTER_KEY, 0, DEAD_LETTER_MAX - 1)
        .ignore()
        .query_async(&mut conn)
        .await;
    if let Err(error) = result {
        tracing::warn!(error = %error, delivery_id, "failed to record webhook dead-letter");
    }
}

/// An empty filter means "every notable event"; otherwise the event's slug
/// must be listed explicitly.
fn should_deliver(filter: &[String], slug: &str) -> bool {
    filter.is_empty() || filter.iter().any(|f| f == slug)
}

fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Compact media-item snapshot attached when `enrich` is on. Hand-built rather
/// than serializing the whole entity so internal/DB-only columns don't leak
/// into the public payload.
async fn enrich(ctx: &PluginContext, id: Option<i64>) -> Option<serde_json::Value> {
    let id = id?;
    let item = match repo::get_media_item(&ctx.db_pool, id).await {
        Ok(Some(item)) => item,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(error = %error, id, "failed to load media item for webhook enrichment");
            return None;
        }
    };
    Some(json!({
        "id": item.id,
        "title": item.title,
        "full_title": item.full_title,
        "item_type": item.item_type,
        "state": item.state,
        "year": item.year,
        "imdb_id": item.imdb_id,
        "tmdb_id": item.tmdb_id,
        "tvdb_id": item.tvdb_id,
        "poster_path": item.poster_path,
        "rating": item.rating,
        "is_anime": item.is_anime,
        "network": item.network,
        "genres": item.genres,
        "season_number": item.season_number,
        "episode_number": item.episode_number,
    }))
}

#[cfg(test)]
mod tests;
