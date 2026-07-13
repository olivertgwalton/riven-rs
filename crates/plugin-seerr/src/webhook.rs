use std::sync::Arc;

use async_graphql::{Context, Object};
use riven_queue::JobQueue;
use riven_queue::lifecycle::LibraryOrchestrator;
use serde::Deserialize;

/// GraphQL surface for Seerr's inbound webhook, merged into `MutationRoot`
/// (`crates/riven-api/src/schema/mutations/mod.rs`). Replaces the previous
/// hand-written `POST /webhook/seerr` REST route: this is the plugin-owned
/// equivalent, reachable at `/graphql`.
#[derive(Default)]
pub struct SeerrMutations;

#[Object]
impl SeerrMutations {
    /// Accepts Seerr's webhook payload (wrapped in a GraphQL envelope by the
    /// JSON payload template `validate_webhook_settings` configures on Seerr)
    /// and either acknowledges a `TEST_NOTIFICATION` ping or upserts the
    /// requested movie/show directly into the library, mirroring what the
    /// periodic content-service flow would have produced for the same
    /// request — so users don't have to wait for the next poll cycle.
    async fn seerr_handle_webhook(
        &self,
        ctx: &Context<'_>,
        payload: async_graphql::Json<serde_json::Value>,
    ) -> async_graphql::Result<bool> {
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let parsed: SeerrWebhookPayload = match serde_json::from_value(payload.0) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "seerr webhook payload failed to parse, falling back to full refresh");
                riven_queue::application::request_content::enqueue(job_queue).await;
                return Ok(true);
            }
        };

        match parsed {
            SeerrWebhookPayload::Test { .. } => {
                tracing::info!("seerr webhook test notification received");
            }
            SeerrWebhookPayload::Notification(n) => {
                handle_notification(job_queue, n).await;
            }
        }

        Ok(true)
    }
}

async fn handle_notification(job_queue: &Arc<JobQueue>, n: NotificationPayload) {
    let trigger_kinds = [
        "MEDIA_PENDING",
        "MEDIA_APPROVED",
        "MEDIA_AVAILABLE",
        "MEDIA_AUTO_APPROVED",
        "MEDIA_AUTO_REQUESTED",
    ];
    if !trigger_kinds.contains(&n.notification_type.as_str()) {
        tracing::debug!(
            kind = %n.notification_type,
            "ignoring seerr webhook (non-request notification)"
        );
        return;
    }

    let media_type = n.media.media_type.as_deref().unwrap_or("");
    let imdb_id = n.media.imdb_id.as_deref().filter(|s| !s.is_empty());
    let tmdb_id = n.media.tmdb_id.as_deref().filter(|s| !s.is_empty());
    let tvdb_id = n.media.tvdb_id.as_deref().filter(|s| !s.is_empty());
    let external_request_id = n.request.as_ref().map(|r| r.request_id.clone());
    let requested_by = n
        .request
        .as_ref()
        .and_then(|r| r.requested_by_email.clone());

    let orchestrator = LibraryOrchestrator::new(job_queue);
    let result = match media_type {
        "movie" => {
            let title = imdb_id.or(tmdb_id).unwrap_or("Unknown");
            orchestrator
                .upsert_requested_movie(
                    title,
                    imdb_id,
                    tmdb_id,
                    requested_by.as_deref(),
                    external_request_id.as_deref(),
                )
                .await
                .map(|outcome| (outcome, None::<Vec<i32>>))
        }
        "tv" => {
            let title = imdb_id.or(tvdb_id).unwrap_or("Unknown");
            let seasons = parse_requested_seasons(&n.extra);
            orchestrator
                .upsert_requested_show(
                    title,
                    imdb_id,
                    tvdb_id,
                    requested_by.as_deref(),
                    external_request_id.as_deref(),
                    seasons.as_deref(),
                )
                .await
                .map(|outcome| (outcome, seasons))
        }
        other => {
            tracing::warn!(media_type = %other, "seerr webhook: unknown media type");
            return;
        }
    };

    match result {
        Ok((outcome, requested_seasons)) => {
            if let Some(event) = outcome.lifecycle_event(requested_seasons.as_deref()) {
                job_queue.notify(event).await;
            }
            orchestrator
                .enqueue_after_request_action(
                    &outcome.item,
                    outcome.action,
                    requested_seasons.as_deref(),
                )
                .await;
            tracing::info!(
                item_id = outcome.item.id,
                kind = %n.notification_type,
                "seerr webhook upserted requested item",
            );
        }
        Err(error) => {
            tracing::warn!(error = %error, "seerr webhook: failed to upsert requested item");
        }
    }
}

fn parse_requested_seasons(extra: &[ExtraField]) -> Option<Vec<i32>> {
    let value = extra
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case("requested seasons"))
        .map(|e| e.value.as_str())?;

    let seasons: Vec<i32> = value
        .split(',')
        .filter_map(|part| part.trim().parse::<i32>().ok())
        .collect();
    if seasons.is_empty() {
        None
    } else {
        Some(seasons)
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SeerrWebhookPayload {
    Test {
        #[serde(rename = "notification_type")]
        _notification_type: TestKind,
    },
    Notification(NotificationPayload),
}

#[derive(Debug, Deserialize)]
enum TestKind {
    #[serde(rename = "TEST_NOTIFICATION")]
    Test,
}

#[derive(Debug, Deserialize)]
struct NotificationPayload {
    notification_type: String,
    #[serde(default)]
    media: MediaPayload,
    #[serde(default)]
    request: Option<RequestPayload>,
    #[serde(default)]
    extra: Vec<ExtraField>,
}

#[derive(Debug, Default, Deserialize)]
struct MediaPayload {
    #[serde(default)]
    media_type: Option<String>,
    #[serde(rename = "imdbId", default)]
    imdb_id: Option<String>,
    #[serde(rename = "tmdbId", default)]
    tmdb_id: Option<String>,
    #[serde(rename = "tvdbId", default)]
    tvdb_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RequestPayload {
    request_id: String,
    #[serde(rename = "requestedBy_email", default)]
    requested_by_email: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExtraField {
    name: String,
    value: String,
}
