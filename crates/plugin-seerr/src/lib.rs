use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{ContentCollection, Plugin, PluginContext, validate_api_key};
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use serde::Deserialize;
use std::time::Duration;

const DEFAULT_URL: &str = "http://localhost:5055";

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("seerr").with_rate_limit(20, Duration::from_secs(1));
const DEFAULT_FILTER: &str = "approved";
const PAGE_SIZE: u32 = 20;

/// Request Approved (bit 4) + Request Automatically Approved (bit 128). Seerr
/// notification-type bitmask required for the `seerrHandleWebhook` GraphQL
/// mutation (`webhook.rs`) to receive the events it acts on.
const REQUIRED_WEBHOOK_TYPES: u32 = 4 | 128;

#[derive(Default)]
pub struct SeerrPlugin;

#[async_trait]
impl Plugin for SeerrPlugin {
    fn name(&self) -> &'static str {
        "seerr"
    }

    fn category(&self) -> &'static str {
        "services"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::ContentServiceRequested]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        let url = settings.get_or("url", DEFAULT_URL);
        let base_url = url.trim_end_matches('/');

        if !validate_api_key(
            http,
            settings,
            "apikey",
            &format!("{base_url}/api/v1/auth/me"),
            "x-api-key",
        )
        .await?
        {
            return Ok(false);
        }

        let api_key = settings.get("apikey").unwrap_or_default();
        validate_metadata_providers(http, settings, base_url, api_key).await?;
        validate_webhook_settings(http, settings, base_url, api_key).await?;

        Ok(true)
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::{FieldType, SettingField};
        vec![
            SettingField::new("apikey", "API Key", FieldType::Password).required(),
            SettingField::new("url", "Server URL", FieldType::Url)
                .with_default("http://localhost:5055")
                .with_placeholder("http://localhost:5055"),
            SettingField::new("filter", "Request Filter", FieldType::Text)
                .with_default("approved")
                .with_placeholder("approved")
                .with_description("Which request status to import (approved, all, pending)."),
            SettingField::new(
                "autofixmetadataproviders",
                "Auto-fix Metadata Providers",
                FieldType::Boolean,
            )
            .with_default("false")
            .with_description(
                "Automatically fix metadata provider settings in Seerr if they are incorrect (both TV and anime must use TVDB).",
            ),
            SettingField::new(
                "autofixwebhookbody",
                "Auto-fix Webhook Settings",
                FieldType::Boolean,
            )
            .with_default("false")
            .with_description(
                "Automatically enable the required notification types on Seerr's configured webhook, if it's enabled but missing them.",
            ),
        ]
    }

    async fn on_content_service_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let url = ctx.settings.get_or("url", DEFAULT_URL);
        let base_url = url.trim_end_matches('/');
        fetch_content(ctx, api_key, base_url).await
    }
}

async fn validate_metadata_providers(
    http: &riven_core::http::HttpClient,
    settings: &PluginSettings,
    base_url: &str,
    api_key: &str,
) -> anyhow::Result<()> {
    #[derive(Deserialize, serde::Serialize)]
    struct MetadataSettingsResponse {
        tv: String,
        anime: String,
    }

    let req_url = format!("{base_url}/api/v1/settings/metadatas");
    let current: MetadataSettingsResponse = http
        .get_json(PROFILE, req_url.clone(), |client| {
            client.get(&req_url).header("x-api-key", api_key)
        })
        .await?;

    if current.tv == "tvdb" && current.anime == "tvdb" {
        return Ok(());
    }

    if settings.get_bool("autofixmetadataproviders") {
        let fixed = MetadataSettingsResponse {
            tv: "tvdb".to_string(),
            anime: "tvdb".to_string(),
        };
        http.send(PROFILE, |client| {
            client
                .put(&req_url)
                .header("x-api-key", api_key)
                .json(&fixed)
        })
        .await?
        .error_for_status()?;
        tracing::info!("automatically fixed Seerr metadata provider settings to TVDB");
        return Ok(());
    }

    anyhow::bail!(
        "Seerr's metadata providers must both be set to TVDB (currently tv={}, anime={}). \
         Fix this at {base_url}/settings/metadata or enable \"autofixmetadataproviders\" \
         in the plugin settings.",
        current.tv,
        current.anime,
    )
}

/// Marker substring used to detect whether Seerr's configured webhook payload
/// template already targets `seerrHandleWebhook` — a full round-trip parse of
/// the (user-editable, freeform) template isn't worth the fragility.
const WEBHOOK_MUTATION_MARKER: &str = "seerrHandleWebhook";

/// JSON payload template Seerr must POST for `SeerrMutations::seerr_handle_webhook`
/// (`crates/plugin-seerr/src/webhook.rs`) to receive it: a GraphQL request
/// wrapping the same fields Seerr's own default webhook template already
/// produces. Placeholders are Overseerr/Jellyseerr's own mustache-style
/// tokens, unchanged from their default template.
const WEBHOOK_PAYLOAD_TEMPLATE: &str = r#"{
  "query": "mutation ($payload: JSON!) { seerrHandleWebhook(payload: $payload) }",
  "variables": {
    "payload": {
      "notification_type": "{{notification_type}}",
      "media": {
        "media_type": "{{media_type}}",
        "imdbId": "{{media_imdbid}}",
        "tmdbId": "{{media_tmdbid}}",
        "tvdbId": "{{media_tvdbid}}"
      },
      "request": {
        "request_id": "{{request_id}}",
        "requestedBy_email": "{{requestedBy_email}}"
      },
      "extra": [
        { "name": "Requested Seasons", "value": "{{extra.[0].value}}" }
      ]
    }
  }
}"#;

async fn validate_webhook_settings(
    http: &riven_core::http::HttpClient,
    settings: &PluginSettings,
    base_url: &str,
    api_key: &str,
) -> anyhow::Result<()> {
    let req_url = format!("{base_url}/api/v1/settings/notifications/webhook");
    let current: serde_json::Value = http
        .get_json(PROFILE, req_url.clone(), |client| {
            client.get(&req_url).header("x-api-key", api_key)
        })
        .await?;

    let enabled = current.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
    if !enabled {
        // riven never enables the webhook or sets its URL on Seerr's behalf;
        // the user opts in manually by pointing it at riven's /graphql
        // endpoint (with ?api_key=... if one is configured).
        return Ok(());
    }

    let current_types = current
        .get("types")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let has_required_types = (current_types & REQUIRED_WEBHOOK_TYPES) == REQUIRED_WEBHOOK_TYPES;
    let has_extra_types = (current_types & !REQUIRED_WEBHOOK_TYPES) != 0;

    if has_extra_types {
        tracing::warn!(
            "Seerr webhook has additional notification types enabled beyond the required ones; \
             this is fine, but may result in unnecessary webhook calls"
        );
    }

    let has_correct_payload = current
        .get("options")
        .and_then(|o| o.get("jsonPayload"))
        .and_then(|v| v.as_str())
        .is_some_and(|s| s.contains(WEBHOOK_MUTATION_MARKER));

    if has_required_types && has_correct_payload {
        return Ok(());
    }

    if settings.get_bool("autofixwebhookbody") {
        // Overseerr's endpoint replaces the whole settings object, so mutate
        // just the fields we care about on the object we just fetched rather
        // than sending a partial body that would clobber other settings.
        let mut updated = current;
        updated["types"] = serde_json::json!(current_types | REQUIRED_WEBHOOK_TYPES);
        if !has_correct_payload {
            updated["options"]["jsonPayload"] = serde_json::json!(WEBHOOK_PAYLOAD_TEMPLATE);
        }
        http.send(PROFILE, |client| {
            client
                .post(&req_url)
                .header("x-api-key", api_key)
                .json(&updated)
        })
        .await?
        .error_for_status()?;
        tracing::info!(
            "automatically fixed Seerr's webhook notification types and payload template"
        );
        return Ok(());
    }

    anyhow::bail!(
        "Seerr's webhook is enabled but missing the required notification types \
         (\"Request Approved\" and \"Request Automatically Approved\") and/or the JSON payload \
         template needed to call riven's GraphQL webhook mutation. Fix this at \
         {base_url}/settings/notifications/webhook or enable \"autofixwebhookbody\" in the \
         plugin settings.",
    )
}

async fn fetch_seerr_content(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    base_url: &str,
    filter: &str,
) -> anyhow::Result<riven_core::types::ContentServiceResponse> {
    let mut content = ContentCollection::default();
    let mut skip = 0u32;
    loop {
        let req_url = format!(
            "{base_url}/api/v1/request?take={PAGE_SIZE}&skip={skip}&filter={filter}&sort=added"
        );
        tracing::debug!(target_url = %req_url, skip, filter, "fetching seerr requests");
        let resp: SeerrRequestResponse = http
            .get_json(PROFILE, req_url.clone(), |client| {
                client.get(&req_url).header("x-api-key", api_key)
            })
            .await?;

        for request in &resp.results {
            let requested_by = request.requested_by.as_ref().and_then(|u| u.email.clone());
            match request.media_type.as_deref() {
                Some("movie") => {
                    if let Some(ref media) = request.media
                        && let Some(tmdb_id) = media.tmdb_id
                    {
                        content.insert_movie(ExternalIds {
                            tmdb_id: Some(tmdb_id.to_string()),
                            external_request_id: Some(request.id.to_string()),
                            requested_by: requested_by.clone(),
                            ..Default::default()
                        });
                    }
                }
                Some("tv") => {
                    if let Some(ref media) = request.media
                        && let Some(tvdb_id) = media.tvdb_id
                    {
                        let seasons: Vec<i32> = request
                            .seasons
                            .iter()
                            .flatten()
                            .filter_map(|s| s.season_number)
                            .collect();
                        // Key on the stable Seerr *media* id, not the per-request
                        // id: Seerr creates a separate request for each partial
                        // season addition to the same show, and using the media
                        // id here lets ContentCollection::insert_show recognize
                        // and merge them instead of treating each as distinct.
                        let external_request_id = media
                            .id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| request.id.to_string());
                        content.insert_show(ExternalIds {
                            tvdb_id: Some(tvdb_id.to_string()),
                            external_request_id: Some(external_request_id),
                            requested_by: requested_by.clone(),
                            requested_seasons: if seasons.is_empty() {
                                None
                            } else {
                                Some(seasons)
                            },
                            ..Default::default()
                        });
                    }
                }
                _ => {}
            }
        }

        if resp.results.len() < PAGE_SIZE as usize {
            break;
        }
        skip += PAGE_SIZE;
    }
    Ok(content.into_response())
}

async fn fetch_content(
    ctx: &PluginContext,
    api_key: &str,
    base_url: &str,
) -> anyhow::Result<HookResponse> {
    let filter = ctx.settings.get_or("filter", DEFAULT_FILTER);
    let content = fetch_seerr_content(&ctx.http, api_key, base_url, &filter).await?;
    Ok(HookResponse::ContentService(Box::new(content)))
}

#[derive(Deserialize)]
struct SeerrRequestResponse {
    results: Vec<SeerrRequest>,
}

#[derive(Deserialize)]
struct SeerrRequest {
    id: i64,
    #[serde(rename = "type")]
    media_type: Option<String>,
    media: Option<SeerrMedia>,
    #[serde(rename = "requestedBy")]
    requested_by: Option<SeerrUser>,
    #[serde(default)]
    seasons: Option<Vec<SeerrSeason>>,
}

#[derive(Deserialize)]
struct SeerrMedia {
    id: Option<i64>,
    #[serde(rename = "tmdbId")]
    tmdb_id: Option<i64>,
    #[serde(rename = "tvdbId")]
    tvdb_id: Option<i64>,
}

#[derive(Deserialize)]
struct SeerrUser {
    email: Option<String>,
}

#[derive(Deserialize)]
struct SeerrSeason {
    #[serde(rename = "seasonNumber")]
    season_number: Option<i32>,
}

mod webhook;
pub use webhook::SeerrMutations;

#[cfg(test)]
mod tests;
