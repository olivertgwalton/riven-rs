mod client;
mod models;

use async_trait::async_trait;
use std::time::Duration;

use riven_core::events::{EventType, HookResponse, ScrapeRequest};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;

use crate::client::{scrape, validate_search};

const DEFAULT_URL: &str = "https://aiostreamsfortheweebs.midnightignite.me/";

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("aiostreams").with_rate_limit(120, Duration::from_secs(60));

#[derive(Default)]
pub struct AioStreamsPlugin;

register_plugin!(AioStreamsPlugin);

#[async_trait]
impl Plugin for AioStreamsPlugin {
    fn name(&self) -> &'static str {
        "aiostreams"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemScrapeRequested]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        let Some(uuid) = settings.get("uuid") else {
            return Ok(false);
        };
        let Some(password) = settings.get("password") else {
            return Ok(false);
        };

        let base_url = settings.get_or("url", DEFAULT_URL);
        let base_url = base_url.trim_end_matches('/');

        validate_search(http, base_url, uuid, password).await
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("url", "URL", "url")
                .required()
                .with_default(DEFAULT_URL)
                .with_placeholder(DEFAULT_URL)
                .with_description("Base URL of your AIOStreams instance."),
            SettingField::new("uuid", "User UUID", "text")
                .required()
                .with_placeholder("00000000-0000-0000-0000-000000000000")
                .with_description("AIOStreams user UUID used for basic auth."),
            SettingField::new("password", "Password", "password")
                .required()
                .with_description("AIOStreams user password used for basic auth."),
        ]
    }

    async fn on_scrape_requested(
        &self,
        request: &ScrapeRequest<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let uuid = ctx.require_setting("uuid")?;
        let password = ctx.require_setting("password")?;
        let base_url = ctx.settings.get_or("url", DEFAULT_URL);
        let base_url = base_url.trim_end_matches('/');

        scrape(&ctx.http, base_url, uuid, password, request)
            .await
            .map(HookResponse::Scrape)
    }
}
