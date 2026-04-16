use async_trait::async_trait;

use super::{PluginContext, SettingField};
use crate::events::{EventType, HookResponse, RivenEvent};
use crate::settings::PluginSettings;
use crate::types::ContentServiceResponse;

#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn show_in_settings(&self) -> bool {
        true
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[]
    }

    async fn validate(
        &self,
        _settings: &PluginSettings,
        _http: &crate::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn handle_event(
        &self,
        _event: &RivenEvent,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![]
    }

    /// Fetch content on-demand for a GraphQL query.
    /// `query` is `"movies"`, `"shows"`, or `"all"`; `args` is plugin-specific JSON.
    /// Returns empty by default; content-provider plugins override this.
    async fn query_content(
        &self,
        _query: &str,
        _args: &serde_json::Value,
        _ctx: &PluginContext,
    ) -> anyhow::Result<ContentServiceResponse> {
        Ok(ContentServiceResponse::default())
    }
}
