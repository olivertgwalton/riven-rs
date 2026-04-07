use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::events::{EventType, HookResponse, RivenEvent};
use crate::settings::PluginSettings;
use crate::types::{ContentServiceResponse, ExternalIds};

pub const PLUGIN_ENABLED_PREFIX: &str = "plugin_enabled.";

/// Describes one configurable setting field for a plugin.
/// Used to render the settings UI dynamically on the frontend.
#[derive(Debug, Clone, Serialize)]
pub struct SettingField {
    pub key: &'static str,
    pub label: &'static str,
    /// Input type hint: "text" | "password" | "url" | "number" | "boolean" | "textarea"
    #[serde(rename = "type")]
    pub field_type: &'static str,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<&'static str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<SettingField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_fields: Option<Vec<SettingField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_placeholder: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add_label: Option<&'static str>,
}

impl SettingField {
    pub const fn new(key: &'static str, label: &'static str, field_type: &'static str) -> Self {
        Self {
            key,
            label,
            field_type,
            required: false,
            default_value: None,
            placeholder: None,
            description: None,
            options: None,
            fields: None,
            item_fields: None,
            key_placeholder: None,
            add_label: None,
        }
    }
    pub const fn required(mut self) -> Self {
        self.required = true;
        self
    }
    pub const fn with_default(mut self, v: &'static str) -> Self {
        self.default_value = Some(v);
        self
    }
    pub const fn with_placeholder(mut self, v: &'static str) -> Self {
        self.placeholder = Some(v);
        self
    }
    pub const fn with_description(mut self, v: &'static str) -> Self {
        self.description = Some(v);
        self
    }
    pub fn with_options(mut self, values: &[&'static str]) -> Self {
        self.options = Some(values.to_vec());
        self
    }
    pub fn with_fields(mut self, fields: Vec<SettingField>) -> Self {
        self.fields = Some(fields);
        self
    }
    pub fn with_item_fields(mut self, fields: Vec<SettingField>) -> Self {
        self.item_fields = Some(fields);
        self
    }
    pub const fn with_key_placeholder(mut self, v: &'static str) -> Self {
        self.key_placeholder = Some(v);
        self
    }
    pub const fn with_add_label(mut self, v: &'static str) -> Self {
        self.add_label = Some(v);
        self
    }
}

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
    async fn validate(&self, _settings: &PluginSettings) -> anyhow::Result<bool> {
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
}

#[derive(Default)]
pub struct ContentCollection {
    movies: HashMap<String, ExternalIds>,
    shows: HashMap<String, ExternalIds>,
}

impl ContentCollection {
    pub fn insert_movie(&mut self, ids: ExternalIds) {
        self.movies.entry(ids.movie_key()).or_insert(ids);
    }

    pub fn insert_show(&mut self, ids: ExternalIds) {
        self.shows.entry(ids.show_key()).or_insert(ids);
    }

    pub fn movie_count(&self) -> usize {
        self.movies.len()
    }

    pub fn show_count(&self) -> usize {
        self.shows.len()
    }

    pub fn into_response(self) -> ContentServiceResponse {
        ContentServiceResponse {
            movies: self.movies.into_values().collect(),
            shows: self.shows.into_values().collect(),
        }
    }

    pub fn into_hook_response(self) -> HookResponse {
        HookResponse::ContentService(Box::new(self.into_response()))
    }
}

pub struct PluginContext {
    pub settings: PluginSettings,
    pub http_client: reqwest::Client,
    pub db_pool: sqlx::PgPool,
    pub redis: redis::aio::ConnectionManager,
}

impl PluginContext {
    pub fn require_setting(&self, key: &str) -> anyhow::Result<&str> {
        self.settings
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("{key} not configured"))
    }
}

pub async fn validate_api_key(
    settings: &PluginSettings,
    key_name: &str,
    url: &str,
    header: &str,
) -> anyhow::Result<bool> {
    let api_key = match settings.get(key_name) {
        Some(k) => k,
        None => return Ok(false),
    };
    let resp = reqwest::Client::new()
        .get(url)
        .header(header, api_key)
        .send()
        .await;
    Ok(resp.is_ok())
}

/// Registration entry for the inventory-based plugin system.
pub struct PluginRegistration {
    pub create: fn() -> Box<dyn Plugin>,
}

inventory::collect!(PluginRegistration);

pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub valid: bool,
    pub schema: Vec<SettingField>,
}

pub fn collect_plugins() -> Vec<Box<dyn Plugin>> {
    inventory::iter::<PluginRegistration>
        .into_iter()
        .map(|reg| (reg.create)())
        .collect()
}

pub struct ActivePlugin {
    pub plugin: Arc<dyn Plugin>,
    pub context: Arc<PluginContext>,
    pub enabled: bool,
    pub valid: bool,
}

pub struct PluginRegistry {
    plugins: RwLock<Vec<ActivePlugin>>,
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self {
            plugins: RwLock::new(Vec::new()),
        }
    }
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(
        &self,
        plugin: Box<dyn Plugin>,
        enabled: bool,
        settings: PluginSettings,
        http_client: reqwest::Client,
        db_pool: sqlx::PgPool,
        redis: redis::aio::ConnectionManager,
    ) {
        let plugin: Arc<dyn Plugin> = Arc::from(plugin);
        let valid = enabled && plugin.validate(&settings).await.unwrap_or(false);

        if !enabled {
            tracing::info!(plugin = plugin.name(), "plugin disabled");
        } else if !valid {
            tracing::warn!(plugin = plugin.name(), "plugin validation failed, skipping");
        } else {
            tracing::info!(plugin = plugin.name(), "plugin registered successfully");
        }

        let context = Arc::new(PluginContext {
            settings,
            http_client,
            db_pool,
            redis,
        });

        self.plugins.write().await.push(ActivePlugin {
            plugin,
            context,
            enabled,
            valid,
        });
    }

    pub async fn revalidate_plugin(
        &self,
        name: &str,
        enabled: bool,
        db_override: &serde_json::Value,
    ) -> bool {
        let mut plugins = self.plugins.write().await;
        if let Some(active) = plugins.iter_mut().find(|p| p.plugin.name() == name) {
            let mut new_settings = active.context.settings.clone();
            new_settings.merge_db_override(db_override);
            let valid = enabled && active.plugin.validate(&new_settings).await.unwrap_or(false);
            active.context = Arc::new(PluginContext {
                settings: new_settings,
                http_client: active.context.http_client.clone(),
                db_pool: active.context.db_pool.clone(),
                redis: active.context.redis.clone(),
            });
            active.enabled = enabled;
            active.valid = valid;
            if !enabled {
                tracing::info!(plugin = name, "plugin disabled");
            } else if valid {
                tracing::info!(plugin = name, "plugin revalidated successfully");
            } else {
                tracing::warn!(plugin = name, "plugin revalidation failed");
            }
            valid
        } else {
            false
        }
    }

    pub async fn dispatch(
        &self,
        event: &RivenEvent,
    ) -> Vec<(String, anyhow::Result<HookResponse>)> {
        let event_type = event.event_type();
        let targets: Vec<_> = self
            .plugins
            .read()
            .await
            .iter()
            .filter(|a| a.valid && a.plugin.subscribed_events().contains(&event_type))
            .map(|active| {
                (
                    active.plugin.name().to_string(),
                    Arc::clone(&active.plugin),
                    Arc::clone(&active.context),
                )
            })
            .collect();

        let futures: Vec<_> = targets
            .into_iter()
            .map(|(name, plugin, context)| async move {
                (name, plugin.handle_event(event, &context).await)
            })
            .collect();

        futures::future::join_all(futures).await
    }

    pub async fn subscriber_names(&self, event_type: EventType) -> Vec<String> {
        self.plugins
            .read()
            .await
            .iter()
            .filter(|active| {
                active.valid && active.plugin.subscribed_events().contains(&event_type)
            })
            .map(|active| active.plugin.name().to_string())
            .collect()
    }

    pub async fn dispatch_to_plugin(
        &self,
        plugin_name: &str,
        event: &RivenEvent,
    ) -> Option<anyhow::Result<HookResponse>> {
        let target = self
            .plugins
            .read()
            .await
            .iter()
            .find(|active| active.valid && active.plugin.name() == plugin_name)
            .map(|active| (Arc::clone(&active.plugin), Arc::clone(&active.context)));

        let (plugin, context) = target?;
        Some(plugin.handle_event(event, &context).await)
    }

    pub async fn all_plugins_info(&self) -> Vec<PluginInfo> {
        self.plugins
            .read()
            .await
            .iter()
            .filter(|p| p.plugin.show_in_settings())
            .map(|p| PluginInfo {
                name: p.plugin.name().to_string(),
                version: p.plugin.version().to_string(),
                enabled: p.enabled,
                valid: p.valid,
                schema: plugin_settings_schema(&*p.plugin),
            })
            .collect()
    }

    pub async fn get_plugin_settings_json(&self, name: &str) -> Option<serde_json::Value> {
        self.plugins
            .read()
            .await
            .iter()
            .find(|p| p.plugin.name() == name)
            .map(|p| p.context.settings.to_json())
    }

    pub async fn valid_plugin_names(&self) -> Vec<String> {
        self.plugins
            .read()
            .await
            .iter()
            .filter(|p| p.valid)
            .map(|p| p.plugin.name().to_string())
            .collect()
    }

    pub async fn plugin_count(&self) -> usize {
        self.plugins.read().await.len()
    }
}

fn plugin_settings_schema(plugin: &dyn Plugin) -> Vec<SettingField> {
    let mut schema = Vec::with_capacity(plugin.settings_schema().len() + 1);
    schema.push(
        SettingField::new("enabled", "Enabled", "boolean")
            .with_default("false")
            .with_description(
                "Enable this plugin without mixing activation state into its config.",
            ),
    );
    schema.extend(plugin.settings_schema());
    schema
}

/// Macro for registering a plugin with the inventory system.
#[macro_export]
macro_rules! register_plugin {
    ($plugin_type:ty) => {
        inventory::submit! {
            $crate::plugin::PluginRegistration {
                create: || Box::new(<$plugin_type>::default()),
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::{ContentCollection, SettingField};
    use crate::events::HookResponse;
    use crate::types::ExternalIds;

    #[test]
    fn setting_field_builder_populates_optional_metadata() {
        let field = SettingField::new("quality", "Quality", "select")
            .required()
            .with_default("1080p")
            .with_placeholder("Choose quality")
            .with_description("Preferred quality")
            .with_options(&["720p", "1080p"])
            .with_fields(vec![SettingField::new("nested", "Nested", "text")])
            .with_item_fields(vec![SettingField::new("item", "Item", "text")])
            .with_key_placeholder("provider")
            .with_add_label("Add provider");

        assert!(field.required);
        assert_eq!(field.default_value, Some("1080p"));
        assert_eq!(field.placeholder, Some("Choose quality"));
        assert_eq!(field.description, Some("Preferred quality"));
        assert_eq!(field.options, Some(vec!["720p", "1080p"]));
        assert_eq!(field.fields.as_ref().map(Vec::len), Some(1));
        assert_eq!(field.item_fields.as_ref().map(Vec::len), Some(1));
        assert_eq!(field.key_placeholder, Some("provider"));
        assert_eq!(field.add_label, Some("Add provider"));
    }

    #[test]
    fn content_collection_deduplicates_movies_and_shows_by_preferred_keys() {
        let mut collection = ContentCollection::default();

        collection.insert_movie(ExternalIds {
            imdb_id: Some("tt001".to_string()),
            tmdb_id: Some("10".to_string()),
            ..ExternalIds::default()
        });
        collection.insert_movie(ExternalIds {
            imdb_id: Some("tt001".to_string()),
            tmdb_id: Some("99".to_string()),
            ..ExternalIds::default()
        });
        collection.insert_show(ExternalIds {
            imdb_id: None,
            tvdb_id: Some("tv-1".to_string()),
            ..ExternalIds::default()
        });
        collection.insert_show(ExternalIds {
            imdb_id: None,
            tvdb_id: Some("tv-1".to_string()),
            ..ExternalIds::default()
        });

        assert_eq!(collection.movie_count(), 1);
        assert_eq!(collection.show_count(), 1);

        let response = collection.into_response();
        assert_eq!(response.movies.len(), 1);
        assert_eq!(response.shows.len(), 1);
        assert_eq!(response.movies[0].imdb_id.as_deref(), Some("tt001"));
        assert_eq!(response.shows[0].tvdb_id.as_deref(), Some("tv-1"));
    }

    #[test]
    fn content_collection_can_be_converted_to_hook_response() {
        let mut collection = ContentCollection::default();
        collection.insert_movie(ExternalIds {
            imdb_id: Some("tt123".to_string()),
            ..ExternalIds::default()
        });

        let response = collection.into_hook_response();

        match response {
            HookResponse::ContentService(payload) => {
                assert_eq!(payload.movies.len(), 1);
                assert_eq!(payload.movies[0].imdb_id.as_deref(), Some("tt123"));
                assert!(payload.shows.is_empty());
            }
            other => panic!("expected content-service response, got {other:?}"),
        }
    }
}
