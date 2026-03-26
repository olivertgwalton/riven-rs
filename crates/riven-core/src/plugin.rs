use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::events::{EventType, HookResponse, RivenEvent};
use crate::settings::PluginSettings;

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
}

/// The core plugin trait. All plugins implement this.
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Unique name for this plugin.
    fn name(&self) -> &'static str;

    /// Semantic version string.
    fn version(&self) -> &'static str;

    /// Which events this plugin handles.
    fn subscribed_events(&self) -> &[EventType];

    /// Validate that this plugin is properly configured and can operate.
    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool>;

    /// Handle an incoming event. Returns a typed response.
    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse>;

    /// Describe the configurable settings this plugin accepts.
    /// Returned to the frontend so it can render the settings form dynamically.
    fn settings_schema(&self) -> Vec<SettingField> {
        vec![]
    }

    /// Return the GraphQL schema fragment for this plugin's queries.
    /// Plugins register their resolvers via the schema builder.
    fn register_graphql(&self, _schema: &mut SchemaBuilder) {}
}

/// Runtime context provided to plugins when handling events.
pub struct PluginContext {
    pub settings: PluginSettings,
    pub http_client: reqwest::Client,
    pub db_pool: sqlx::PgPool,
    pub redis: redis::aio::ConnectionManager,
}

impl PluginContext {
    /// Get a required setting or return an error.
    pub fn require_setting(&self, key: &str) -> anyhow::Result<&str> {
        self.settings
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("{key} not configured"))
    }
}

/// Validate that an API key is present and the given URL responds successfully.
/// Returns `Ok(false)` if the key is missing (plugin not configured).
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

/// A lightweight schema builder that plugins can add queries/mutations to.
/// Actual GraphQL schema is assembled by riven-api.
pub struct SchemaBuilder {
    pub query_fields: Vec<Box<dyn Any + Send + Sync>>,
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self { query_fields: Vec::new() }
    }
}

/// Registration entry for the inventory-based plugin system.
pub struct PluginRegistration {
    pub create: fn() -> Box<dyn Plugin>,
}

inventory::collect!(PluginRegistration);

/// Lightweight plugin metadata for the settings API.
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub valid: bool,
    pub schema: Vec<SettingField>,
}

/// Collect all registered plugins.
pub fn collect_plugins() -> Vec<Box<dyn Plugin>> {
    inventory::iter::<PluginRegistration>
        .into_iter()
        .map(|reg| (reg.create)())
        .collect()
}

/// A validated, active plugin with its context.
pub struct ActivePlugin {
    pub plugin: Box<dyn Plugin>,
    pub context: Arc<PluginContext>,
    pub valid: bool,
}

/// The plugin registry holds all plugins and dispatches events.
pub struct PluginRegistry {
    plugins: RwLock<Vec<ActivePlugin>>,
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self { plugins: RwLock::new(Vec::new()) }
    }
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(
        &self,
        plugin: Box<dyn Plugin>,
        settings: PluginSettings,
        http_client: reqwest::Client,
        db_pool: sqlx::PgPool,
        redis: redis::aio::ConnectionManager,
    ) {
        let valid = plugin.validate(&settings).await.unwrap_or(false);

        if !valid {
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
            valid,
        });
    }

    /// Re-apply DB settings override for a plugin and re-run its validate().
    /// Returns the new valid status. No-op if the plugin name is not found.
    pub async fn revalidate_plugin(&self, name: &str, db_override: &serde_json::Value) -> bool {
        let mut plugins = self.plugins.write().await;
        if let Some(active) = plugins.iter_mut().find(|p| p.plugin.name() == name) {
            let mut new_settings = active.context.settings.clone();
            new_settings.merge_db_override(db_override);
            let valid = active.plugin.validate(&new_settings).await.unwrap_or(false);
            active.context = Arc::new(PluginContext {
                settings: new_settings,
                http_client: active.context.http_client.clone(),
                db_pool: active.context.db_pool.clone(),
                redis: active.context.redis.clone(),
            });
            active.valid = valid;
            if valid {
                tracing::info!(plugin = name, "plugin revalidated successfully");
            } else {
                tracing::warn!(plugin = name, "plugin revalidation failed");
            }
            valid
        } else {
            false
        }
    }

    /// Dispatch an event to all valid plugins that subscribe to it, running them concurrently.
    pub async fn dispatch(&self, event: &RivenEvent) -> Vec<(String, anyhow::Result<HookResponse>)> {
        let event_type = event.event_type();
        let plugins = self.plugins.read().await;

        let futures: Vec<_> = plugins
            .iter()
            .filter(|a| a.valid && a.plugin.subscribed_events().contains(&event_type))
            .map(|active| {
                let name = active.plugin.name().to_string();
                async move { (name, active.plugin.handle_event(event, &active.context).await) }
            })
            .collect();

        futures::future::join_all(futures).await
    }

    pub async fn all_plugins_info(&self) -> Vec<PluginInfo> {
        self.plugins
            .read()
            .await
            .iter()
            .map(|p| PluginInfo {
                name: p.plugin.name().to_string(),
                version: p.plugin.version().to_string(),
                valid: p.valid,
                schema: p.plugin.settings_schema(),
            })
            .collect()
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
