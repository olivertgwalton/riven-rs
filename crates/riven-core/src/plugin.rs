use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use crate::events::{EventType, HookResponse, RivenEvent};
use crate::settings::PluginSettings;

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

impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            query_fields: Vec::new(),
        }
    }
}

/// Registration entry for the inventory-based plugin system.
pub struct PluginRegistration {
    pub create: fn() -> Box<dyn Plugin>,
}

inventory::collect!(PluginRegistration);

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
    plugins: Vec<ActivePlugin>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
        }
    }

    pub async fn register(
        &mut self,
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

        self.plugins.push(ActivePlugin {
            plugin,
            context,
            valid,
        });
    }

    /// Dispatch an event to all valid plugins that subscribe to it.
    pub async fn dispatch(&self, event: &RivenEvent) -> Vec<(&str, anyhow::Result<HookResponse>)> {
        let event_type = event.event_type();
        let mut results = Vec::new();

        for active in &self.plugins {
            if !active.valid {
                continue;
            }
            if !active.plugin.subscribed_events().contains(&event_type) {
                continue;
            }

            let name = active.plugin.name();
            let result = active.plugin.handle_event(event, &active.context).await;
            results.push((name, result));
        }

        results
    }

    pub fn valid_plugins(&self) -> impl Iterator<Item = &ActivePlugin> {
        self.plugins.iter().filter(|p| p.valid)
    }

    pub fn all_plugins(&self) -> &[ActivePlugin] {
        &self.plugins
    }
}

/// Macro for registering a plugin with the inventory system.
#[macro_export]
macro_rules! register_plugin {
    ($plugin_type:ty) => {
        inventory::submit! {
            $crate::plugin::PluginRegistration {
                create: || Box::new(<$plugin_type>::new()),
            }
        }
    };
}
