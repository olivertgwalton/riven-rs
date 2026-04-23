use std::sync::Arc;

use tokio::sync::RwLock;

use super::{Plugin, PluginContext, SettingField};
use crate::events::{EventType, HookResponse, RivenEvent};
use crate::settings::PluginSettings;
use crate::types::ContentServiceResponse;

pub const PLUGIN_ENABLED_PREFIX: &str = "plugin_enabled.";

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
        http: crate::http::HttpClient,
        db_pool: sqlx::PgPool,
        redis: redis::aio::ConnectionManager,
        vfs_mount_path: String,
    ) {
        let plugin: Arc<dyn Plugin> = Arc::from(plugin);
        let valid = enabled
            && match plugin.validate(&settings, &http).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(plugin = plugin.name(), error = %e, "plugin validation error");
                    false
                }
            };

        log_plugin_state(
            plugin.name(),
            enabled,
            valid,
            "registered successfully",
            "validation failed, skipping",
        );

        let context = Arc::new(PluginContext::new(settings, http, db_pool, redis, vfs_mount_path));
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
        let extracted = {
            let mut plugins = self.plugins.write().await;
            let Some(active) = plugins.iter_mut().find(|p| p.plugin.name() == name) else {
                return false;
            };
            let mut new_settings = active.context.settings.clone();
            new_settings.merge_db_override(db_override);
            let plugin = Arc::clone(&active.plugin);
            let http = active.context.http.clone();
            let db_pool = active.context.db_pool.clone();
            let redis = active.context.redis.clone();
            let vfs_mount_path = active.context.vfs_mount_path.clone();
            (plugin, new_settings, http, db_pool, redis, vfs_mount_path)
        };

        let (plugin, new_settings, http, db_pool, redis, vfs_mount_path) = extracted;
        let valid = enabled
            && match plugin.validate(&new_settings, &http).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(plugin = name, error = %e, "plugin revalidation error");
                    false
                }
            };

        let mut plugins = self.plugins.write().await;
        let Some(active) = plugins.iter_mut().find(|p| p.plugin.name() == name) else {
            return false;
        };
        active.context = Arc::new(PluginContext::new(new_settings, http, db_pool, redis, vfs_mount_path));
        active.enabled = enabled;
        active.valid = valid;
        log_plugin_state(name, enabled, valid, "revalidated successfully", "revalidation failed");
        valid
    }

    pub async fn dispatch(
        &self,
        event: &RivenEvent,
    ) -> Vec<(&'static str, anyhow::Result<HookResponse>)> {
        let event_type = event.event_type();
        let targets: Vec<_> = self
            .plugins
            .read()
            .await
            .iter()
            .filter(|a| a.valid && a.plugin.subscribed_events().contains(&event_type))
            .map(|active| {
                (
                    active.plugin.name(),
                    Arc::clone(&active.plugin),
                    Arc::clone(&active.context),
                )
            })
            .collect();

        let futures = targets
            .into_iter()
            .map(|(name, plugin, context)| async move {
                (name, plugin.handle_event(event, &context).await)
            });
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

    pub async fn is_plugin_enabled(&self, name: &str) -> Option<bool> {
        self.plugins
            .read()
            .await
            .iter()
            .find(|p| p.plugin.name() == name)
            .map(|p| p.enabled)
    }

    /// Validate a plugin against its current settings without mutating state.
    pub async fn validate_plugin_current(&self, name: &str) -> bool {
        let target = self
            .plugins
            .read()
            .await
            .iter()
            .find(|p| p.plugin.name() == name)
            .map(|active| (Arc::clone(&active.plugin), Arc::clone(&active.context)));

        let Some((plugin, context)) = target else {
            return false;
        };
        plugin
            .validate(&context.settings, &context.http)
            .await
            .unwrap_or(false)
    }

    /// Call a plugin's `query_content` with the given query type and args.
    pub async fn query_plugin_content(
        &self,
        name: &str,
        query: &str,
        args: &serde_json::Value,
    ) -> anyhow::Result<ContentServiceResponse> {
        let target = self
            .plugins
            .read()
            .await
            .iter()
            .find(|p| p.plugin.name() == name)
            .map(|active| (Arc::clone(&active.plugin), Arc::clone(&active.context)))
            .ok_or_else(|| anyhow::anyhow!("plugin '{name}' not found"))?;
        let (plugin, context) = target;
        plugin.query_content(query, args, &context).await
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

fn log_plugin_state(
    plugin: &str,
    enabled: bool,
    valid: bool,
    valid_message: &'static str,
    invalid_message: &'static str,
) {
    if !enabled {
        tracing::info!(plugin, "plugin disabled");
    } else if valid {
        tracing::info!(plugin, "plugin {valid_message}");
    } else {
        tracing::warn!(plugin, "plugin {invalid_message}");
    }
}
