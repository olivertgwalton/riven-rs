use std::sync::Arc;

use riven_core::plugin::PluginRegistry;
use riven_core::settings::{PluginSettings, RivenSettings};

use riven_plugins::all_plugins;

pub async fn register_plugins(
    http: riven_core::http::HttpClient,
    redis_conn: redis::aio::ConnectionManager,
    vfs_mount_path: String,
    settings: &RivenSettings,
) -> Arc<PluginRegistry> {
    let registry = PluginRegistry::new();
    let plugins = all_plugins();

    tracing::info!(count = plugins.len(), "discovered plugins");

    // Every plugin needs two settings rows. Fetch them all in one query rather
    // than blocking startup on two sequential round trips per plugin.
    let setting_keys: Vec<String> = plugins
        .iter()
        .flat_map(|plugin| {
            let name = plugin.name();
            [format!("plugin.{name}"), format!("plugin_enabled.{name}")]
        })
        .collect();
    let db_settings = riven_db::repo::get_settings(&setting_keys)
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(%error, "failed to load plugin settings; using file/env defaults");
            Default::default()
        });

    for plugin in plugins {
        let name = plugin.name();
        let prefix = name.to_uppercase();
        let mut plugin_settings = PluginSettings::load(&prefix);

        if let Some(db_val) = db_settings.get(&format!("plugin.{name}")) {
            plugin_settings.merge_db_override(db_val);
        }

        let enabled = match db_settings.get(&format!("plugin_enabled.{name}")) {
            Some(serde_json::Value::Bool(enabled)) => *enabled,
            _ => settings.plugin_enabled_default(name, plugin_settings.has_effective_values()),
        };

        registry
            .register(
                plugin,
                enabled,
                plugin_settings,
                http.clone(),
                redis_conn.clone(),
                vfs_mount_path.clone(),
            )
            .await;
    }

    Arc::new(registry)
}
