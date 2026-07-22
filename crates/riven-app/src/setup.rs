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

    for plugin in plugins {
        let name = plugin.name();
        let prefix = name.to_uppercase();
        let mut plugin_settings = PluginSettings::load(&prefix);

        let db_key = format!("plugin.{name}");
        if let Ok(Some(db_val)) = riven_db::repo::get_setting(&db_key).await {
            plugin_settings.merge_db_override(&db_val);
        }

        let enabled = riven_db::repo::get_plugin_enabled_setting(name)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| {
                settings.plugin_enabled_default(name, plugin_settings.has_effective_values())
            });

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
