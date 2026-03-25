use std::sync::Arc;

use riven_core::plugin::{collect_plugins, PluginRegistry};
use riven_core::settings::{PluginSettings, RivenSettings};

pub async fn register_plugins(
    settings: &RivenSettings,
    http_client: reqwest::Client,
    db_pool: sqlx::PgPool,
    redis_conn: redis::aio::ConnectionManager,
) -> Arc<PluginRegistry> {
    let registry = PluginRegistry::new();
    let plugins = collect_plugins();

    tracing::info!(count = plugins.len(), "discovered plugins");

    for plugin in plugins {
        let name = plugin.name();
        let prefix = name.to_uppercase();
        let mut plugin_settings = PluginSettings::load(&prefix);

        // DB-stored settings override env vars.
        let db_key = format!("plugin.{name}");
        if let Ok(Some(db_val)) = riven_db::repo::get_setting(&db_pool, &db_key).await {
            plugin_settings.merge_db_override(&db_val);
        }

        registry
            .register(
                plugin,
                plugin_settings,
                http_client.clone(),
                db_pool.clone(),
                redis_conn.clone(),
            )
            .await;
    }

    let _ = settings;
    Arc::new(registry)
}
