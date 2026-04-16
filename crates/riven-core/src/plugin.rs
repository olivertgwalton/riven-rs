mod collection;
mod context;
mod registry;
mod schema;
mod traits;

pub use collection::ContentCollection;
pub use context::{PluginContext, validate_api_key};
pub use registry::{
    ActivePlugin, PLUGIN_ENABLED_PREFIX, PluginInfo, PluginRegistration, PluginRegistry,
    collect_plugins,
};
pub use schema::SettingField;
pub use traits::Plugin;

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
mod tests;
