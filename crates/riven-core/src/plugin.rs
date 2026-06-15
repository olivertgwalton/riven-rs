mod collection;
mod context;
mod registry;
mod schema;
mod traits;

pub use collection::ContentCollection;
pub use context::{PluginContext, validate_api_key};
pub use registry::{ActivePlugin, PLUGIN_ENABLED_PREFIX, PluginInfo, PluginRegistry};
pub use schema::SettingField;
pub use traits::Plugin;

#[cfg(test)]
mod tests;
