use async_graphql::*;
use riven_core::plugin::{Display, FieldType, PluginRegistry, SettingField};
use riven_core::settings::RivenSettings;
use riven_db::repo;
use std::sync::Arc;

use crate::schema::auth::require_settings_access;
use crate::schema::types::{InstanceStatus, SettingsSection, SetupGroup};

mod general;
mod plugins;
mod ranking;

pub(crate) use general::build_general_section;
pub(crate) use plugins::build_plugin_section;
use plugins::plugin_section_from;
use ranking::{
    build_rank_settings_schema, effective_profile_settings, inject_rank_defaults, strip_zero_ranks,
};

#[derive(Default)]
pub struct CoreSettingsQuery;

#[Object]
impl CoreSettingsQuery {
    /// Return all quality profiles as an ordered array of
    /// `{ id, label, description, settings }` objects.
    /// The `settings` field reflects the *effective* settings (base preset merged
    /// with any user overrides stored in the database), so the UI always shows
    /// the values that will actually be used at runtime.
    async fn quality_profiles(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;

        let db_profiles = repo::list_ranking_profiles().await.unwrap_or_default();

        let profiles: serde_json::Value = riven_rank::QualityProfile::ALL
            .iter()
            .map(|&p| {
                let db_row = db_profiles.iter().find(|r| r.name == p.id());

                let mut settings = effective_profile_settings(p, db_row.map(|row| &row.settings));
                inject_rank_defaults(&mut settings);
                serde_json::json!({
                    "id":          p.id(),
                    "label":       p.label(),
                    "description": p.description(),
                    "settings":    settings,
                })
            })
            .collect();
        Ok(profiles)
    }

    async fn rank_settings_schema(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        Ok(serde_json::to_value(build_rank_settings_schema())
            .unwrap_or(serde_json::Value::Array(vec![])))
    }

    async fn default_rank_profile(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let db_profiles = repo::list_ranking_profiles().await.unwrap_or_default();

        // Prefer an enabled built-in preset (matched by name to a QualityProfile).
        if let Some(preset) = riven_rank::QualityProfile::ALL
            .iter()
            .find(|p| db_profiles.iter().any(|r| r.enabled && r.name == p.id()))
        {
            let row = db_profiles.iter().find(|r| r.name == preset.id());
            let mut settings = effective_profile_settings(*preset, row.map(|row| &row.settings));
            inject_rank_defaults(&mut settings);
            return Ok(serde_json::json!({ "name": preset.id(), "settings": settings }));
        }

        // Then an enabled custom (non-built-in) profile.
        if let Some(row) = db_profiles.iter().find(|r| r.enabled && !r.is_builtin) {
            let mut settings = row.settings.clone();
            inject_rank_defaults(&mut settings);
            return Ok(serde_json::json!({ "name": row.name, "settings": settings }));
        }

        // Otherwise the global rank settings, with no active profile.
        let settings: riven_rank::RankSettings = repo::get_setting("rank_settings")
            .await?
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let mut json = serde_json::to_value(&settings)
            .map_err(|e| Error::new(format!("failed to serialise rank settings: {e}")))?;
        strip_zero_ranks(&mut json);
        inject_rank_defaults(&mut json);
        Ok(serde_json::json!({ "name": serde_json::Value::Null, "settings": json }))
    }

    /// Return all ranking profiles (built-in + custom) with their enabled status.
    async fn custom_profiles(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        let profiles = repo::list_ranking_profiles().await?;
        Ok(serde_json::to_value(profiles)?)
    }

    /// Get all stored settings as a JSON object.
    async fn all_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        require_settings_access(ctx)?;
        Ok(repo::get_all_settings().await?)
    }

    /// Return instance-level status flags used by frontend bootstrap flows.
    /// Owns the setup-readiness rule so the UI never has to recompute it.
    async fn instance_status(&self, ctx: &Context<'_>) -> Result<InstanceStatus> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        let db_setup_completed = matches!(
            repo::get_setting("instance.setup_completed").await?,
            Some(serde_json::Value::Bool(true))
        );
        let setup_completed =
            env_bool("RIVEN_SETTING__SETUP_COMPLETED").unwrap_or(db_setup_completed);

        let enabled_profile_count = repo::get_enabled_profiles().await?.len() as i32;
        let enabled_valid_plugin_count = registry
            .all_plugins_info()
            .await
            .into_iter()
            .filter(|p| p.enabled && p.valid)
            .count() as i32;

        let mut blockers = Vec::new();
        if enabled_valid_plugin_count == 0 {
            blockers.push(
                "Enable and configure at least one plugin (a media server and a content \
                       source)."
                    .to_string(),
            );
        }
        if enabled_profile_count == 0 {
            blockers.push("Enable at least one quality profile.".to_string());
        }
        let ready_to_complete = blockers.is_empty();

        Ok(InstanceStatus {
            setup_completed,
            ready_to_complete,
            enabled_valid_plugin_count,
            enabled_profile_count,
            blockers,
        })
    }

    /// Ordered setup sections that plugins are grouped under (by `PluginInfo.category`).
    /// This is the single source of truth for setup-step grouping, labels, and order.
    async fn setup_groups(&self, ctx: &Context<'_>) -> Result<Vec<SetupGroup>> {
        require_settings_access(ctx)?;
        Ok([
            (
                "media",
                "Media Servers",
                "Pick the server Riven should update after downloads finish.",
            ),
            (
                "sources",
                "Content Sources",
                "Pick the sources Riven should scrape from.",
            ),
            (
                "services",
                "Metadata & Requests",
                "Connect metadata, lists, calendars, and request services.",
            ),
        ]
        .map(|(id, title, description)| SetupGroup {
            id: id.into(),
            title: title.into(),
            description: description.into(),
        })
        .into())
    }

    /// Every configurable settings surface — the instance-wide "general"
    /// section plus one section per plugin — each with the schema to render it
    /// and its typed values. This is the single read the settings/setup UIs use.
    async fn settings_sections(&self, ctx: &Context<'_>) -> Result<Vec<SettingsSection>> {
        require_settings_access(ctx)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        let mut sections = vec![build_general_section().await?];
        for p in registry.all_plugins_info().await {
            sections.push(plugin_section_from(registry, &p).await);
        }
        Ok(sections)
    }
}

/// Strict boolean env var: `true` => Some(true), `false` => Some(false),
/// anything else (or unset) => None (caller falls back).
fn env_bool(key: &str) -> Option<bool> {
    match std::env::var(key)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}
