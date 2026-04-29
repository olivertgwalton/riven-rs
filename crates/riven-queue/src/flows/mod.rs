pub mod download_item;
pub mod request_content;

use std::future::Future;

use futures::future;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::http::{RateLimitedError, RetryLaterError};
use riven_db::repo;
use riven_rank::{QualityProfile, RankSettings};
use serde::Serialize;
use serde_json::Value;

use crate::JobQueue;

pub(crate) async fn start_plugin_flow<Push, Fut>(
    queue: &JobQueue,
    prefix: &str,
    id: i64,
    event_type: EventType,
    push_plugin_job: Push,
) -> usize
where
    Push: FnMut(String) -> Fut,
    Fut: Future<Output = ()>,
{
    let subscribers = queue.registry.subscriber_names(event_type).await;
    let pending = subscribers.len();

    if pending == 0 {
        return 0;
    }

    queue.init_flow(prefix, id, pending).await;

    // Push all plugin jobs concurrently — each is an independent Redis RPUSH.
    future::join_all(subscribers.into_iter().map(push_plugin_job)).await;

    pending
}

pub(crate) async fn run_plugin_hook<T, Extract>(
    queue: &JobQueue,
    prefix: &str,
    id: i64,
    plugin_name: &str,
    event: &RivenEvent,
    hook_label: &str,
    extract: Extract,
) -> bool
where
    T: Serialize,
    Extract: FnOnce(HookResponse) -> Option<T>,
{
    match queue.registry.dispatch_to_plugin(plugin_name, event).await {
        Some(Ok(response)) => {
            if let Some(payload) = extract(response) {
                tracing::debug!(plugin = plugin_name, id, "{hook_label} responded");
                queue
                    .flow_store_result(prefix, id, plugin_name, &payload)
                    .await;
            }
        }
        Some(Err(ref error)) if error.is::<RateLimitedError>() || error.is::<RetryLaterError>() => {
            // Record that this child was deferred so `finalize` can distinguish
            // a temporary upstream failure from a genuine no-results verdict.
            queue.flow_increment_rate_limited(prefix, id).await;
            tracing::warn!(
                plugin = plugin_name,
                id,
                "{hook_label} deferred; worker slot freed"
            );
        }
        Some(Err(error)) => {
            tracing::error!(
                plugin = plugin_name,
                id,
                error = %error,
                "{hook_label} hook failed"
            );
        }
        None => {
            tracing::warn!(plugin = plugin_name, id, "{hook_label} not found");
        }
    }

    queue.flow_complete_child(prefix, id).await
}

/// Load `RankSettings` for every profile that has `enabled = true` in the
/// `ranking_profiles` DB table.
///
/// Built-in profiles start from Rust-computed defaults, then merge any user
/// overrides from the DB `settings` column on top. Custom profiles use the
/// `settings` JSON stored in the DB directly.
///
/// Returns `(profile_name, RankSettings)` pairs.
pub(crate) async fn load_active_profiles(db_pool: &sqlx::PgPool) -> Vec<(String, RankSettings)> {
    let profiles = match repo::get_enabled_profiles(db_pool).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "failed to load enabled ranking profiles");
            return Vec::new();
        }
    };

    profiles
        .into_iter()
        .filter_map(|p| {
            let settings = if p.is_builtin {
                QualityProfile::ALL
                    .iter()
                    .find(|q| q.id() == p.name.as_str())
                    .map(|q| {
                        let db_empty = matches!(&p.settings, serde_json::Value::Object(m) if m.is_empty())
                            || matches!(&p.settings, serde_json::Value::Null);
                        if db_empty {
                            tracing::debug!(profile = p.name, "built-in profile: using Rust defaults (no DB overrides)");
                            return q.base_settings().prepare();
                        }
                        tracing::debug!(profile = p.name, "built-in profile: merging DB settings override with Rust defaults");
                        match merge_builtin_profile_settings(*q, &p.settings) {
                            Ok(s) => {
                                tracing::debug!(
                                    profile = q.id(),
                                    r2160p = s.resolutions.high_definition.r2160p,
                                    r1080p = s.resolutions.high_definition.r1080p,
                                    r720p = s.resolutions.high_definition.r720p,
                                    unknown = s.resolutions.unknown,
                                    "loaded profile resolutions from DB"
                                );
                                s
                            }
                            Err(e) => {
                                tracing::warn!(profile = p.name, error = %e, "failed to parse DB settings, falling back to Rust defaults");
                                q.base_settings().prepare()
                            }
                        }
                    })
            } else {
                match serde_json::from_value::<RankSettings>(p.settings) {
                    Ok(s) => Some(RankSettings::prepare(s)),
                    Err(e) => {
                        tracing::warn!(profile = p.name, error = %e, "failed to parse custom profile settings, skipping");
                        None
                    }
                }
            };
            settings.map(|s| (p.name, s))
        })
        .collect()
}

pub fn merge_builtin_profile_settings(
    profile: QualityProfile,
    override_settings: &Value,
) -> serde_json::Result<RankSettings> {
    let mut merged = serde_json::to_value(profile.base_settings())?;
    merge_json_value(&mut merged, override_settings);
    serde_json::from_value::<RankSettings>(merged).map(RankSettings::prepare)
}

fn merge_json_value(base: &mut Value, override_value: &Value) {
    match (base, override_value) {
        (Value::Object(base_obj), Value::Object(override_obj)) => {
            for (key, value) in override_obj {
                match base_obj.get_mut(key) {
                    Some(existing) => merge_json_value(existing, value),
                    None => {
                        base_obj.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base_slot, replacement) => {
            *base_slot = replacement.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::merge_builtin_profile_settings;
    use riven_rank::QualityProfile;
    use serde_json::json;

    #[test]
    fn built_in_profile_overrides_are_merged_on_top_of_preset() {
        let settings = merge_builtin_profile_settings(
            QualityProfile::UltraHd,
            &json!({
                "resolutions": {
                    "r1080p": true
                }
            }),
        )
        .expect("settings should parse");

        assert!(settings.resolutions.high_definition.r2160p);
        assert!(settings.resolutions.high_definition.r1080p);
        assert!(!settings.resolutions.high_definition.r720p);
        assert!(!settings.custom_ranks.quality.hdtv.fetch);
        assert!(!settings.custom_ranks.rips.webrip.fetch);
        assert!(!settings.custom_ranks.audio.mono.fetch);
    }
}
