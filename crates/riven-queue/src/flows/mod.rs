pub mod download_item;
pub mod index_item;
pub mod request_content;
pub mod scrape_item;

use riven_db::{entities::MediaItem, repo};
use riven_rank::{QualityProfile, RankSettings};

/// Load a media item by id, logging an error and returning `None` on failure.
pub(crate) async fn load_item_or_log(id: i64, db_pool: &sqlx::PgPool, context: &str) -> Option<MediaItem> {
    match repo::get_media_item(db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => { tracing::error!(id, "media item not found for {context}"); None }
        Err(e) => { tracing::error!(id, error = %e, "failed to load media item for {context}"); None }
    }
}

/// Load `RankSettings` for every profile that has `enabled = true` in the
/// `ranking_profiles` DB table.
///
/// Built-in profiles start from Rust-computed defaults, then merge any user
/// overrides from the DB `settings` column on top. Custom profiles use the
/// `settings` JSON stored in the DB directly.
///
/// Returns `(profile_name, RankSettings)` pairs. An empty result means
/// single-version mode — the caller should fall back to the active
/// `rank_settings` DB key.
pub(crate) async fn load_active_profiles(
    db_pool: &sqlx::PgPool,
) -> Vec<(String, RankSettings)> {
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
                        // DB has a full settings object — use it directly (the frontend
                        // round-trips through RankSettings so it's always complete).
                        tracing::debug!(profile = p.name, "built-in profile: using DB settings override");
                        match serde_json::from_value::<RankSettings>(p.settings.clone()) {
                            Ok(s) => {
                                tracing::debug!(
                                    profile = q.id(),
                                    r2160p = s.resolutions.r2160p,
                                    r1080p = s.resolutions.r1080p,
                                    r720p = s.resolutions.r720p,
                                    "loaded profile resolutions from DB"
                                );
                                s.prepare()
                            }
                            Err(e) => {
                                tracing::warn!(profile = p.name, error = %e, "failed to parse DB settings, falling back to Rust defaults");
                                q.base_settings().prepare()
                            }
                        }
                    })
            } else {
                serde_json::from_value::<RankSettings>(p.settings)
                    .ok()
                    .map(|s| s.prepare())
            };
            settings.map(|s| (p.name, s))
        })
        .collect()
}

