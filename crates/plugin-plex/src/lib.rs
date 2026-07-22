use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use riven_core::events::{DownloadSuccessInfo, EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::settings::{FilesystemSettings, LibraryProfileMembership, PluginSettings};
use riven_core::types::{ActivePlaybackSession, PlaybackMethod, PlaybackState};
use riven_db::repo;

mod client;
mod paths;

use client::{PlexSection, get_active_sessions, get_library_sections, refresh_section};
#[cfg(test)]
use client::{map_playback_method, map_playback_state, urlencoding};
use paths::{effective_library_path, entry_vfs_dirs, load_filesystem_settings};

const SECTIONS_CACHE_TTL: Duration = Duration::from_secs(300);

pub(crate) const PROFILE: HttpServiceProfile = HttpServiceProfile::new("plex");

pub struct PlexPlugin {
    sections_cache: Arc<RwLock<Option<(Instant, Vec<PlexSection>)>>>,
}

impl Default for PlexPlugin {
    fn default() -> Self {
        Self {
            sections_cache: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait]
impl Plugin for PlexPlugin {
    fn name(&self) -> &'static str {
        "plex"
    }

    fn category(&self) -> &'static str {
        "media"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[
            EventType::MediaItemDownloadSuccess,
            EventType::ActivePlaybackSessionsRequested,
        ]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(settings.has("plextoken") && settings.has("plexserverurl"))
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::{FieldType, SettingField};
        vec![
            SettingField::new("plextoken", "Plex Token", FieldType::Password).required(),
            SettingField::new("plexserverurl", "Server URL", FieldType::Url)
                .required()
                .with_placeholder("http://localhost:32400"),
            SettingField::new("plexlibrarypath", "Library Path", FieldType::Text)
                .with_default("/mount")
                .with_placeholder("/mount")
                .with_description("Path Plex uses to reference the VFS mount."),
        ]
    }

    async fn on_download_success(
        &self,
        info: &DownloadSuccessInfo<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let id = info.id;
        tracing::debug!(id, "plex: handling download success event");
        let plex_token = ctx.require_setting("plextoken")?;
        let plex_url = ctx.require_setting("plexserverurl")?.trim_end_matches('/');

        let entries = repo::get_media_entries_recursive(id).await?;
        if entries.is_empty() {
            anyhow::bail!("no filesystem entries found for media item {id}");
        }
        tracing::debug!(id, count = entries.len(), "plex: found filesystem entries");

        let fs_settings = load_filesystem_settings().await;
        let library_path =
            effective_library_path(&ctx.settings, fs_settings.as_ref(), &ctx.vfs_mount_path);

        let sections = self
            .cached_library_sections(&ctx.http, plex_url, plex_token)
            .await?;
        tracing::debug!(count = sections.len(), "plex: fetched library sections");

        let section_locations: Vec<String> = sections
            .iter()
            .flat_map(|s| s.locations.iter().map(|l| l.path.clone()))
            .collect();

        let mut refresh_tasks: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        let mut all_vfs_dirs: Vec<String> = Vec::new();
        for entry in &entries {
            let dir_path = entry
                .path
                .rsplit_once('/')
                .map(|(dir, _)| dir)
                .unwrap_or(&entry.path);

            let profile_keys = LibraryProfileMembership::from_json(entry.library_profiles.as_ref());
            let vfs_dirs =
                entry_vfs_dirs(dir_path, &library_path, &profile_keys, fs_settings.as_ref());

            for full_path in &vfs_dirs {
                for section in &sections {
                    for location in &section.locations {
                        if full_path.starts_with(&location.path) {
                            refresh_tasks.insert((section.key.clone(), full_path.clone()));
                        }
                    }
                }
                all_vfs_dirs.push(full_path.clone());
            }
        }

        if refresh_tasks.is_empty() {
            tracing::warn!(
                id,
                tried_paths = ?all_vfs_dirs,
                plex_section_locations = ?section_locations,
                "plex: no library sections matched any entry paths — set plexlibrarypath to the path prefix Plex uses to see the VFS mount"
            );
            return Ok(HookResponse::Empty);
        }

        let results =
            futures::future::join_all(refresh_tasks.into_iter().map(|(section_key, path)| {
                let http = ctx.http.clone();
                let token = plex_token.to_string();
                let url = plex_url.to_string();
                async move {
                    let result = refresh_section(&http, &url, &token, &section_key, &path).await;
                    (section_key, path, result)
                }
            }))
            .await;

        for (section_key, path, result) in results {
            match result {
                Ok(()) => tracing::info!(
                    section = section_key,
                    path,
                    "plex library section refreshed"
                ),
                Err(e) => tracing::warn!(
                    section = section_key,
                    path,
                    error = %e,
                    "plex library section refresh failed"
                ),
            }
        }

        Ok(HookResponse::Empty)
    }

    async fn on_active_playback_sessions_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let plex_token = ctx.require_setting("plextoken")?;
        let plex_url = ctx.require_setting("plexserverurl")?.trim_end_matches('/');
        let sessions = get_active_sessions(&ctx.http, plex_url, plex_token).await?;
        Ok(HookResponse::ActivePlaybackSessions(sessions))
    }
}

impl PlexPlugin {
    async fn cached_library_sections(
        &self,
        http: &riven_core::http::HttpClient,
        plex_url: &str,
        token: &str,
    ) -> anyhow::Result<Vec<PlexSection>> {
        {
            let cache = self.sections_cache.read().await;
            if let Some((fetched_at, ref sections)) = *cache
                && fetched_at.elapsed() < SECTIONS_CACHE_TTL
            {
                tracing::debug!("using cached plex library sections");
                return Ok(sections.clone());
            }
        }

        let sections = get_library_sections(http, plex_url, token).await?;
        *self.sections_cache.write().await = Some((Instant::now(), sections.clone()));
        Ok(sections)
    }
}

#[cfg(test)]
mod tests;
