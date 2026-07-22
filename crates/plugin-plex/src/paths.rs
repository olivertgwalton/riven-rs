use super::*;

pub(crate) async fn load_filesystem_settings() -> Option<FilesystemSettings> {
    riven_db::repo::get_setting("filesystem")
        .await
        .ok()
        .flatten()
        .and_then(|v| serde_json::from_value(v).ok())
}

/// Returns the effective Plex library path: the explicit plugin setting if configured,
/// otherwise the VFS mount path from filesystem settings, otherwise the app-level VFS mount path.
pub(crate) fn effective_library_path(
    settings: &riven_core::settings::PluginSettings,
    fs_settings: Option<&FilesystemSettings>,
    app_vfs_mount_path: &str,
) -> String {
    if let Some(explicit) = settings.get("plexlibrarypath") {
        return explicit.trim_end_matches('/').to_string();
    }
    let from_fs = fs_settings
        .map(|s| s.mount_path.trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty());
    if let Some(path) = from_fs {
        return path;
    }
    let app_path = app_vfs_mount_path.trim_end_matches('/');
    if !app_path.is_empty() {
        return app_path.to_string();
    }
    "/mount".to_string()
}

/// Returns all VFS directory paths an entry appears at, given its canonical dir path and profile keys.
pub(crate) fn entry_vfs_dirs(
    canonical_dir: &str,
    plex_library_path: &str,
    profile_keys: &LibraryProfileMembership,
    fs_settings: Option<&FilesystemSettings>,
) -> Vec<String> {
    let base = plex_library_path.trim_end_matches('/');
    let mut paths = Vec::new();
    let mut any_exclusive = false;

    if let Some(settings) = fs_settings {
        for key in &profile_keys.0 {
            if let Some(profile) = settings.library_profiles.get(key)
                && profile.enabled
            {
                paths.push(format!("{base}{}{canonical_dir}", profile.library_path));
                if profile.exclusive {
                    any_exclusive = true;
                }
            }
        }
    }

    if !any_exclusive {
        paths.push(format!("{base}{canonical_dir}"));
    }

    paths
}
