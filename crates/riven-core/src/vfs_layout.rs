use std::collections::BTreeSet;

use crate::settings::FilesystemSettings;

#[derive(Debug, Clone)]
pub struct ActiveLibraryProfile {
    pub key: String,
    pub library_path: String,
    pub segments: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct VfsLibraryLayout {
    profiles: Vec<ActiveLibraryProfile>,
}

impl VfsLibraryLayout {
    pub fn new(settings: FilesystemSettings) -> Self {
        let mut profiles: Vec<_> = settings
            .library_profiles
            .into_iter()
            .filter_map(|(key, profile)| {
                if !profile.enabled {
                    return None;
                }
                let normalized = normalize_library_path(&profile.library_path)?;
                Some(ActiveLibraryProfile {
                    key,
                    segments: normalized
                        .trim_start_matches('/')
                        .split('/')
                        .map(ToString::to_string)
                        .collect(),
                    library_path: normalized,
                })
            })
            .collect();
        profiles.sort_by(|a, b| a.library_path.cmp(&b.library_path));
        Self { profiles }
    }

    pub fn profiles(&self) -> &[ActiveLibraryProfile] {
        &self.profiles
    }

    pub fn root_entries(&self) -> Vec<String> {
        self.profiles
            .iter()
            .filter_map(|profile| profile.segments.first().cloned())
            .chain(["movies".to_string(), "shows".to_string()])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn profile_prefix_children(&self, path: &str) -> Vec<String> {
        let current = split_path(path);

        self.profiles
            .iter()
            .filter(|profile| is_prefix(&current, &profile.segments))
            .flat_map(|profile| match profile.segments.get(current.len()) {
                Some(next) => vec![next.clone()],
                None => vec!["movies".to_string(), "shows".to_string()],
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn match_profile<'a>(&'a self, path: &str) -> Option<&'a ActiveLibraryProfile> {
        let segments = split_path(path);
        self.profiles
            .iter()
            .filter(|profile| is_prefix(&profile.segments, &segments))
            .max_by_key(|profile| profile.segments.len())
    }

    pub fn is_profile_prefix(&self, path: &str) -> bool {
        let segments = split_path(path);
        self.profiles
            .iter()
            .any(|profile| is_prefix(&segments, &profile.segments))
    }
}

pub fn split_path(path: &str) -> Vec<String> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub fn is_prefix(prefix: &[String], full: &[String]) -> bool {
    prefix.len() <= full.len() && prefix.iter().zip(full).all(|(a, b)| a == b)
}

fn normalize_library_path(path: &str) -> Option<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = format!("/{}", trimmed.trim_matches('/'));
    if normalized == "/" || normalized == "/movies" || normalized == "/shows" {
        return None;
    }
    Some(normalized)
}
