use std::borrow::Cow;
use std::collections::BTreeSet;

use crate::settings::FilesystemSettings;

#[derive(Debug, Clone)]
pub struct ActiveLibraryProfile {
    pub key: String,
    pub library_path: String,
    pub segments: Vec<String>,
    pub exclusive: bool,
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
                let segments = split_path(&normalized).into_iter().map(String::from).collect();
                Some(ActiveLibraryProfile {
                    key,
                    exclusive: profile.exclusive,
                    segments,
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
        let mut entries: BTreeSet<Cow<'static, str>> = self
            .profiles
            .iter()
            .filter_map(|profile| profile.segments.first())
            .map(|s| Cow::Owned(s.clone()))
            .collect();
        entries.insert(Cow::Borrowed("movies"));
        entries.insert(Cow::Borrowed("shows"));
        entries.into_iter().map(Cow::into_owned).collect()
    }

    pub fn profile_prefix_children(&self, path: &str) -> Vec<String> {
        let current = split_path(path);
        let mut entries: BTreeSet<Cow<'static, str>> = BTreeSet::new();
        for profile in &self.profiles {
            if !is_prefix(&current, &profile.segments) {
                continue;
            }
            match profile.segments.get(current.len()) {
                Some(next) => {
                    entries.insert(Cow::Owned(next.clone()));
                }
                None => {
                    entries.insert(Cow::Borrowed("movies"));
                    entries.insert(Cow::Borrowed("shows"));
                }
            }
        }
        entries.into_iter().map(Cow::into_owned).collect()
    }

    /// Returns the keys of all enabled exclusive profiles.
    pub fn exclusive_profile_keys(&self) -> Vec<&str> {
        self.profiles
            .iter()
            .filter(|p| p.exclusive)
            .map(|p| p.key.as_str())
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

pub fn split_path(path: &str) -> Vec<&str> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

pub fn is_prefix<A, B>(prefix: &[A], full: &[B]) -> bool
where
    A: AsRef<str>,
    B: AsRef<str>,
{
    prefix.len() <= full.len()
        && prefix
            .iter()
            .zip(full)
            .all(|(a, b)| a.as_ref() == b.as_ref())
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
