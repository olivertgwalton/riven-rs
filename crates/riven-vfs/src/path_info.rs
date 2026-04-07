use riven_core::settings::FilesystemSettings;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalPath {
    Root,
    AllMovies,
    MovieDir { actual_dir: String },
    MovieFile { actual_path: String },
    AllShows,
    ShowDir { actual_dir: String },
    SeasonDir { actual_dir: String },
    EpisodeFile { actual_path: String },
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathTarget {
    Root,
    ProfilePrefixDir,
    Canonical {
        profile_key: Option<String>,
        path: CanonicalPath,
    },
    Invalid,
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

    pub fn parse(&self, path: &str) -> PathTarget {
        let segments = split_path(path);
        if segments.is_empty() {
            return PathTarget::Root;
        }

        if let Some(profile) = self
            .profiles
            .iter()
            .filter(|profile| is_prefix(&profile.segments, &segments))
            .max_by_key(|profile| profile.segments.len())
        {
            if segments.len() <= profile.segments.len() {
                return PathTarget::ProfilePrefixDir;
            }

            let remainder = &segments[profile.segments.len()..];
            return PathTarget::Canonical {
                profile_key: Some(profile.key.clone()),
                path: parse_canonical_segments(remainder),
            };
        }

        if self
            .profiles
            .iter()
            .any(|profile| is_prefix(&segments, &profile.segments))
        {
            return PathTarget::ProfilePrefixDir;
        }

        PathTarget::Canonical {
            profile_key: None,
            path: parse_canonical_segments(&segments),
        }
    }

    pub fn root_entries(&self) -> Vec<String> {
        let mut entries = vec!["movies".to_string(), "shows".to_string()];
        for profile in &self.profiles {
            if let Some(first) = profile.segments.first() {
                if !entries.contains(first) {
                    entries.push(first.clone());
                }
            }
        }
        entries.sort();
        entries.dedup();
        entries
    }

    pub fn profile_prefix_children(&self, path: &str) -> Vec<String> {
        let current = split_path(path);
        let mut children = Vec::new();

        for profile in &self.profiles {
            if !is_prefix(&current, &profile.segments) {
                continue;
            }
            if let Some(next) = profile.segments.get(current.len()) {
                children.push(next.clone());
            } else {
                children.push("movies".to_string());
                children.push("shows".to_string());
            }
        }

        children.sort();
        children.dedup();
        children
    }
}

fn parse_canonical_segments(segments: &[String]) -> CanonicalPath {
    match segments {
        [] => CanonicalPath::Root,
        [first] if first == "movies" => CanonicalPath::AllMovies,
        [first, dir] if first == "movies" => CanonicalPath::MovieDir {
            actual_dir: format!("/movies/{dir}"),
        },
        [first, dir, file] if first == "movies" => CanonicalPath::MovieFile {
            actual_path: format!("/movies/{dir}/{file}"),
        },
        [first] if first == "shows" => CanonicalPath::AllShows,
        [first, dir] if first == "shows" => CanonicalPath::ShowDir {
            actual_dir: format!("/shows/{dir}"),
        },
        [first, dir, season] if first == "shows" => CanonicalPath::SeasonDir {
            actual_dir: format!("/shows/{dir}/{season}"),
        },
        [first, dir, season, file] if first == "shows" => CanonicalPath::EpisodeFile {
            actual_path: format!("/shows/{dir}/{season}/{file}"),
        },
        _ => CanonicalPath::Invalid,
    }
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

fn split_path(path: &str) -> Vec<String> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn is_prefix(prefix: &[String], full: &[String]) -> bool {
    prefix.len() <= full.len() && prefix.iter().zip(full).all(|(a, b)| a == b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riven_core::settings::{FilesystemLibraryProfile, FilesystemSettings};
    use std::collections::HashMap;

    #[test]
    fn parses_profile_prefixed_canonical_paths() {
        let mut profiles = HashMap::new();
        profiles.insert(
            "kids".to_string(),
            FilesystemLibraryProfile {
                name: "Kids".to_string(),
                library_path: "/kids".to_string(),
                enabled: true,
                filter_rules: Default::default(),
            },
        );
        let layout = VfsLibraryLayout::new(FilesystemSettings {
            mount_path: "/mount".to_string(),
            library_profiles: profiles,
        });

        assert_eq!(layout.parse("/kids"), PathTarget::ProfilePrefixDir);
        assert_eq!(
            layout.parse("/kids/movies/Film/Film.mkv"),
            PathTarget::Canonical {
                profile_key: Some("kids".to_string()),
                path: CanonicalPath::MovieFile {
                    actual_path: "/movies/Film/Film.mkv".to_string(),
                },
            }
        );
    }
}
