use riven_core::vfs_layout::{VfsLibraryLayout, split_path};

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

pub fn parse_path(layout: &VfsLibraryLayout, path: &str) -> PathTarget {
    let segments = split_path(path);
    if segments.is_empty() {
        return PathTarget::Root;
    }

    if let Some(profile) = layout.match_profile(path) {
        if segments.len() <= profile.segments.len() {
            return PathTarget::ProfilePrefixDir;
        }

        let remainder = &segments[profile.segments.len()..];
        return PathTarget::Canonical {
            profile_key: Some(profile.key.clone()),
            path: parse_canonical_segments(remainder),
        };
    }

    if layout.is_profile_prefix(path) {
        return PathTarget::ProfilePrefixDir;
    }

    PathTarget::Canonical {
        profile_key: None,
        path: parse_canonical_segments(&segments),
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
                exclusive: false,
                filter_rules: Default::default(),
            },
        );
        let layout = VfsLibraryLayout::new(FilesystemSettings {
            mount_path: "/mount".to_string(),
            library_profiles: profiles,
        });

        assert_eq!(parse_path(&layout, "/kids"), PathTarget::ProfilePrefixDir);
        assert_eq!(
            parse_path(&layout, "/kids/movies/Film/Film.mkv"),
            PathTarget::Canonical {
                profile_key: Some("kids".to_string()),
                path: CanonicalPath::MovieFile {
                    actual_path: "/movies/Film/Film.mkv".to_string(),
                },
            }
        );
    }

    #[test]
    fn root_entries_include_profile_prefixes_once() {
        let mut profiles = HashMap::new();
        profiles.insert(
            "kids".to_string(),
            FilesystemLibraryProfile {
                name: "Kids".to_string(),
                library_path: "/library/kids".to_string(),
                enabled: true,
                exclusive: false,
                filter_rules: Default::default(),
            },
        );
        profiles.insert(
            "anime".to_string(),
            FilesystemLibraryProfile {
                name: "Anime".to_string(),
                library_path: "/library/anime".to_string(),
                enabled: true,
                exclusive: false,
                filter_rules: Default::default(),
            },
        );

        let layout = VfsLibraryLayout::new(FilesystemSettings {
            mount_path: "/mount".to_string(),
            library_profiles: profiles,
        });

        assert_eq!(
            layout.root_entries(),
            vec![
                "library".to_string(),
                "movies".to_string(),
                "shows".to_string()
            ]
        );
    }

    #[test]
    fn profile_prefix_children_expand_nested_profiles() {
        let mut profiles = HashMap::new();
        profiles.insert(
            "kids".to_string(),
            FilesystemLibraryProfile {
                name: "Kids".to_string(),
                library_path: "/library/kids".to_string(),
                enabled: true,
                exclusive: false,
                filter_rules: Default::default(),
            },
        );

        let layout = VfsLibraryLayout::new(FilesystemSettings {
            mount_path: "/mount".to_string(),
            library_profiles: profiles,
        });

        assert_eq!(
            layout.profile_prefix_children("/"),
            vec!["library".to_string()]
        );
        assert_eq!(
            layout.profile_prefix_children("/library"),
            vec!["kids".to_string()]
        );
        assert_eq!(
            layout.profile_prefix_children("/library/kids"),
            vec!["movies".to_string(), "shows".to_string()]
        );
    }
}
