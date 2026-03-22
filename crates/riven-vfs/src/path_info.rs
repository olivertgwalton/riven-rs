use regex::Regex;
use std::sync::LazyLock;

/// Parsed path information from a VFS path.
#[derive(Debug, Clone)]
pub struct PathInfo {
    pub path_type: PathType,
    pub raw: String,
}

#[derive(Debug, Clone)]
pub enum PathType {
    Root,
    AllMovies,
    MovieDir {
        pretty_name: String,
        tmdb_id: Option<String>,
    },
    MovieFile {
        pretty_name: String,
        tmdb_id: Option<String>,
        filename: String,
    },
    AllShows,
    ShowDir {
        pretty_name: String,
        tvdb_id: Option<String>,
    },
    SeasonDir {
        show_pretty_name: String,
        tvdb_id: Option<String>,
        season_number: i32,
    },
    EpisodeFile {
        show_pretty_name: String,
        tvdb_id: Option<String>,
        season_number: i32,
        filename: String,
    },
}

static RE_TMDB: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{tmdb-(\d+)\}").unwrap());
static RE_TVDB: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{tvdb-(\d+)\}").unwrap());
static RE_SEASON: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^Season (\d{2})$").unwrap());

pub fn parse_path(path: &str) -> PathInfo {
    let parts: Vec<&str> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    let path_type = match parts.as_slice() {
        [] => PathType::Root,
        ["movies"] => PathType::AllMovies,
        ["movies", dir] => {
            let tmdb_id = RE_TMDB.captures(dir).map(|c| c[1].to_string());
            PathType::MovieDir {
                pretty_name: dir.to_string(),
                tmdb_id,
            }
        }
        ["movies", dir, file] => {
            let tmdb_id = RE_TMDB.captures(dir).map(|c| c[1].to_string());
            PathType::MovieFile {
                pretty_name: dir.to_string(),
                tmdb_id,
                filename: file.to_string(),
            }
        }
        ["shows"] => PathType::AllShows,
        ["shows", dir] => {
            let tvdb_id = RE_TVDB.captures(dir).map(|c| c[1].to_string());
            PathType::ShowDir {
                pretty_name: dir.to_string(),
                tvdb_id,
            }
        }
        ["shows", dir, season_str] => {
            let tvdb_id = RE_TVDB.captures(dir).map(|c| c[1].to_string());
            let season_number = RE_SEASON
                .captures(season_str)
                .and_then(|c| c[1].parse().ok())
                .unwrap_or(0);
            PathType::SeasonDir {
                show_pretty_name: dir.to_string(),
                tvdb_id,
                season_number,
            }
        }
        ["shows", dir, _season_str, file] => {
            let tvdb_id = RE_TVDB.captures(dir).map(|c| c[1].to_string());
            // Extract season from path
            let season_number = parts
                .get(2)
                .and_then(|s| RE_SEASON.captures(s))
                .and_then(|c| c[1].parse().ok())
                .unwrap_or(0);
            PathType::EpisodeFile {
                show_pretty_name: dir.to_string(),
                tvdb_id,
                season_number,
                filename: file.to_string(),
            }
        }
        _ => PathType::Root,
    };

    PathInfo {
        path_type,
        raw: path.to_string(),
    }
}
