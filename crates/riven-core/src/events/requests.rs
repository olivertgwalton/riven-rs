use crate::types::MediaItemType;

pub struct ScrapeRequest<'a> {
    pub id: i64,
    pub item_type: MediaItemType,
    pub imdb_id: Option<&'a str>,
    pub tvdb_id: Option<&'a str>,
    pub title: &'a str,
    /// The artist, when `title` is an album.
    pub artist: Option<&'a str>,
    pub year: Option<i32>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
}

impl ScrapeRequest<'_> {
    pub fn season_or_1(&self) -> i32 {
        self.season.unwrap_or(1)
    }

    pub fn episode_or_1(&self) -> i32 {
        self.episode.unwrap_or(1)
    }
}

pub struct IndexRequest<'a> {
    pub id: i64,
    pub item_type: MediaItemType,
    pub imdb_id: Option<&'a str>,
    pub tvdb_id: Option<&'a str>,
    pub tmdb_id: Option<&'a str>,
    pub musicbrainz_id: Option<&'a str>,
    /// Release group MBIDs an artist's request names, which are wanted
    /// whatever their type; `None` asks for the discography.
    pub requested_albums: Option<&'a [String]>,
}

pub struct DownloadSuccessInfo<'a> {
    pub id: i64,
    pub title: &'a str,
    pub full_title: Option<&'a str>,
    pub item_type: MediaItemType,
    pub year: Option<i32>,
    pub imdb_id: Option<&'a str>,
    pub tmdb_id: Option<&'a str>,
    pub poster_path: Option<&'a str>,
    pub plugin_name: &'a str,
    pub provider: Option<&'a str>,
    pub duration_seconds: f64,
}
