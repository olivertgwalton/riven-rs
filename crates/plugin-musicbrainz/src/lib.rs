use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::NaiveDate;
use serde::Deserialize;

use riven_core::events::{EventType, HookResponse, IndexRequest};
use riven_core::http::{HttpClient, HttpServiceProfile};
use riven_core::plugin::{FieldType, Plugin, PluginContext, SettingField};
use riven_core::settings::PluginSettings;
use riven_core::types::{IndexedAlbum, IndexedMediaItem, IndexedTrack, MediaItemType};

/// MusicBrainz allows one request a second from each application and answers
/// more with a 503 rather than a 429 riven could react to, so the cap is kept
/// here instead.
pub const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("musicbrainz").with_rate_limit(1, Duration::from_secs(1));

const BASE_URL: &str = "https://musicbrainz.org/ws/2/";
const COVER_ART_URL: &str = "https://coverartarchive.org/";
/// MusicBrainz refuses clients that do not say who they are and how to reach
/// whoever runs them.
const USER_AGENT: &str = concat!(
    "riven-rs/",
    env!("CARGO_PKG_VERSION"),
    " ( https://github.com/olivertgwalton/riven-rs )"
);
/// The most MusicBrainz returns in one browse page.
const PAGE_SIZE: usize = 100;
/// Media that are films rather than sound; a release that carries one beside
/// its CD lists the film's chapters as tracks.
const VIDEO_FORMATS: &[&str] = &["DVD-Video", "Blu-ray", "HD-DVD", "VHS", "VCD", "SVCD"];

/// Which kinds of release group make up an artist's discography.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleaseType {
    Album,
    Ep,
    Single,
}

impl ReleaseType {
    const ALL: [Self; 3] = [Self::Album, Self::Ep, Self::Single];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Album => "album",
            Self::Ep => "ep",
            Self::Single => "single",
        }
    }

    fn from_setting(settings: &PluginSettings) -> Vec<Self> {
        let chosen: Vec<Self> = settings
            .get_list("release_types")
            .iter()
            .filter_map(|value| {
                Self::ALL
                    .into_iter()
                    .find(|kind| kind.as_str().eq_ignore_ascii_case(value))
            })
            .collect();
        if chosen.is_empty() {
            vec![Self::Album]
        } else {
            chosen
        }
    }
}

pub struct MusicbrainzPlugin;

#[async_trait]
impl Plugin for MusicbrainzPlugin {
    fn name(&self) -> &'static str {
        "musicbrainz"
    }

    fn category(&self) -> &'static str {
        "services"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemIndexRequested]
    }

    async fn validate(
        &self,
        _settings: &PluginSettings,
        _http: &HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        let kinds: Vec<&'static str> = ReleaseType::ALL.iter().map(|kind| kind.as_str()).collect();
        vec![
            SettingField::new("release_types", "Discography", FieldType::StringArray)
                .with_options(&kinds)
                .with_default("album")
                .with_description(
                    "Which release groups an artist followed in full is made of. Live albums, compilations, remixes and other secondary types are left out; an album asked for by name is fetched whatever its type.",
                ),
        ]
    }

    async fn on_index_requested(
        &self,
        request: &IndexRequest<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        if request.item_type != MediaItemType::Artist {
            return Ok(HookResponse::Empty);
        }
        let Some(artist_id) = request.musicbrainz_id else {
            return Ok(HookResponse::Empty);
        };
        let release_types = ReleaseType::from_setting(&ctx.settings);
        let indexed = index_artist(
            &ctx.http,
            artist_id,
            request.requested_albums,
            &release_types,
        )
        .await?;
        Ok(HookResponse::Index(Box::new(indexed)))
    }
}

async fn get<T: serde::de::DeserializeOwned>(http: &HttpClient, path: &str) -> anyhow::Result<T> {
    let url = format!("{BASE_URL}{path}");
    http.get_json(PROFILE, url.clone(), |client| {
        client
            .get(&url)
            .header("User-Agent", USER_AGENT)
            .header("Accept", "application/json")
    })
    .await
}

async fn index_artist(
    http: &HttpClient,
    artist_id: &str,
    requested_albums: Option<&[String]>,
    release_types: &[ReleaseType],
) -> anyhow::Result<IndexedMediaItem> {
    let artist: MbArtist = get(
        http,
        &format!("artist/{artist_id}?inc=aliases+genres&fmt=json"),
    )
    .await?;

    let groups = match requested_albums {
        Some(ids) => {
            let mut groups = Vec::with_capacity(ids.len());
            for id in ids {
                groups.push(
                    get::<MbReleaseGroup>(http, &format!("release-group/{id}?fmt=json")).await?,
                );
            }
            groups
        }
        None => discography(http, artist_id, release_types).await?,
    };

    let mut albums = Vec::with_capacity(groups.len());
    for group in &groups {
        let page: MbReleasePage = get(
            http,
            &format!(
                "release?release-group={}&status=official&inc=recordings+media&limit={PAGE_SIZE}&fmt=json",
                group.id
            ),
        )
        .await?;
        match album_from_releases(group, &page.releases) {
            Some(album) => albums.push(album),
            None => tracing::debug!(
                release_group = %group.id,
                title = %group.title,
                "musicbrainz: release group has no official release with a tracklist; leaving it out"
            ),
        }
    }

    Ok(artist_item(artist, albums))
}

/// Every release group of the chosen types that is the artist's own work:
/// one with a secondary type is a live album, compilation, remix, soundtrack
/// or the like.
async fn discography(
    http: &HttpClient,
    artist_id: &str,
    release_types: &[ReleaseType],
) -> anyhow::Result<Vec<MbReleaseGroup>> {
    let types = release_types
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join("|");
    let mut groups = Vec::new();
    let mut offset = 0;
    loop {
        let page: MbReleaseGroupPage = get(
            http,
            &format!(
                "release-group?artist={artist_id}&type={types}&limit={PAGE_SIZE}&offset={offset}&fmt=json"
            ),
        )
        .await?;
        let fetched = page.release_groups.len();
        groups.extend(
            page.release_groups
                .into_iter()
                .filter(|group| group.secondary_types.is_empty()),
        );
        offset += fetched;
        if fetched == 0 || offset >= page.release_group_count {
            break;
        }
    }
    Ok(groups)
}

fn artist_item(artist: MbArtist, mut albums: Vec<IndexedAlbum>) -> IndexedMediaItem {
    albums.sort_by_key(|album| album.aired_at);
    let mut aliases: HashMap<String, Vec<String>> = HashMap::new();
    for alias in artist.aliases {
        aliases
            .entry(alias.locale.unwrap_or_else(|| "und".to_owned()))
            .or_default()
            .push(alias.name);
    }
    let mut genres = artist.genres;
    genres.sort_by_key(|genre| std::cmp::Reverse(genre.count));
    // MusicBrainz has no artist images; the newest cover stands for one.
    let poster_path = albums
        .iter()
        .rev()
        .find_map(|album| album.poster_path.clone());
    IndexedMediaItem {
        title: Some(artist.name),
        musicbrainz_id: Some(artist.id),
        country: artist.country,
        genres: (!genres.is_empty()).then(|| genres.into_iter().map(|genre| genre.name).collect()),
        aliases: (!aliases.is_empty()).then_some(aliases),
        poster_path,
        albums: Some(albums),
        ..IndexedMediaItem::default()
    }
}

fn album_from_releases(group: &MbReleaseGroup, releases: &[MbRelease]) -> Option<IndexedAlbum> {
    let release = choose_release(releases)?;
    let tracks = tracks_of(release);
    if tracks.is_empty() {
        return None;
    }
    let poster_path = if release.cover_art_archive.front {
        Some(format!("{COVER_ART_URL}release/{}/front-500", release.id))
    } else if releases.iter().any(|other| other.cover_art_archive.front) {
        Some(format!(
            "{COVER_ART_URL}release-group/{}/front-500",
            group.id
        ))
    } else {
        None
    };
    Some(IndexedAlbum {
        musicbrainz_id: group.id.clone(),
        release_id: Some(release.id.clone()),
        title: group.title.clone(),
        aired_at: group.first_release_date.as_deref().and_then(parse_date),
        poster_path,
        genres: None,
        tracks,
    })
}

/// The release whose tracklist the album is taken to be. Releases are named
/// for the tracklist most editions share — a deluxe or bonus-track edition
/// adds to it — so of the releases with the commonest track count, the
/// earliest is chosen, one with cover art before one without.
fn choose_release(releases: &[MbRelease]) -> Option<&MbRelease> {
    let mut counts: HashMap<usize, usize> = HashMap::new();
    for release in releases {
        let count = audio_track_count(release);
        if count > 0 {
            *counts.entry(count).or_default() += 1;
        }
    }
    let (&common, _) = counts
        .iter()
        .max_by(|(count_a, seen_a), (count_b, seen_b)| {
            seen_a.cmp(seen_b).then(count_b.cmp(count_a))
        })?;
    releases
        .iter()
        .filter(|release| audio_track_count(release) == common)
        .min_by(|a, b| {
            (
                !a.cover_art_archive.front,
                a.date.as_deref().unwrap_or("9999"),
            )
                .cmp(&(
                    !b.cover_art_archive.front,
                    b.date.as_deref().unwrap_or("9999"),
                ))
        })
}

fn is_audio(medium: &MbMedium) -> bool {
    !medium
        .format
        .as_deref()
        .is_some_and(|format| VIDEO_FORMATS.contains(&format))
}

fn audio_track_count(release: &MbRelease) -> usize {
    release
        .media
        .iter()
        .filter(|medium| is_audio(medium))
        .map(|medium| medium.tracks.len())
        .sum()
}

fn tracks_of(release: &MbRelease) -> Vec<IndexedTrack> {
    release
        .media
        .iter()
        .filter(|medium| is_audio(medium))
        .flat_map(|medium| {
            medium.tracks.iter().map(|track| IndexedTrack {
                musicbrainz_id: track
                    .recording
                    .as_ref()
                    .map(|recording| recording.id.clone()),
                disc_number: medium.position,
                number: track.position,
                title: track.title.clone(),
            })
        })
        .collect()
}

/// MusicBrainz dates are as precise as is known: a year, a month or a day.
fn parse_date(value: &str) -> Option<NaiveDate> {
    let mut parts = value.split('-').map(str::parse::<u32>);
    let year = i32::try_from(parts.next()?.ok()?).ok()?;
    let month = parts.next().and_then(Result::ok).unwrap_or(1);
    let day = parts.next().and_then(Result::ok).unwrap_or(1);
    NaiveDate::from_ymd_opt(year, month, day)
}

#[derive(Debug, Deserialize)]
struct MbArtist {
    id: String,
    name: String,
    country: Option<String>,
    #[serde(default)]
    aliases: Vec<MbAlias>,
    #[serde(default)]
    genres: Vec<MbGenre>,
}

#[derive(Debug, Deserialize)]
struct MbAlias {
    name: String,
    locale: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MbGenre {
    name: String,
    #[serde(default)]
    count: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MbReleaseGroupPage {
    release_group_count: usize,
    release_groups: Vec<MbReleaseGroup>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MbReleaseGroup {
    id: String,
    title: String,
    first_release_date: Option<String>,
    #[serde(default)]
    secondary_types: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct MbReleasePage {
    releases: Vec<MbRelease>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MbRelease {
    id: String,
    date: Option<String>,
    #[serde(default)]
    cover_art_archive: MbCoverArt,
    #[serde(default)]
    media: Vec<MbMedium>,
}

#[derive(Debug, Default, Deserialize)]
struct MbCoverArt {
    #[serde(default)]
    front: bool,
}

#[derive(Debug, Deserialize)]
struct MbMedium {
    position: i32,
    format: Option<String>,
    #[serde(default)]
    tracks: Vec<MbTrack>,
}

#[derive(Debug, Deserialize)]
struct MbTrack {
    position: i32,
    title: String,
    recording: Option<MbRecording>,
}

#[derive(Debug, Deserialize)]
struct MbRecording {
    id: String,
}

#[cfg(test)]
mod tests;
