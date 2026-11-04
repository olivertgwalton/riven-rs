use async_graphql::{Context, Object, Result, SimpleObject};
use chrono::NaiveDate;
use riven_core::http::HttpClient;
use riven_core::types::{MediaItemState, MediaItemType};
use riven_db::repo;

#[derive(SimpleObject)]
pub struct MusicArtist {
    /// MusicBrainz artist id.
    id: String,
    name: String,
    country: Option<String>,
    disambiguation: Option<String>,
    kind: Option<String>,
}

impl From<plugin_musicbrainz::ArtistSummary> for MusicArtist {
    fn from(artist: plugin_musicbrainz::ArtistSummary) -> Self {
        Self {
            id: artist.id,
            name: artist.name,
            country: artist.country,
            disambiguation: artist.disambiguation.filter(|text| !text.is_empty()),
            kind: artist.kind,
        }
    }
}

#[derive(SimpleObject)]
pub struct MusicAlbum {
    /// MusicBrainz release group id.
    id: String,
    title: String,
    release_date: Option<NaiveDate>,
    primary_type: Option<String>,
    cover_url: String,
    /// The album's state in the library; `None` when it was never requested.
    state: Option<MediaItemState>,
}

#[derive(SimpleObject)]
pub struct MusicArtistDetails {
    artist: MusicArtist,
    albums: Vec<MusicAlbum>,
    /// The artist's id in the library, once requested.
    library_id: Option<i64>,
    /// Whether the request follows the whole discography, new albums included.
    follows_discography: bool,
}

#[derive(Default)]
pub struct MusicQuery;

#[Object]
impl MusicQuery {
    /// Artists on MusicBrainz matching `query`, best match first.
    async fn search_music_artists(
        &self,
        ctx: &Context<'_>,
        query: String,
    ) -> Result<Vec<MusicArtist>> {
        let http = ctx.data::<HttpClient>()?;
        Ok(plugin_musicbrainz::search_artists(http, &query)
            .await?
            .into_iter()
            .map(MusicArtist::from)
            .collect())
    }

    /// An artist's albums, EPs and singles, with what of them the library
    /// already holds.
    async fn music_artist(&self, ctx: &Context<'_>, id: String) -> Result<MusicArtistDetails> {
        let http = ctx.data::<HttpClient>()?;
        let (artist, groups) = plugin_musicbrainz::artist_discography(http, &id).await?;

        let library_artist =
            repo::find_existing_media_item(MediaItemType::Artist, None, None, None, Some(&id))
                .await?;
        let (library_albums, follows_discography) = match &library_artist {
            Some(item) => {
                let albums = repo::get_requested_albums_for_artist(item.id).await?;
                let request = match item.item_request_id {
                    Some(request_id) => repo::get_item_request_by_id(request_id).await?,
                    None => None,
                };
                let follows = request
                    .as_ref()
                    .is_some_and(|request| repo::parse_request_albums(request).is_none());
                (albums, follows)
            }
            None => (Vec::new(), false),
        };

        let albums = groups
            .into_iter()
            .map(|group| MusicAlbum {
                cover_url: plugin_musicbrainz::release_group_cover_url(&group.id),
                state: library_albums
                    .iter()
                    .find(|album| album.musicbrainz_id.as_deref() == Some(group.id.as_str()))
                    .map(|album| album.state),
                id: group.id,
                title: group.title,
                release_date: group.first_release_date,
                primary_type: group.primary_type,
            })
            .collect();

        Ok(MusicArtistDetails {
            artist: artist.into(),
            albums,
            library_id: library_artist.map(|item| item.id),
            follows_discography,
        })
    }
}
