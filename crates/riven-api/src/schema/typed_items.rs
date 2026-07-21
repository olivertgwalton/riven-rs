use async_graphql::*;
use riven_core::types::MediaItemType;
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;

async fn load_streams(media_item_id: i64, info_hashes: Option<Vec<String>>) -> Result<Vec<Stream>> {
    let mut streams = repo::get_streams_for_item(media_item_id).await?;
    if let Some(info_hashes) = info_hashes {
        streams.retain(|stream| {
            info_hashes
                .iter()
                .any(|info_hash| stream.info_hash.eq_ignore_ascii_case(info_hash))
        });
    }
    Ok(streams)
}

/// Shared body for the `streams` resolver on every typed media item.
async fn load_streams_for(
    item: &MediaItem,
    _ctx: &Context<'_>,
    info_hashes: Option<Vec<String>>,
) -> Result<Vec<Stream>> {
    load_streams(item.id, info_hashes).await
}

pub struct Movie {
    pub item: MediaItem,
}

#[Object]
impl Movie {
    #[graphql(flatten)]
    async fn base(&self) -> &MediaItem {
        &self.item
    }

    async fn streams(
        &self,
        ctx: &Context<'_>,
        info_hashes: Option<Vec<String>>,
    ) -> Result<Vec<Stream>> {
        load_streams_for(&self.item, ctx, info_hashes).await
    }

    /// Always 1 — a movie has exactly one expected media file.
    async fn expected_file_count(&self) -> i64 {
        1
    }
}

pub struct Show {
    pub item: MediaItem,
}

#[Object]
impl Show {
    #[graphql(flatten)]
    async fn base(&self) -> &MediaItem {
        &self.item
    }

    async fn streams(
        &self,
        ctx: &Context<'_>,
        info_hashes: Option<Vec<String>>,
    ) -> Result<Vec<Stream>> {
        load_streams_for(&self.item, ctx, info_hashes).await
    }

    /// Seasons for this show. Excludes season 0 (specials) by default.
    async fn seasons(
        &self,
        _ctx: &Context<'_>,
        #[graphql(default = false)] include_specials: bool,
    ) -> Result<Vec<Season>> {
        let items = if include_specials {
            repo::list_seasons(self.item.id).await?
        } else {
            repo::list_seasons_excluding_specials(self.item.id).await?
        };
        Ok(items.into_iter().map(|item| Season { item }).collect())
    }

    /// Total expected downloadable episode files.
    /// For continuing shows the currently-airing season is excluded.
    async fn expected_file_count(&self, _ctx: &Context<'_>) -> Result<i64> {
        Ok(repo::count_expected_files_for_show(self.item.id).await?)
    }
}

pub struct Season {
    pub item: MediaItem,
}

#[Object]
impl Season {
    #[graphql(flatten)]
    async fn base(&self) -> &MediaItem {
        &self.item
    }

    async fn streams(
        &self,
        ctx: &Context<'_>,
        info_hashes: Option<Vec<String>>,
    ) -> Result<Vec<Stream>> {
        load_streams_for(&self.item, ctx, info_hashes).await
    }

    /// The parent show for this season.
    async fn show(&self, _ctx: &Context<'_>) -> Result<Show> {
        let parent_id = self
            .item
            .parent_id
            .ok_or_else(|| Error::new("Season has no parent show"))?;
        let item = repo::get_media_item(parent_id)
            .await?
            .ok_or_else(|| Error::new("Parent show not found"))?;
        Ok(Show { item })
    }

    /// All episodes in this season.
    async fn episodes(&self, _ctx: &Context<'_>) -> Result<Vec<Episode>> {
        let items = repo::list_episodes(self.item.id).await?;
        Ok(items.into_iter().map(|item| Episode { item }).collect())
    }

    /// Total number of episodes in this season.
    async fn total_episodes(&self, _ctx: &Context<'_>) -> Result<i64> {
        Ok(repo::count_episodes_in_season(self.item.id).await?)
    }

    /// Expected number of episode files to download (equals total episodes).
    async fn expected_file_count(&self, _ctx: &Context<'_>) -> Result<i64> {
        Ok(repo::count_episodes_in_season(self.item.id).await?)
    }
}

pub struct Episode {
    pub item: MediaItem,
}

#[Object]
impl Episode {
    #[graphql(flatten)]
    async fn base(&self) -> &MediaItem {
        &self.item
    }

    async fn streams(
        &self,
        ctx: &Context<'_>,
        info_hashes: Option<Vec<String>>,
    ) -> Result<Vec<Stream>> {
        load_streams_for(&self.item, ctx, info_hashes).await
    }

    /// The parent season for this episode.
    async fn season(&self, _ctx: &Context<'_>) -> Result<Season> {
        let parent_id = self
            .item
            .parent_id
            .ok_or_else(|| Error::new("Episode has no parent season"))?;
        let item = repo::get_media_item(parent_id)
            .await?
            .ok_or_else(|| Error::new("Parent season not found"))?;
        Ok(Season { item })
    }

    /// Lookup keys: `["abs:{absoluteNumber}", "{seasonNumber}:{episodeNumber}"]`.
    async fn lookup_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        if let Some(abs) = self.item.absolute_number {
            keys.push(format!("abs:{abs}"));
        }
        if let (Some(season), Some(episode)) = (self.item.season_number, self.item.episode_number) {
            keys.push(format!("{season}:{episode}"));
        }
        keys
    }

    /// Always 1 — an episode has exactly one expected media file.
    async fn expected_file_count(&self) -> i64 {
        1
    }
}

/// Discriminated union of all concrete media item types.
#[derive(Union)]
pub enum MediaItemUnion {
    Movie(Movie),
    Show(Show),
    Season(Season),
    Episode(Episode),
}

impl From<MediaItem> for MediaItemUnion {
    fn from(item: MediaItem) -> Self {
        match item.item_type {
            MediaItemType::Movie => MediaItemUnion::Movie(Movie { item }),
            MediaItemType::Show => MediaItemUnion::Show(Show { item }),
            MediaItemType::Season => MediaItemUnion::Season(Season { item }),
            MediaItemType::Episode => MediaItemUnion::Episode(Episode { item }),
        }
    }
}
