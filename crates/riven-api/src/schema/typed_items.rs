use async_graphql::*;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;
use riven_db::repo;
use sqlx::PgPool;

// ── Movie ──────────────────────────────────────────────────────────────────

pub struct Movie {
    pub item: MediaItem,
}

#[Object]
impl Movie {
    #[graphql(flatten)]
    async fn base(&self) -> &MediaItem {
        &self.item
    }

    /// Always 1 — a movie has exactly one expected media file.
    async fn expected_file_count(&self) -> i64 {
        1
    }
}

// ── Show ───────────────────────────────────────────────────────────────────

pub struct Show {
    pub item: MediaItem,
}

#[Object]
impl Show {
    #[graphql(flatten)]
    async fn base(&self) -> &MediaItem {
        &self.item
    }

    /// Seasons for this show. Excludes season 0 (specials) by default.
    async fn seasons(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = false)] include_specials: bool,
    ) -> Result<Vec<Season>> {
        let pool = ctx.data::<PgPool>()?;
        let items = if include_specials {
            repo::list_seasons(pool, self.item.id).await?
        } else {
            repo::list_seasons_excluding_specials(pool, self.item.id).await?
        };
        Ok(items.into_iter().map(|item| Season { item }).collect())
    }

    /// Total expected downloadable episode files.
    /// For continuing shows the currently-airing season is excluded.
    async fn expected_file_count(&self, ctx: &Context<'_>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::count_expected_files_for_show(pool, self.item.id).await?)
    }
}

// ── Season ─────────────────────────────────────────────────────────────────

pub struct Season {
    pub item: MediaItem,
}

#[Object]
impl Season {
    #[graphql(flatten)]
    async fn base(&self) -> &MediaItem {
        &self.item
    }

    /// The parent show for this season.
    async fn show(&self, ctx: &Context<'_>) -> Result<Show> {
        let pool = ctx.data::<PgPool>()?;
        let parent_id = self
            .item
            .parent_id
            .ok_or_else(|| Error::new("Season has no parent show"))?;
        let item = repo::get_media_item(pool, parent_id)
            .await?
            .ok_or_else(|| Error::new("Parent show not found"))?;
        Ok(Show { item })
    }

    /// All episodes in this season.
    async fn episodes(&self, ctx: &Context<'_>) -> Result<Vec<Episode>> {
        let pool = ctx.data::<PgPool>()?;
        let items = repo::list_episodes(pool, self.item.id).await?;
        Ok(items.into_iter().map(|item| Episode { item }).collect())
    }

    /// Total number of episodes in this season.
    async fn total_episodes(&self, ctx: &Context<'_>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::count_episodes_in_season(pool, self.item.id).await?)
    }

    /// Expected number of episode files to download (equals total episodes).
    async fn expected_file_count(&self, ctx: &Context<'_>) -> Result<i64> {
        let pool = ctx.data::<PgPool>()?;
        Ok(repo::count_episodes_in_season(pool, self.item.id).await?)
    }
}

// ── Episode ────────────────────────────────────────────────────────────────

pub struct Episode {
    pub item: MediaItem,
}

#[Object]
impl Episode {
    #[graphql(flatten)]
    async fn base(&self) -> &MediaItem {
        &self.item
    }

    /// The parent season for this episode.
    async fn season(&self, ctx: &Context<'_>) -> Result<Season> {
        let pool = ctx.data::<PgPool>()?;
        let parent_id = self
            .item
            .parent_id
            .ok_or_else(|| Error::new("Episode has no parent season"))?;
        let item = repo::get_media_item(pool, parent_id)
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

// ── Union ──────────────────────────────────────────────────────────────────

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
