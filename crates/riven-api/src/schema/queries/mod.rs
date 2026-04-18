mod anilist;
mod media;
mod settings;
mod tmdb;
mod trakt;
mod tvdb;

use async_graphql::MergedObject;

pub use anilist::CoreAnilistQuery;
pub use media::MediaQuery;
pub use settings::CoreSettingsQuery;
pub use tmdb::CoreTmdbQuery;
pub use trakt::CoreTraktQuery;
pub use tvdb::CoreTvdbQuery;

#[derive(MergedObject, Default)]
pub struct CoreQuery(
    MediaQuery,
    CoreSettingsQuery,
    CoreTmdbQuery,
    CoreAnilistQuery,
    CoreTraktQuery,
    CoreTvdbQuery,
);
