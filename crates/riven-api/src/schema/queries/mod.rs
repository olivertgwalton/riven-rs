mod anilist;
mod external_ids;
mod media;
mod ratings;
mod settings;
mod tmdb;
mod trakt;
mod tvdb;

use async_graphql::MergedObject;

pub use anilist::CoreAnilistQuery;
pub use external_ids::CoreExternalIdsQuery;
pub use media::MediaQuery;
pub use ratings::CoreRatingsQuery;
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
    CoreExternalIdsQuery,
    CoreRatingsQuery,
    CoreTraktQuery,
    CoreTvdbQuery,
);
