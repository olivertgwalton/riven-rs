mod anilist;
mod external_ids;
mod indexer_stats;
pub(crate) mod logs;
mod media;
mod ratings;
pub(crate) mod settings;
mod tmdb;
mod trakt;
mod tvdb;
mod usenet_health;

use async_graphql::MergedObject;

pub use anilist::CoreAnilistQuery;
pub use external_ids::CoreExternalIdsQuery;
pub use indexer_stats::IndexerStatsQuery;
pub use logs::LogsQuery;
pub use media::MediaQuery;
pub use ratings::CoreRatingsQuery;
pub use settings::CoreSettingsQuery;
pub use tmdb::CoreTmdbQuery;
pub use trakt::CoreTraktQuery;
pub use tvdb::CoreTvdbQuery;
pub use usenet_health::UsenetHealthQuery;

use crate::schema::auth::ViewerQuery;

#[derive(MergedObject, Default)]
pub struct CoreQuery(
    ViewerQuery,
    MediaQuery,
    CoreSettingsQuery,
    CoreTmdbQuery,
    CoreAnilistQuery,
    CoreExternalIdsQuery,
    CoreRatingsQuery,
    CoreTraktQuery,
    CoreTvdbQuery,
    LogsQuery,
    UsenetHealthQuery,
    IndexerStatsQuery,
);
