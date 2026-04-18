mod media;
mod settings;
mod tmdb;

use async_graphql::MergedObject;

pub use media::MediaQuery;
pub use settings::CoreSettingsQuery;
pub use tmdb::CoreTmdbQuery;

#[derive(MergedObject, Default)]
pub struct CoreQuery(MediaQuery, CoreSettingsQuery, CoreTmdbQuery);
