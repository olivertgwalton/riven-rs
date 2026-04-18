mod media;
mod settings;
mod tmdb;

use async_graphql::MergedObject;

pub use media::MediaQuery;
pub use settings::SettingsQuery;
pub use tmdb::TmdbQuery;

#[derive(MergedObject, Default)]
pub struct CoreQuery(MediaQuery, SettingsQuery, TmdbQuery);
