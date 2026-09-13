mod item_request;
mod library;
mod settings;
mod streams;
mod usenet_health;

use async_graphql::{Enum, MergedObject};
use plugin_seerr::SeerrMutations;

/// Shared status enum returned by all structured mutation responses.
#[derive(Enum, Copy, Clone, PartialEq, Eq)]
pub enum MutationStatusText {
    Ok,
    Created,
    BadRequest,
    NotFound,
    Conflict,
    InternalServerError,
}

#[derive(MergedObject, Default)]
pub struct MutationRoot(
    item_request::ItemRequestMutations,
    settings::SettingsMutations,
    library::LibraryMutations,
    streams::StreamsMutations,
    usenet_health::UsenetHealthMutations,
    SeerrMutations,
);
