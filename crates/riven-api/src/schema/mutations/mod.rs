mod item_request;
mod library;
mod media_entry;
mod movie;
mod settings;
mod show;
mod streams;

use async_graphql::{Enum, MergedObject};

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
    movie::MovieMutations,
    show::ShowMutations,
    media_entry::MediaEntryMutations,
    settings::SettingsMutations,
    library::LibraryMutations,
    streams::StreamsMutations,
);
