use serde::{Deserialize, Serialize};

// These enums map 1:1 to Postgres enum types via SeaORM's `DeriveActiveEnum`.
// They live in riven-core — the shared crate — so they are the single source of
// truth for the app and every plugin. The `string_value`s must match the
// Postgres enum labels exactly.

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    async_graphql::Enum,
    sea_orm::EnumIter,
    sea_orm::DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "media_item_type")]
pub enum MediaItemType {
    #[sea_orm(string_value = "movie")]
    Movie,
    #[sea_orm(string_value = "show")]
    Show,
    #[sea_orm(string_value = "season")]
    Season,
    #[sea_orm(string_value = "episode")]
    Episode,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    async_graphql::Enum,
    sea_orm::EnumIter,
    sea_orm::DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "media_item_state")]
#[graphql(rename_items = "PascalCase")]
pub enum MediaItemState {
    #[sea_orm(string_value = "indexed")]
    Indexed,
    #[sea_orm(string_value = "unreleased")]
    Unreleased,
    #[sea_orm(string_value = "scraped")]
    Scraped,
    #[sea_orm(string_value = "ongoing")]
    Ongoing,
    #[sea_orm(string_value = "partially_completed")]
    PartiallyCompleted,
    #[sea_orm(string_value = "completed")]
    Completed,
    #[sea_orm(string_value = "paused")]
    Paused,
    #[sea_orm(string_value = "failed")]
    Failed,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    async_graphql::Enum,
    sea_orm::EnumIter,
    sea_orm::DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "show_status")]
pub enum ShowStatus {
    #[sea_orm(string_value = "continuing")]
    Continuing,
    #[sea_orm(string_value = "ended")]
    Ended,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    async_graphql::Enum,
    sea_orm::EnumIter,
    sea_orm::DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "content_rating")]
pub enum ContentRating {
    #[serde(rename = "G")]
    #[sea_orm(string_value = "G")]
    G,
    #[serde(rename = "PG")]
    #[sea_orm(string_value = "PG")]
    Pg,
    #[serde(rename = "PG-13")]
    #[sea_orm(string_value = "PG-13")]
    Pg13,
    #[serde(rename = "R")]
    #[sea_orm(string_value = "R")]
    R,
    #[serde(rename = "NC-17")]
    #[sea_orm(string_value = "NC-17")]
    Nc17,
    #[serde(rename = "TV-Y")]
    #[graphql(name = "TV_Y")]
    #[sea_orm(string_value = "TV-Y")]
    TvY,
    #[serde(rename = "TV-Y7")]
    #[graphql(name = "TV_Y7")]
    #[sea_orm(string_value = "TV-Y7")]
    TvY7,
    #[serde(rename = "TV-G")]
    #[graphql(name = "TV_G")]
    #[sea_orm(string_value = "TV-G")]
    TvG,
    #[serde(rename = "TV-PG")]
    #[graphql(name = "TV_PG")]
    #[sea_orm(string_value = "TV-PG")]
    TvPg,
    #[serde(rename = "TV-14")]
    #[graphql(name = "TV_14")]
    #[sea_orm(string_value = "TV-14")]
    Tv14,
    #[serde(rename = "TV-MA")]
    #[graphql(name = "TV_MA")]
    #[sea_orm(string_value = "TV-MA")]
    TvMa,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    async_graphql::Enum,
    sea_orm::EnumIter,
    sea_orm::DeriveActiveEnum,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "filesystem_entry_type"
)]
pub enum FileSystemEntryType {
    #[sea_orm(string_value = "media")]
    Media,
    #[sea_orm(string_value = "subtitle")]
    Subtitle,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    async_graphql::Enum,
    sea_orm::EnumIter,
    sea_orm::DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "item_request_type")]
pub enum ItemRequestType {
    #[sea_orm(string_value = "movie")]
    Movie,
    #[sea_orm(string_value = "show")]
    Show,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    async_graphql::Enum,
    sea_orm::EnumIter,
    sea_orm::DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "item_request_state")]
pub enum ItemRequestState {
    #[sea_orm(string_value = "requested")]
    Requested,
    /// An existing request had additional seasons appended after it was
    /// already completed/ongoing. Signals to the indexer that it should
    /// re-process this show; the recompute pipeline will then transition it
    /// back to its derived state (completed/ongoing/unreleased).
    #[sea_orm(string_value = "requested_additional_seasons")]
    RequestedAdditionalSeasons,
    #[sea_orm(string_value = "completed")]
    Completed,
    #[sea_orm(string_value = "failed")]
    Failed,
    #[sea_orm(string_value = "ongoing")]
    Ongoing,
    #[sea_orm(string_value = "unreleased")]
    Unreleased,
}
