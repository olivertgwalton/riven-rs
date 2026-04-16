use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "media_item_type", rename_all = "lowercase")]
pub enum MediaItemType {
    Movie,
    Show,
    Season,
    Episode,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "media_item_state", rename_all = "snake_case")]
#[graphql(rename_items = "PascalCase")]
pub enum MediaItemState {
    Indexed,
    Unreleased,
    Scraped,
    Ongoing,
    PartiallyCompleted,
    Completed,
    Paused,
    Failed,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "show_status", rename_all = "lowercase")]
pub enum ShowStatus {
    Continuing,
    Ended,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "content_rating")]
pub enum ContentRating {
    #[sqlx(rename = "G")]
    G,
    #[sqlx(rename = "PG")]
    Pg,
    #[serde(rename = "PG-13")]
    #[sqlx(rename = "PG-13")]
    Pg13,
    #[sqlx(rename = "R")]
    R,
    #[serde(rename = "NC-17")]
    #[sqlx(rename = "NC-17")]
    Nc17,
    #[graphql(name = "TV_Y")]
    #[sqlx(rename = "TV-Y")]
    TvY,
    #[graphql(name = "TV_Y7")]
    #[sqlx(rename = "TV-Y7")]
    TvY7,
    #[graphql(name = "TV_G")]
    #[sqlx(rename = "TV-G")]
    TvG,
    #[graphql(name = "TV_PG")]
    #[sqlx(rename = "TV-PG")]
    TvPg,
    #[graphql(name = "TV_14")]
    #[sqlx(rename = "TV-14")]
    Tv14,
    #[graphql(name = "TV_MA")]
    #[sqlx(rename = "TV-MA")]
    TvMa,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "filesystem_entry_type", rename_all = "lowercase")]
pub enum FileSystemEntryType {
    Media,
    Subtitle,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "item_request_type", rename_all = "lowercase")]
pub enum ItemRequestType {
    Movie,
    Show,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, async_graphql::Enum,
)]
#[sqlx(type_name = "item_request_state", rename_all = "lowercase")]
pub enum ItemRequestState {
    Requested,
    Completed,
    Failed,
    Ongoing,
    Unreleased,
}
