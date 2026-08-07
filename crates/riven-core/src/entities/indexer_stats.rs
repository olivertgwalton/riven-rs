//! Lifetime per-indexer query and grab counters. Written by the flusher that
//! drains [`crate::indexer_stats`], read by the dashboard.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "indexer_stats")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub indexer: String,
    pub search_queries: i64,
    pub caps_queries: i64,
    pub successful_grabs: i64,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
