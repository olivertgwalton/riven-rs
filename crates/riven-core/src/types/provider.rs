use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInfo {
    pub name: String,
    pub store: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DebridUserInfo {
    pub store: String,
    pub email: Option<String>,
    pub username: Option<String>,
    pub subscription_status: Option<String>,
    pub premium_until: Option<String>,
    pub cooldown_until: Option<String>,
    pub total_downloaded_bytes: Option<i64>,
    pub points: Option<i64>,
}
