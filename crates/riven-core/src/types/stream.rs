use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamLinkResponse {
    pub link: String,
    /// The store/provider that minted this link. May differ from the
    /// originally-pinned provider when the link-request handler fell back
    /// after the original store reported the torrent dead. The link-request
    /// consumer uses this to keep `filesystem_entries.provider` in sync so
    /// the next refresh prefers the live store first.
    #[serde(default)]
    pub provider: Option<String>,
}
