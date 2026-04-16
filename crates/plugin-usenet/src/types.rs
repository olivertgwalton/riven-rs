use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

const PAYLOAD_PREFIX: &str = "riven-usenet:";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsenetPayload {
    pub(crate) nzb_url: Option<String>,
    pub(crate) nzb_urls: Vec<String>,
    pub(crate) servers: Vec<String>,
    pub(crate) filename: String,
    pub(crate) video_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) files: Vec<UsenetFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsenetFile {
    pub(crate) name: String,
    pub(crate) size: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PreflightResult {
    pub(crate) files: Vec<UsenetFile>,
}

pub(crate) struct ParsedPreflight {
    pub(crate) result: PreflightResult,
    pub(crate) message_ids: Vec<String>,
}

impl UsenetPayload {
    pub(crate) fn stable_id(&self) -> anyhow::Result<String> {
        let json = serde_json::to_vec(self)?;
        let digest = Sha1::digest(json);
        Ok(hex::encode(digest))
    }

    pub(crate) fn to_magnet(&self) -> anyhow::Result<String> {
        let json = serde_json::to_vec(self)?;
        Ok(format!("{PAYLOAD_PREFIX}{}", URL_SAFE_NO_PAD.encode(json)))
    }

    pub(crate) fn from_magnet(value: &str) -> Option<Self> {
        let encoded = value.strip_prefix(PAYLOAD_PREFIX)?;
        let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}
