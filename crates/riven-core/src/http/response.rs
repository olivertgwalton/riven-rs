use bytes::Bytes;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

#[derive(Clone, Debug)]
pub struct HttpResponseData {
    status: StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Bytes,
}

impl HttpResponseData {
    pub(super) async fn from_response(response: reqwest::Response) -> anyhow::Result<Self> {
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.bytes().await?;
        Ok(Self {
            status,
            headers,
            body,
        })
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        &self.headers
    }

    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    pub fn text(&self) -> anyhow::Result<String> {
        Ok(String::from_utf8(self.body.to_vec())?)
    }

    pub fn json<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(serde_json::from_slice(&self.body)?)
    }

    pub fn error_for_status_ref(&self) -> anyhow::Result<()> {
        if self.is_success() {
            return Ok(());
        }
        let preview_len = self.body.len().min(200);
        let preview = String::from_utf8_lossy(&self.body[..preview_len]);
        anyhow::bail!(
            "http request failed with status {}: {}",
            self.status,
            preview
        )
    }
}
