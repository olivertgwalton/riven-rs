use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct AioStreamsResponse {
    pub success: bool,
    pub detail: Option<String>,
    pub error: Option<AioStreamsError>,
    pub data: Option<AioStreamsData>,
}

impl AioStreamsResponse {
    pub(crate) fn error_message(&self) -> Option<&str> {
        self.error
            .as_ref()
            .and_then(|error| error.message.as_deref())
            .or(self.detail.as_deref())
    }
}

#[derive(Deserialize)]
pub(crate) struct AioStreamsError {
    pub message: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct AioStreamsData {
    #[serde(default)]
    pub results: Vec<AioStreamsStream>,
}

#[derive(Deserialize)]
pub(crate) struct AioStreamsStream {
    #[serde(rename = "infoHash")]
    pub info_hash: Option<String>,
    #[serde(rename = "folderName")]
    pub folder_name: Option<String>,
    pub filename: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
}
