#[derive(Debug, thiserror::Error)]
pub enum StreamerError {
    #[error("nzb parse error: {0}")]
    Nzb(#[from] crate::nzb::NzbError),
    #[error("nntp error: {0}")]
    Nntp(#[from] crate::nntp::NntpError),
    #[error("yenc error: {0}")]
    Yenc(#[from] crate::yenc::YencError),
    #[error("database error: {0}")]
    Db(#[from] sea_orm::DbErr),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("metadata not found for info_hash {0}")]
    NotIngested(String),
    #[error("file index {0} out of range")]
    BadFileIndex(usize),
    #[error("range out of bounds")]
    BadRange,
    #[error("no media files in NZB")]
    NoMediaFile,
    #[error("article availability too low: {missing}/{checked} segments missing from provider")]
    IncompleteRelease { missing: usize, checked: usize },
    #[error("archive is encrypted but no password was provided")]
    MissingPassword,
    #[error("crypto error: {0}")]
    Crypto(#[from] crate::crypto::CryptoError),
    #[error("ingest queue full")]
    IngestQueueFull,
}
