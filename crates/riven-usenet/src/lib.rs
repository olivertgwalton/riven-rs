//! Usenet streaming engine.
//!
//! This crate owns three concerns the rest of the codebase doesn't care about:
//!   1. Parsing NZB files (XML descriptions of which Usenet articles compose
//!      a given binary).
//!   2. Talking NNTP (with TLS + AUTHINFO) to a backbone provider, with a
//!      small connection pool.
//!   3. Decoding yEnc article bodies and stitching segments into a contiguous
//!      byte stream that supports approximate byte-range seeking.
//!
//! The public surface is `UsenetStreamer`. It is constructed once at process
//! startup with NNTP credentials and a SeaORM `DatabaseConnection`, and
//! consumed by both the
//! ingest path (`plugin-usenet`, which parses an NZB and persists its
//! segment map in Postgres) and the serving path (riven-vfs reads it
//! in-process via the `LocalByteSource` trait).

pub mod nntp;
pub mod nzb;
pub mod state;
pub mod streamer;

pub(crate) mod bufpool;
pub(crate) mod cache;
pub(crate) mod crypto;
pub(crate) mod par2;
pub(crate) mod rar;
pub(crate) mod yenc;

pub use nntp::{DEFAULT_DOWNLOAD_WORKERS, NntpConfig};
pub use nzb::{
    NzbDocument, NzbFile, NzbSegment, parse_nzb, parse_nzb_document, peek_release_title,
};
pub use streamer::{
    DEFAULT_AVAILABILITY_SAMPLE_PERCENT, NzbMeta, NzbMetaFile, NzbMetaSource, StreamerError,
    UNKNOWN_FILE_LABEL, UsenetStreamer, active_streams,
};
