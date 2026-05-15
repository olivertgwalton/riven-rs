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
//! startup with NNTP credentials, and consumed by both the ingest path
//! (`plugin-usenet`, which parses an NZB and stores its metadata in Redis) and
//! the serving path (riven-api's `/usenet/...` HTTP route).

pub mod nntp;
pub mod nzb;
pub mod state;
pub mod streamer;

pub(crate) mod cache;
pub(crate) mod crypto;
pub(crate) mod par2;
pub(crate) mod rar;
pub(crate) mod yenc;

pub use nntp::NntpConfig;
pub use nzb::{NzbDocument, NzbFile, NzbSegment, parse_nzb, parse_nzb_document};
pub use streamer::{NzbMeta, NzbMetaFile, NzbMetaSource, UsenetStreamer, active_streams};
