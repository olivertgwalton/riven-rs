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

pub mod cache;
pub mod crypto;
pub mod nntp;
pub mod nzb;
pub mod par2;
pub mod rar;
pub mod state;
pub mod streamer;
pub mod yenc;

pub use nzb::{NzbDocument, NzbFile, NzbSegment, parse_nzb, parse_nzb_document};
pub use par2::{Par2FileDesc, looks_like_par2, parse_file_descriptors};
pub use streamer::{NntpConfig, NzbMeta, NzbMetaFile, UsenetStreamer, active_streams};
