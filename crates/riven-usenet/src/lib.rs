//! Usenet streaming engine.
//!
//! Layered bottom-up:
//!   1. [`nntp`] — one client per TCP/TLS connection, and a per-provider pool
//!      that enforces that provider's configured connection limit.
//!   2. [`pool`] — one segment pool above every provider: decoded-segment
//!      cache, permanent-missing table, single-flight, provider failover.
//!   3. [`streamer`] — NZB ingest, the Postgres meta store, and byte-range
//!      reads over direct or RAR-contained sources.
//!
//! The public surface is `UsenetStreamer`, constructed once at process
//! startup and consumed by both the ingest path (`plugin-usenet`) and the
//! serving path (riven-vfs, in-process via `LocalByteSource`).

pub mod nntp;
pub mod nzb;
pub mod pool;
pub mod state;
pub mod streamer;

pub(crate) mod bufpool;
pub(crate) mod crypto;
pub(crate) mod par2;
pub(crate) mod rar;
pub(crate) mod yenc;

pub use nntp::{DEFAULT_DOWNLOAD_WORKERS, NntpConfig};
pub use nzb::{
    NzbDocument, NzbFile, NzbSegment, parse_nzb, parse_nzb_document, peek_release_title,
};
pub use pool::SegmentPool;
pub use streamer::{
    DEFAULT_AVAILABILITY_SAMPLE_PERCENT, NzbMeta, NzbMetaFile, NzbMetaSource, StreamerError,
    UNKNOWN_FILE_LABEL, UsenetStreamer, active_streams, set_degraded_playback,
};
