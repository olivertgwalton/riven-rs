//! Minimal RAR archive parser sufficient to expose stored (uncompressed)
//! single-file multi-volume archives as streamable virtual files.
//!
//! Supports both RAR4 and RAR5 formats; the entry point sniffs the
//! signature and dispatches.
//!
//! Intentionally narrow:
//!   - Stored compression only (RAR4 METHOD == 0x30, RAR5 method bits == 0).
//!     Compressed RAR would require unpacking, which we don't do.
//!   - No password / encryption handling.
//!
//! The parser consumes the leading bytes of each RAR volume — typically the
//! first NNTP segment is sufficient — and returns the locations of contained
//! file data within each volume so the streamer can later fetch the right
//! byte ranges from NNTP and skip RAR headers.
//!
//! References:
//!   - https://www.rarlab.com/technote.htm

mod v4;
mod v5;

#[cfg(test)]
mod tests;

use v4::parse_volume_header_v4;
use v5::parse_volume_header_v5;

#[derive(Debug, thiserror::Error)]
pub enum RarError {
    #[error("not a RAR archive")]
    NotRar,
    #[error("truncated header: needed {needed} bytes, had {available}")]
    Truncated { needed: usize, available: usize },
    #[error("invalid block: {0}")]
    InvalidBlock(&'static str),
}

pub(crate) const METHOD_STORE: u8 = 0x30;

pub(crate) const RAR4_SIGNATURE: [u8; 7] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00];
pub(crate) const RAR5_SIGNATURE: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

/// A single file-header occurrence within one RAR volume. A contained file
/// that spans N volumes appears as N entries (one per volume).
#[derive(Debug, Clone)]
pub struct RarVolumeFileEntry {
    /// Filename as decoded from the FILE_HEAD. For multi-volume entries
    /// this is the same name across all volumes.
    pub name: String,
    /// Offset (in the volume's byte stream) where this file header's data
    /// payload starts. The streamer reads `[data_offset .. data_offset +
    /// packed_size]` from the volume to get this slice of the contained
    /// file.
    pub data_offset: u64,
    /// Bytes of compressed (or stored) data this header contributes from
    /// the current volume.
    pub packed_size: u64,
    /// Total uncompressed file size, repeated in every FILE_HEAD of a
    /// multi-volume file.
    pub unpacked_size: u64,
    pub method: u8,
    /// Encryption record from the file header's extra area, if the file
    /// is encrypted. RAR5 only; RAR4 encryption isn't currently parsed.
    pub encryption: Option<RarEncryption>,
}

/// RAR5 file-level encryption parameters. Captures everything needed to
/// derive the AES-256 key and decrypt the data area.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RarEncryption {
    /// Encryption algorithm version. RAR5 currently defines 0 = AES-256.
    pub version: u64,
    /// PBKDF2 iterations are `1 << log2_count`. Typical: 15 (i.e. 2^15 = 32768).
    pub log2_count: u8,
    /// 16-byte salt for PBKDF2-HMAC-SHA256.
    pub salt: [u8; 16],
    /// 16-byte IV for AES-256-CBC. The data area is encrypted as a single
    /// CBC stream starting with this IV.
    pub iv: [u8; 16],
    /// Whether the archive carries a password check value (we don't use it).
    pub has_check_value: bool,
}

impl RarVolumeFileEntry {
    pub fn is_stored(&self) -> bool {
        self.method == METHOD_STORE
    }
}

/// Parser output for one volume's leading bytes.
#[derive(Debug, Default)]
pub struct RarVolumeHeader {
    pub is_volume: bool,
    pub files: Vec<RarVolumeFileEntry>,
}

/// Parse the leading bytes of a RAR volume, sniffing RAR4 vs RAR5 from the
/// signature and dispatching to the appropriate parser.
pub fn parse_volume_header(bytes: &[u8]) -> Result<RarVolumeHeader, RarError> {
    if bytes.len() >= RAR5_SIGNATURE.len() && bytes.starts_with(&RAR5_SIGNATURE) {
        return parse_volume_header_v5(bytes);
    }
    if bytes.len() >= RAR4_SIGNATURE.len() && bytes.starts_with(&RAR4_SIGNATURE) {
        return parse_volume_header_v4(bytes);
    }
    Err(RarError::NotRar)
}
