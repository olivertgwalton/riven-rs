//! PAR2 packet parsing — specifically the `FileDesc` packets that carry the
//! real (pre-obfuscation) filenames for the files an archive set protects.
//!
//! The spec [par2-2.0](https://parchive.sourceforge.net/docs/specifications/parity-volume-spec/article-spec.html)
//! defines a packet container with a fixed 64-byte header followed by a
//! per-packet-type body. We care only about `FileDesc` (`"PAR 2.0\0FileDesc"`),
//! which gives us the file ID + MD5 + 16K-MD5 + length + UTF-8 filename. nzbdav
//! uses this to map obfuscated downloaded files back to their real names via
//! a 16K-MD5 lookup; decypharr ignores par2 entirely.
//!
//! This module is a strict parser — anything malformed yields `Err`. The
//! caller is responsible for fetching the par2 file body (usually the smallest
//! `.par2` in an NZB) and feeding it in.

use serde::{Deserialize, Serialize};

const PACKET_MAGIC: &[u8; 8] = b"PAR2\0PKT";
const PACKET_TYPE_FILE_DESC: &[u8; 16] = b"PAR 2.0\0FileDesc";

#[derive(Debug, thiserror::Error)]
pub enum Par2Error {
    #[error("par2 data is empty")]
    Empty,
    #[error("no PAR2 packets found")]
    NoPackets,
    #[error("par2 packet truncated at offset {0}")]
    Truncated(usize),
    #[error("par2 packet length {0} below minimum header")]
    BadLength(u64),
}

/// A PAR2 `FileDesc` packet — describes one of the files the archive set
/// protects.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Par2FileDesc {
    /// 16-byte File ID (the MD5 hash the rest of the par2 set uses to refer
    /// to this file). Useful as a stable key.
    pub file_id: [u8; 16],
    /// MD5 of the full file contents.
    pub md5_full: [u8; 16],
    /// MD5 of the first 16 KiB of the file. Lets a caller identify which
    /// downloaded (often obfuscated) file maps to which FileDesc without
    /// reading the whole thing.
    pub md5_16k: [u8; 16],
    /// File length in bytes.
    pub length: u64,
    /// UTF-8 filename. Length-prefix is implicit (`packet_length` minus the
    /// fixed FileDesc body bytes); trailing NUL padding is stripped here.
    pub filename: String,
}

/// Walk a PAR2 blob and return every `FileDesc` packet found. Duplicate
/// packets (PAR2 mirrors descriptors across the set for redundancy) are
/// deduped by `file_id`.
pub fn parse_file_descriptors(par2: &[u8]) -> Result<Vec<Par2FileDesc>, Par2Error> {
    if par2.is_empty() {
        return Err(Par2Error::Empty);
    }
    let mut out: Vec<Par2FileDesc> = Vec::new();
    let mut seen: std::collections::HashSet<[u8; 16]> = std::collections::HashSet::new();
    let mut cursor = 0usize;
    while cursor + 64 <= par2.len() {
        // Magic search — par2 files often have leading garbage / yEnc
        // padding, so advance byte-by-byte until we find a valid header.
        if &par2[cursor..cursor + 8] != PACKET_MAGIC {
            cursor += 1;
            continue;
        }
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&par2[cursor + 8..cursor + 16]);
        let packet_length = u64::from_le_bytes(len_bytes);
        if packet_length < 64 {
            return Err(Par2Error::BadLength(packet_length));
        }
        let packet_length = packet_length as usize;
        if cursor + packet_length > par2.len() {
            return Err(Par2Error::Truncated(cursor));
        }
        let packet_type = &par2[cursor + 48..cursor + 64];
        if packet_type == PACKET_TYPE_FILE_DESC {
            let body = &par2[cursor + 64..cursor + packet_length];
            // FileDesc body: file_id(16) + md5_full(16) + md5_16k(16) +
            // length(8 LE) + filename(rest, NUL-padded to 4-byte alignment).
            if body.len() < 56 {
                return Err(Par2Error::Truncated(cursor));
            }
            let mut file_id = [0u8; 16];
            file_id.copy_from_slice(&body[0..16]);
            let mut md5_full = [0u8; 16];
            md5_full.copy_from_slice(&body[16..32]);
            let mut md5_16k = [0u8; 16];
            md5_16k.copy_from_slice(&body[32..48]);
            let mut len_bytes = [0u8; 8];
            len_bytes.copy_from_slice(&body[48..56]);
            let length = u64::from_le_bytes(len_bytes);
            // Filename: strip trailing NUL padding and decode as UTF-8 lossily
            // (some par2 producers historically emit Windows-1252; lossy is
            // good enough for our display/match use).
            let name_raw = &body[56..];
            let trimmed = match name_raw.iter().rposition(|&b| b != 0) {
                Some(p) => &name_raw[..=p],
                None => &name_raw[..0],
            };
            let filename = String::from_utf8_lossy(trimmed).into_owned();
            if seen.insert(file_id) {
                out.push(Par2FileDesc {
                    file_id,
                    md5_full,
                    md5_16k,
                    length,
                    filename,
                });
            }
        }
        cursor += packet_length;
    }
    if out.is_empty() {
        return Err(Par2Error::NoPackets);
    }
    Ok(out)
}

/// Returns true if the NZB filename looks like a PAR2 file. Recognises both
/// the index (`*.par2`) and the per-volume slices (`*.volNN+NN.par2`).
pub fn looks_like_par2(filename: &str) -> bool {
    let lower = filename.to_ascii_lowercase();
    lower.ends_with(".par2")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_filedesc_packet(name: &str, length: u64) -> Vec<u8> {
        // Body: 16 + 16 + 16 + 8 + name (padded to 4-byte alignment).
        let mut body = Vec::new();
        body.extend_from_slice(&[0u8; 16]); // file_id
        body.extend_from_slice(&[1u8; 16]); // md5_full
        body.extend_from_slice(&[2u8; 16]); // md5_16k
        body.extend_from_slice(&length.to_le_bytes());
        body.extend_from_slice(name.as_bytes());
        while body.len() % 4 != 0 {
            body.push(0);
        }
        let packet_length: u64 = 64 + body.len() as u64;
        let mut out = Vec::new();
        out.extend_from_slice(PACKET_MAGIC);
        out.extend_from_slice(&packet_length.to_le_bytes());
        out.extend_from_slice(&[0u8; 16]); // packet md5 (unused by parser)
        out.extend_from_slice(&[0u8; 16]); // recovery set id
        out.extend_from_slice(PACKET_TYPE_FILE_DESC);
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn parses_filedesc() {
        let bytes = make_filedesc_packet("Movie.2024.1080p.WEB.x264.mkv", 12345678);
        let descs = parse_file_descriptors(&bytes).expect("parse ok");
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0].filename, "Movie.2024.1080p.WEB.x264.mkv");
        assert_eq!(descs[0].length, 12345678);
    }

    #[test]
    fn dedupes_repeated_filedesc() {
        // PAR2 mirrors FileDesc packets across the set; we want one entry per
        // unique file_id.
        let one = make_filedesc_packet("Movie.mkv", 1);
        let mut blob = one.clone();
        blob.extend(one);
        let descs = parse_file_descriptors(&blob).expect("parse ok");
        assert_eq!(descs.len(), 1);
    }

    #[test]
    fn rejects_empty_input() {
        assert!(matches!(
            parse_file_descriptors(&[]),
            Err(Par2Error::Empty)
        ));
    }

    #[test]
    fn rejects_no_filedesc_packets() {
        // Non-FileDesc packet; loop should yield NoPackets.
        let mut packet = Vec::new();
        packet.extend_from_slice(PACKET_MAGIC);
        let packet_length: u64 = 64 + 4;
        packet.extend_from_slice(&packet_length.to_le_bytes());
        packet.extend_from_slice(&[0u8; 16]);
        packet.extend_from_slice(&[0u8; 16]);
        packet.extend_from_slice(b"PAR 2.0\0Main\0\0\0\0");
        packet.extend_from_slice(&[0u8; 4]);
        assert!(matches!(
            parse_file_descriptors(&packet),
            Err(Par2Error::NoPackets)
        ));
    }
}
