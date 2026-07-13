//! PAR2 packet parsing — `FileDesc` packets carrying the real
//! (pre-obfuscation) filenames for the files an archive set protects, plus
//! `Main` (slice size) and `IFSC` (per-block MD5/CRC32) packets used to
//! verify that a downloaded RAR volume's actual payload matches what the
//! release's own PAR2 set says it should be.
//!
//! The spec [par2-2.0](https://parchive.sourceforge.net/docs/specifications/parity-volume-spec/article-spec.html)
//! defines a packet container with a fixed 64-byte header followed by a
//! per-packet-type body. `FileDesc` (`"PAR 2.0\0FileDesc"`) gives the file
//! ID, MD5, 16K-MD5, length and UTF-8 filename; the ingest path uses these
//! to map obfuscated NZB filenames back to their real names. `Main`
//! (`"PAR 2.0\0Main\0\0\0\0"`) gives the slice size every `IFSC` block is
//! measured in. `IFSC` (`"PAR 2.0\0IFSC\0\0\0\0"`) gives, per file ID, one
//! MD5-and-CRC32 pair per slice of that file — this is what lets ingest
//! catch a volume whose segments exist, STAT fine, and parse as a valid RAR
//! header, but whose actual payload doesn't match the archive's own
//! checksums, i.e. segments pointing at the wrong content entirely, as
//! opposed to missing.
//!
//! This module is a strict parser — anything malformed yields `Err`. The
//! caller is responsible for fetching the par2 file body (usually the smallest
//! `.par2` in an NZB, which — per the PAR2 spec — mirrors Main/FileDesc/IFSC
//! across every volume in the set; only `RecvSlic` recovery data scales with
//! volume size) and feeding it in.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const PACKET_MAGIC: &[u8; 8] = b"PAR2\0PKT";
const PACKET_TYPE_FILE_DESC: &[u8; 16] = b"PAR 2.0\0FileDesc";
const PACKET_TYPE_MAIN: &[u8; 16] = b"PAR 2.0\0Main\0\0\0\0";
const PACKET_TYPE_IFSC: &[u8; 16] = b"PAR 2.0\0IFSC\0\0\0\0";

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

/// One PAR2 slice's checksums from an `IFSC` packet. `crc32` is used for the
/// actual verification (already a workspace dependency via `crc32fast`,
/// consistent with a strong-enough integrity check, not a security check);
/// `md5` is kept since it's on the wire at no extra parse cost and useful for
/// a caller that wants belt-and-suspenders confirmation.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Par2Block {
    pub md5: [u8; 16],
    pub crc32: u32,
}

/// Slice size (bytes per verification/recovery block) from the `Main`
/// packet. Every `IFSC` block for every file in the set is measured in this
/// unit; a file's final block is conceptually zero-padded to it.
pub fn parse_slice_size(par2: &[u8]) -> Result<u64, Par2Error> {
    if par2.is_empty() {
        return Err(Par2Error::Empty);
    }
    let mut cursor = 0usize;
    while cursor + 64 <= par2.len() {
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
        if packet_type == PACKET_TYPE_MAIN {
            let body = &par2[cursor + 64..cursor + packet_length];
            if body.len() < 8 {
                return Err(Par2Error::Truncated(cursor));
            }
            let mut sz = [0u8; 8];
            sz.copy_from_slice(&body[0..8]);
            return Ok(u64::from_le_bytes(sz));
        }
        cursor += packet_length;
    }
    Err(Par2Error::NoPackets)
}

/// Walk a PAR2 blob and return every `IFSC` packet found, keyed by file ID.
/// Each packet lists one (MD5, CRC32) pair per PAR2 slice of that file, in
/// file order.
pub fn parse_ifsc_packets(par2: &[u8]) -> Result<HashMap<[u8; 16], Vec<Par2Block>>, Par2Error> {
    if par2.is_empty() {
        return Err(Par2Error::Empty);
    }
    let mut out: HashMap<[u8; 16], Vec<Par2Block>> = HashMap::new();
    let mut cursor = 0usize;
    while cursor + 64 <= par2.len() {
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
        if packet_type == PACKET_TYPE_IFSC {
            let body = &par2[cursor + 64..cursor + packet_length];
            if body.len() < 16 {
                return Err(Par2Error::Truncated(cursor));
            }
            let mut file_id = [0u8; 16];
            file_id.copy_from_slice(&body[0..16]);
            let rest = &body[16..];
            let mut blocks = Vec::with_capacity(rest.len() / 20);
            let mut i = 0usize;
            while i + 20 <= rest.len() {
                let mut md5 = [0u8; 16];
                md5.copy_from_slice(&rest[i..i + 16]);
                let mut crc_bytes = [0u8; 4];
                crc_bytes.copy_from_slice(&rest[i + 16..i + 20]);
                blocks.push(Par2Block {
                    md5,
                    crc32: u32::from_le_bytes(crc_bytes),
                });
                i += 20;
            }
            // IFSC packets are only mirrored, never split, across a set —
            // keep the first occurrence rather than the last.
            out.entry(file_id).or_insert(blocks);
        }
        cursor += packet_length;
    }
    if out.is_empty() {
        return Err(Par2Error::NoPackets);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_filedesc_packet(name: &str, length: u64) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&[0u8; 16]);
        body.extend_from_slice(&[1u8; 16]);
        body.extend_from_slice(&[2u8; 16]);
        body.extend_from_slice(&length.to_le_bytes());
        body.extend_from_slice(name.as_bytes());
        while body.len() % 4 != 0 {
            body.push(0);
        }
        let packet_length: u64 = 64 + body.len() as u64;
        let mut out = Vec::new();
        out.extend_from_slice(PACKET_MAGIC);
        out.extend_from_slice(&packet_length.to_le_bytes());
        out.extend_from_slice(&[0u8; 16]);
        out.extend_from_slice(&[0u8; 16]);
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
        let one = make_filedesc_packet("Movie.mkv", 1);
        let mut blob = one.clone();
        blob.extend(one);
        let descs = parse_file_descriptors(&blob).expect("parse ok");
        assert_eq!(descs.len(), 1);
    }

    #[test]
    fn rejects_empty_input() {
        assert!(matches!(parse_file_descriptors(&[]), Err(Par2Error::Empty)));
    }

    #[test]
    fn rejects_no_filedesc_packets() {
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

    fn make_main_packet(slice_size: u64, file_ids: &[[u8; 16]]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&slice_size.to_le_bytes());
        body.extend_from_slice(&(file_ids.len() as u32).to_le_bytes());
        for id in file_ids {
            body.extend_from_slice(id);
        }
        let packet_length: u64 = 64 + body.len() as u64;
        let mut out = Vec::new();
        out.extend_from_slice(PACKET_MAGIC);
        out.extend_from_slice(&packet_length.to_le_bytes());
        out.extend_from_slice(&[0u8; 16]);
        out.extend_from_slice(&[0u8; 16]);
        out.extend_from_slice(PACKET_TYPE_MAIN);
        out.extend_from_slice(&body);
        out
    }

    fn make_ifsc_packet(file_id: [u8; 16], blocks: &[Par2Block]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&file_id);
        for b in blocks {
            body.extend_from_slice(&b.md5);
            body.extend_from_slice(&b.crc32.to_le_bytes());
        }
        let packet_length: u64 = 64 + body.len() as u64;
        let mut out = Vec::new();
        out.extend_from_slice(PACKET_MAGIC);
        out.extend_from_slice(&packet_length.to_le_bytes());
        out.extend_from_slice(&[0u8; 16]);
        out.extend_from_slice(&[0u8; 16]);
        out.extend_from_slice(PACKET_TYPE_IFSC);
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn parses_main_slice_size() {
        let file_id = [7u8; 16];
        let bytes = make_main_packet(1_097_604, &[file_id]);
        assert_eq!(parse_slice_size(&bytes).expect("parse ok"), 1_097_604);
    }

    #[test]
    fn rejects_main_missing() {
        let bytes = make_filedesc_packet("Movie.mkv", 1);
        assert!(matches!(
            parse_slice_size(&bytes),
            Err(Par2Error::NoPackets)
        ));
    }

    #[test]
    fn parses_ifsc_blocks() {
        let file_id = [9u8; 16];
        let blocks = vec![
            Par2Block {
                md5: [1u8; 16],
                crc32: 0xdead_beef,
            },
            Par2Block {
                md5: [2u8; 16],
                crc32: 0x1234_5678,
            },
        ];
        let bytes = make_ifsc_packet(file_id, &blocks);
        let map = parse_ifsc_packets(&bytes).expect("parse ok");
        assert_eq!(map.get(&file_id).expect("file present"), &blocks);
    }

    #[test]
    fn ifsc_keeps_first_occurrence_on_duplicate() {
        let file_id = [9u8; 16];
        let first = vec![Par2Block {
            md5: [1u8; 16],
            crc32: 1,
        }];
        let second = vec![Par2Block {
            md5: [2u8; 16],
            crc32: 2,
        }];
        let mut blob = make_ifsc_packet(file_id, &first);
        blob.extend(make_ifsc_packet(file_id, &second));
        let map = parse_ifsc_packets(&blob).expect("parse ok");
        assert_eq!(map.get(&file_id).expect("file present"), &first);
    }

    #[test]
    fn rejects_no_ifsc_packets() {
        let bytes = make_filedesc_packet("Movie.mkv", 1);
        assert!(matches!(
            parse_ifsc_packets(&bytes),
            Err(Par2Error::NoPackets)
        ));
    }
}
