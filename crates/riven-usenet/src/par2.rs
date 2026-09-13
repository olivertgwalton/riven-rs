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
    /// File length in bytes.
    pub length: u64,
    /// UTF-8 filename. Length-prefix is implicit (`packet_length` minus the
    /// fixed FileDesc body bytes); trailing NUL padding is stripped here.
    pub filename: String,
}

/// Walk every well-formed packet in a PAR2 blob, handing each one's type,
/// body and offset to `visit`. Returns `Err` on the first malformed packet;
/// `visit` may return `Break` to stop early.
///
/// Every public parser here is a thin filter over this walk, so the container
/// framing — magic scan, length validation, truncation check — is decoded in
/// exactly one place.
fn for_each_packet<F>(par2: &[u8], mut visit: F) -> Result<(), Par2Error>
where
    F: FnMut(&[u8], &[u8], usize) -> Result<std::ops::ControlFlow<()>, Par2Error>,
{
    if par2.is_empty() {
        return Err(Par2Error::Empty);
    }
    let mut cursor = 0usize;
    while par2.len().saturating_sub(cursor) >= 64 {
        if &par2[cursor..cursor + 8] != PACKET_MAGIC {
            cursor += 1;
            continue;
        }
        // At least 64 bytes remain, so the 8-byte length field is present.
        let packet_length = par2[cursor + 8..]
            .first_chunk()
            .map_or(0, |len| u64::from_le_bytes(*len));
        if packet_length < 64 {
            return Err(Par2Error::BadLength(packet_length));
        }
        let Ok(packet_length) = usize::try_from(packet_length) else {
            return Err(Par2Error::Truncated(cursor));
        };
        let packet_end = cursor
            .checked_add(packet_length)
            .filter(|&end| end <= par2.len())
            .ok_or(Par2Error::Truncated(cursor))?;
        let packet_type = &par2[cursor + 48..cursor + 64];
        let body = &par2[cursor + 64..packet_end];
        if visit(packet_type, body, cursor)?.is_break() {
            return Ok(());
        }
        cursor = packet_end;
    }
    Ok(())
}

fn decode_file_desc(body: &[u8], at: usize) -> Result<Par2FileDesc, Par2Error> {
    // File ID, full MD5, 16K MD5, then the length; only the ID, length and
    // name are used.
    let (Some(file_id), Some(length)) = (
        body.first_chunk::<16>(),
        body.get(48..).and_then(|rest| rest.first_chunk::<8>()),
    ) else {
        return Err(Par2Error::Truncated(at));
    };
    let name_raw = &body[56..];
    let trimmed = match name_raw.iter().rposition(|&b| b != 0) {
        Some(p) => &name_raw[..=p],
        None => &name_raw[..0],
    };
    Ok(Par2FileDesc {
        file_id: *file_id,
        length: u64::from_le_bytes(*length),
        filename: String::from_utf8_lossy(trimmed).into_owned(),
    })
}

fn decode_slice_size(body: &[u8], at: usize) -> Result<u64, Par2Error> {
    body.first_chunk()
        .map(|size| u64::from_le_bytes(*size))
        .ok_or(Par2Error::Truncated(at))
}

fn decode_ifsc(body: &[u8], at: usize) -> Result<([u8; 16], Vec<Par2Block>), Par2Error> {
    let Some((file_id, rest)) = body.split_first_chunk::<16>() else {
        return Err(Par2Error::Truncated(at));
    };
    // Each entry is a 16-byte MD5 (unused) followed by a 4-byte CRC32.
    let (entries, _) = rest.as_chunks::<20>();
    let blocks = entries
        .iter()
        .map(|entry| Par2Block {
            crc32: u32::from_le_bytes([entry[16], entry[17], entry[18], entry[19]]),
        })
        .collect();
    Ok((*file_id, blocks))
}

/// Walk a PAR2 blob and return every `FileDesc` packet found. Duplicate
/// packets (PAR2 mirrors descriptors across the set for redundancy) are
/// deduped by `file_id`.
pub fn parse_file_descriptors(par2: &[u8]) -> Result<Vec<Par2FileDesc>, Par2Error> {
    let mut out: Vec<Par2FileDesc> = Vec::new();
    let mut seen: std::collections::HashSet<[u8; 16]> = std::collections::HashSet::new();
    for_each_packet(par2, |packet_type, body, at| {
        if packet_type == PACKET_TYPE_FILE_DESC {
            let desc = decode_file_desc(body, at)?;
            if seen.insert(desc.file_id) {
                out.push(desc);
            }
        }
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
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

/// One PAR2 slice's checksum from an `IFSC` packet. `crc32` is used for the
/// actual verification (already a workspace dependency via `crc32fast`,
/// consistent with a strong-enough integrity check, not a security check).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Par2Block {
    pub crc32: u32,
}

/// Everything the ingest path needs from one PAR2 blob.
///
/// `slice_size` is the bytes-per-verification-block figure from the `Main`
/// packet; every `IFSC` block for every file in the set is measured in this
/// unit, and a file's final block is conceptually zero-padded to it.
/// `ifsc` maps a file ID to one (MD5, CRC32) pair per slice, in file order.
pub struct Par2Set {
    pub slice_size: u64,
    pub file_descs: Vec<Par2FileDesc>,
    pub ifsc: HashMap<[u8; 16], Vec<Par2Block>>,
}

/// Collect the `Main`, `FileDesc` and `IFSC` packets in a **single** pass.
///
/// Callers that want all three (ingest does) should prefer this over the three
/// individual parsers, which each walk the whole blob independently — and that
/// walk is byte-at-a-time whenever the magic doesn't match, so three passes
/// over a large `.par2` is real work, not just three cheap scans.
///
/// Errors match calling the three parsers in sequence: a set missing any of the
/// three packet types yields [`Par2Error::NoPackets`].
pub fn parse_set(par2: &[u8]) -> Result<Par2Set, Par2Error> {
    let mut slice_size: Option<u64> = None;
    let mut file_descs: Vec<Par2FileDesc> = Vec::new();
    let mut seen: std::collections::HashSet<[u8; 16]> = std::collections::HashSet::new();
    let mut ifsc: HashMap<[u8; 16], Vec<Par2Block>> = HashMap::new();

    for_each_packet(par2, |packet_type, body, at| {
        match packet_type {
            t if t == PACKET_TYPE_MAIN => {
                // Keep the first Main, consistent with mirrored packet handling.
                if slice_size.is_none() {
                    slice_size = Some(decode_slice_size(body, at)?);
                }
            }
            t if t == PACKET_TYPE_FILE_DESC => {
                let desc = decode_file_desc(body, at)?;
                if seen.insert(desc.file_id) {
                    file_descs.push(desc);
                }
            }
            t if t == PACKET_TYPE_IFSC => {
                let (file_id, blocks) = decode_ifsc(body, at)?;
                ifsc.entry(file_id).or_insert(blocks);
            }
            _ => {}
        }
        Ok(std::ops::ControlFlow::Continue(()))
    })?;

    match slice_size {
        Some(slice_size) if !file_descs.is_empty() && !ifsc.is_empty() => Ok(Par2Set {
            slice_size,
            file_descs,
            ifsc,
        }),
        _ => Err(Par2Error::NoPackets),
    }
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

    #[test]
    fn rejects_packet_length_that_overflows_the_cursor() {
        let mut packet = vec![0xff];
        packet.extend_from_slice(PACKET_MAGIC);
        packet.extend_from_slice(&u64::MAX.to_le_bytes());
        packet.resize(65, 0);
        assert!(matches!(
            parse_file_descriptors(&packet),
            Err(Par2Error::Truncated(1))
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
            body.extend_from_slice(&[0u8; 16]);
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

    /// A blob carrying all three packet types, as a real .par2 index does.
    fn make_full_set(slice_size: u64, filename: &str, blocks: &[Par2Block]) -> Vec<u8> {
        // `make_filedesc_packet` writes an all-zero file_id, so the IFSC
        // packet must key off the same id for the two to correlate.
        let file_id = [0u8; 16];
        let mut blob = make_main_packet(slice_size, &[file_id]);
        blob.extend(make_filedesc_packet(filename, 12345));
        blob.extend(make_ifsc_packet(file_id, blocks));
        blob
    }

    fn sample_blocks() -> Vec<Par2Block> {
        vec![
            Par2Block { crc32: 0xdead_beef },
            Par2Block { crc32: 0x1234_5678 },
        ]
    }

    #[test]
    fn parse_set_collects_all_three_in_one_pass() {
        let blocks = sample_blocks();
        let blob = make_full_set(1_097_604, "Movie.2024.1080p.WEB.x264.mkv", &blocks);
        let set = parse_set(&blob).expect("parse ok");

        assert_eq!(set.slice_size, 1_097_604);
        assert_eq!(set.file_descs.len(), 1);
        assert_eq!(set.file_descs[0].filename, "Movie.2024.1080p.WEB.x264.mkv");
        assert_eq!(set.ifsc.get(&[0u8; 16]).expect("file present"), &blocks);
    }

    #[test]
    fn parse_set_rejects_main_missing() {
        let mut blob = make_filedesc_packet("Movie.mkv", 1);
        blob.extend(make_ifsc_packet([0u8; 16], &sample_blocks()));
        assert!(matches!(parse_set(&blob), Err(Par2Error::NoPackets)));
    }

    #[test]
    fn parse_set_rejects_ifsc_missing() {
        let mut blob = make_main_packet(1024, &[[0u8; 16]]);
        blob.extend(make_filedesc_packet("Movie.mkv", 1));
        assert!(matches!(parse_set(&blob), Err(Par2Error::NoPackets)));
    }

    #[test]
    fn parse_set_rejects_filedesc_missing() {
        let mut blob = make_main_packet(1024, &[[0u8; 16]]);
        blob.extend(make_ifsc_packet([0u8; 16], &sample_blocks()));
        assert!(matches!(parse_set(&blob), Err(Par2Error::NoPackets)));
    }

    #[test]
    fn parse_set_keeps_first_main_and_first_ifsc_on_duplicate() {
        let first = vec![Par2Block { crc32: 1 }];
        let second = vec![Par2Block { crc32: 2 }];
        let mut blob = make_full_set(1024, "Movie.mkv", &first);
        // A second, conflicting mirror of both packets later in the set.
        blob.extend(make_main_packet(4096, &[[0u8; 16]]));
        blob.extend(make_ifsc_packet([0u8; 16], &second));

        let set = parse_set(&blob).expect("parse ok");
        assert_eq!(set.slice_size, 1024);
        assert_eq!(set.ifsc.get(&[0u8; 16]).expect("file present"), &first);
    }

    #[test]
    fn parse_set_rejects_empty_input() {
        assert!(matches!(parse_set(&[]), Err(Par2Error::Empty)));
    }
}
