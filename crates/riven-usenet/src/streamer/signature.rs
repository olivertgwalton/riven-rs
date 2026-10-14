//! Container signatures, for telling a video file from bytes that only carry
//! its name.
//!
//! A release can have every article on the provider and still not be a video:
//! a scrambled or obfuscated post riven cannot undo, a text listing posted
//! under an episode's name, or an archive whose payload reads back empty.
//! Article STATs pass all of those; the first bytes do not.

/// Bytes read from the start of a file to identify its container. Two MPEG-TS
/// packets at either stride fit, as does every other signature here.
pub const HEAD_PROBE_BYTES: u64 = 512;

const TS_SYNC: u8 = 0x47;
const TS_PACKET: usize = 188;
/// Blu-ray BDAV (.m2ts): a 4-byte timestamp before each 188-byte packet.
const BDAV_PACKET: usize = 192;

/// Whether `head`, a file's first bytes, opens a video container riven serves.
pub fn is_media_container(head: &[u8]) -> bool {
    let at = |offset: usize, magic: &[u8]| head.get(offset..offset + magic.len()) == Some(magic);
    at(0, &[0x1A, 0x45, 0xDF, 0xA3]) // Matroska / WebM (EBML)
        || [b"ftyp", b"moov", b"mdat", b"free", b"wide"].iter().any(|atom| at(4, *atom)) // MP4 / MOV
        || (at(0, b"RIFF") && at(8, b"AVI "))
        || at(0, &[0x30, 0x26, 0xB2, 0x75, 0x8E, 0x66, 0xCF, 0x11]) // ASF / WMV
        || at(0, &[0x00, 0x00, 0x01, 0xBA]) // MPEG program stream
        || is_transport_stream(head, 0, TS_PACKET)
        || is_transport_stream(head, 4, BDAV_PACKET)
}

/// A lone 0x47 is common in any data, so a transport stream needs the sync
/// byte at three consecutive packet starts.
fn is_transport_stream(head: &[u8], first: usize, stride: usize) -> bool {
    (0..3).all(|n| head.get(first + n * stride) == Some(&TS_SYNC))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn padded(prefix: &[u8]) -> Vec<u8> {
        let mut head = prefix.to_vec();
        head.resize(HEAD_PROBE_BYTES as usize, 0xAB);
        head
    }

    #[test]
    fn video_containers_are_media() {
        assert!(is_media_container(&padded(&[
            0x1A, 0x45, 0xDF, 0xA3, 0xA3, 0x42
        ])));
        assert!(is_media_container(&padded(b"\x00\x00\x00\x20ftypisom")));
        assert!(is_media_container(&padded(b"RIFF\x1e\x89\xe8\x0aAVI LIST")));
        let mut ts = vec![0u8; HEAD_PROBE_BYTES as usize];
        for n in 0..3 {
            ts[4 + n * BDAV_PACKET] = TS_SYNC;
        }
        assert!(is_media_container(&ts));
    }

    // The heads below are what broken releases in a real library served.
    #[test]
    fn text_noise_and_nothing_are_not_media() {
        assert!(!is_media_container(&padded(
            "\u{feff}Family.Guy.S03E01.The.Thin.White.Line.1080p.mkv".as_bytes()
        )));
        assert!(!is_media_container(&padded(&[
            0xB9, 0x6E, 0x3E, 0x0B, 0x9B, 0x1F, 0xF5, 0xA9, 0xAB, 0x86, 0xCE, 0x5A
        ])));
        assert!(!is_media_container(&[0u8; HEAD_PROBE_BYTES as usize]));
        assert!(!is_media_container(&[]));
    }

    #[test]
    fn one_stray_sync_byte_is_not_a_transport_stream() {
        let mut head = vec![0u8; HEAD_PROBE_BYTES as usize];
        head[0] = TS_SYNC;
        head[TS_PACKET] = TS_SYNC;
        assert!(!is_media_container(&head));
    }
}
