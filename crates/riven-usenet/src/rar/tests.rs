use super::*;
use super::v4::{FILE_HEAD, FLAG_LONG_BLOCK, MAIN_HEAD};
use super::v5::{RAR5_HEAD_FLAG_DATA};

/// Build a minimal RAR4 byte stream:
///   MARK + MAIN + FILE("contained.mkv", method=store, pack=N, unp=N) + N data bytes
fn make_single_volume(name: &str, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&RAR4_SIGNATURE);

    let main = [
        0x00, 0x00, // CRC
        MAIN_HEAD,
        0x00, 0x00, // FLAGS (no volume)
        0x0D, 0x00, // HEAD_SIZE = 13
        0x00, 0x00, // HighPosAv
        0x00, 0x00, 0x00, 0x00, // PosAv
    ];
    out.extend_from_slice(&main);

    let name_bytes = name.as_bytes();
    let head_size = 7 + 4 + 4 + 1 + 4 + 4 + 1 + 1 + 2 + 4 + name_bytes.len();
    let pack_size = data.len() as u32;

    let mut fh = Vec::new();
    fh.extend_from_slice(&[0x00, 0x00]); // CRC
    fh.push(FILE_HEAD);
    let flags: u16 = FLAG_LONG_BLOCK; // no split, no unicode, no salt
    fh.extend_from_slice(&flags.to_le_bytes());
    fh.extend_from_slice(&(head_size as u16).to_le_bytes());
    fh.extend_from_slice(&pack_size.to_le_bytes()); // ADD_SIZE
    fh.extend_from_slice(&pack_size.to_le_bytes()); // UNP_SIZE_LO
    fh.push(0x00); // HOST_OS
    fh.extend_from_slice(&[0u8; 4]); // FILE_CRC
    fh.extend_from_slice(&[0u8; 4]); // FTIME
    fh.push(0x14); // UNP_VER (2.0)
    fh.push(METHOD_STORE);
    fh.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    fh.extend_from_slice(&[0u8; 4]); // ATTR
    fh.extend_from_slice(name_bytes);
    out.extend_from_slice(&fh);
    out.extend_from_slice(data);

    out
}

#[test]
fn rejects_non_rar() {
    let bytes = b"hello world this is not a rar archive";
    let err = parse_volume_header(bytes).unwrap_err();
    assert!(matches!(err, RarError::NotRar));
}

#[test]
fn parses_single_volume_stored() {
    let payload = b"hello-world-stored-payload";
    let bytes = make_single_volume("contained.mkv", payload);
    let header = parse_volume_header(&bytes).unwrap();
    assert!(!header.is_volume);
    assert_eq!(header.files.len(), 1);
    let f = &header.files[0];
    assert_eq!(f.name, "contained.mkv");
    assert!(f.is_stored());
    assert_eq!(f.packed_size, payload.len() as u64);
    assert_eq!(f.unpacked_size, payload.len() as u64);
    assert!(!f.split_before);
    assert!(!f.split_after);

    let observed = &bytes[f.data_offset as usize..f.data_offset as usize + payload.len()];
    assert_eq!(observed, payload);
}

/// Encode a value as an RAR5 vint (LSB first, MSB = continuation).
fn vint(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return out;
        }
        out.push(b | 0x80);
    }
}

/// Hand-built RAR5 single-volume stored archive containing one file.
fn make_rar5_single(name: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&RAR5_SIGNATURE);

    let mut main_body = Vec::new();
    main_body.extend_from_slice(&vint(1)); // header_type = main
    main_body.extend_from_slice(&vint(0)); // header_flags
    main_body.extend_from_slice(&vint(0)); // archive_flags
    out.extend_from_slice(&[0u8; 4]); // CRC32, ignored
    out.extend_from_slice(&vint(main_body.len() as u64));
    out.extend_from_slice(&main_body);

    let name_bytes = name.as_bytes();
    let mut file_body = Vec::new();
    file_body.extend_from_slice(&vint(2)); // header_type = file
    file_body.extend_from_slice(&vint(RAR5_HEAD_FLAG_DATA)); // head_flags = has data
    file_body.extend_from_slice(&vint(payload.len() as u64)); // data_size
    file_body.extend_from_slice(&vint(0)); // file_flags
    file_body.extend_from_slice(&vint(payload.len() as u64)); // unpacked_size
    file_body.extend_from_slice(&vint(0)); // attributes
    file_body.extend_from_slice(&vint(0)); // compression_info (method 0 = store)
    file_body.extend_from_slice(&vint(0)); // host_os
    file_body.extend_from_slice(&vint(name_bytes.len() as u64)); // name_length
    file_body.extend_from_slice(name_bytes);
    out.extend_from_slice(&[0u8; 4]); // crc32
    out.extend_from_slice(&vint(file_body.len() as u64));
    out.extend_from_slice(&file_body);

    out.extend_from_slice(payload);
    out
}

#[test]
fn parses_rar5_single_volume_stored() {
    let payload = b"rar5-payload-bytes-here";
    let bytes = make_rar5_single("inside.mkv", payload);
    let header = parse_volume_header(&bytes).unwrap();
    assert_eq!(header.files.len(), 1);
    let f = &header.files[0];
    assert_eq!(f.name, "inside.mkv");
    assert!(f.is_stored());
    assert_eq!(f.packed_size, payload.len() as u64);
    assert_eq!(f.unpacked_size, payload.len() as u64);

    let observed = &bytes[f.data_offset as usize..f.data_offset as usize + payload.len()];
    assert_eq!(observed, payload);
}
