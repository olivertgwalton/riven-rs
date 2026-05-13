use super::{
    METHOD_STORE, RAR5_SIGNATURE, RarEncryption, RarError, RarVolumeFileEntry, RarVolumeHeader,
};

// RAR5 format (from rarlab tech note):
//   - 8-byte signature.
//   - Stream of blocks. Each block has:
//       crc32           : 4 bytes
//       header_size     : vint (size of header AFTER this field)
//       header_type     : vint
//       header_flags    : vint
//       [extra_size]    : vint, present iff header_flags & 0x01
//       [data_size]     : vint, present iff header_flags & 0x02
//       <type-specific fields>
//       [extra area]    : `extra_size` bytes
//       [data area]     : `data_size` bytes (this is where the file payload lives)
//   - Block types: 1=Main, 2=File, 3=Service, 4=Encryption, 5=EndOfArchive.
//   - File header (type 2) type-specific fields:
//       file_flags      : vint    (bit 0 = directory, 1 = mtime, 2 = crc32,
//                                   3 = unp-size-unknown)
//       unpacked_size   : vint
//       attributes      : vint
//       [mtime          : 4 bytes if file_flags & 0x02]
//       [crc32          : 4 bytes if file_flags & 0x04]
//       compression_info: vint    (bits 0-5 version, 6 solid, 7-9 method, ...)
//       host_os         : vint
//       name_length     : vint
//       name            : name_length bytes
//   - Method 0 (bits 7-9 of compression_info == 0) is stored.
//
// We only need: filename, unpacked_size, stored?, header_size (to compute
// data offset), data_size (this volume's data contribution).

/// RAR5 file-header extra-area record types.
const RAR5_EXTRA_ENCRYPTION: u64 = 0x01;

pub(super) const RAR5_BLOCK_TYPE_FILE: u64 = 2;
const RAR5_BLOCK_TYPE_END: u64 = 5;

const RAR5_HEAD_FLAG_EXTRA: u64 = 0x0001;
pub(super) const RAR5_HEAD_FLAG_DATA: u64 = 0x0002;

const RAR5_FILE_FLAG_DIRECTORY: u64 = 0x0001;
const RAR5_FILE_FLAG_HAS_MTIME: u64 = 0x0002;
const RAR5_FILE_FLAG_HAS_CRC: u64 = 0x0004;

pub(super) fn parse_volume_header_v5(bytes: &[u8]) -> Result<RarVolumeHeader, RarError> {
    let mut pos: usize = RAR5_SIGNATURE.len();
    let mut out = RarVolumeHeader::default();

    while pos < bytes.len() {
        let block_start = pos;

        // crc32 (4 bytes), skipped.
        if pos + 4 > bytes.len() {
            break;
        }
        pos += 4;

        let header_size = match read_vint(bytes, &mut pos) {
            Some(v) => v as usize,
            None => break,
        };
        // header_size is the size of the rest of the header (after crc32 and
        // the header_size vint itself, but including header_type/flags/...).
        // checked_add guards against malformed vints with absurdly large
        // values; we treat such inputs as truncated and stop parsing.
        let Some(header_end) = pos.checked_add(header_size) else {
            break;
        };
        if header_end > bytes.len() {
            break;
        }

        let header_type = match read_vint(bytes, &mut pos) {
            Some(v) => v,
            None => break,
        };
        let head_flags = match read_vint(bytes, &mut pos) {
            Some(v) => v,
            None => break,
        };

        let extra_size = if head_flags & RAR5_HEAD_FLAG_EXTRA != 0 {
            match read_vint(bytes, &mut pos) {
                Some(v) => v as usize,
                None => break,
            }
        } else {
            0
        };
        let data_size = if head_flags & RAR5_HEAD_FLAG_DATA != 0 {
            match read_vint(bytes, &mut pos) {
                Some(v) => v,
                None => break,
            }
        } else {
            0
        };

        match header_type {
            RAR5_BLOCK_TYPE_END => break,
            RAR5_BLOCK_TYPE_FILE => {
                let file_flags = match read_vint(bytes, &mut pos) {
                    Some(v) => v,
                    None => break,
                };
                let unpacked_size = match read_vint(bytes, &mut pos) {
                    Some(v) => v,
                    None => break,
                };
                let _attributes = match read_vint(bytes, &mut pos) {
                    Some(v) => v,
                    None => break,
                };
                if file_flags & RAR5_FILE_FLAG_HAS_MTIME != 0 {
                    pos += 4;
                }
                if file_flags & RAR5_FILE_FLAG_HAS_CRC != 0 {
                    pos += 4;
                }
                let compression_info = match read_vint(bytes, &mut pos) {
                    Some(v) => v,
                    None => break,
                };
                let _host_os = match read_vint(bytes, &mut pos) {
                    Some(v) => v,
                    None => break,
                };
                let name_len = match read_vint(bytes, &mut pos) {
                    Some(v) => v as usize,
                    None => break,
                };
                let Some(name_end) = pos.checked_add(name_len) else {
                    break;
                };
                if name_end > bytes.len() {
                    break;
                }
                let name = String::from_utf8_lossy(&bytes[pos..name_end]).into_owned();
                // We don't advance `pos` past the name field — we jump
                // straight to `header_end + extra_size + data_size` below.

                // Compression method lives at bits 8-10 of compression_info
                // (mask 0x0700). 0 = store.
                let method_bits = (compression_info >> 8) & 0b111;
                let is_stored = method_bits == 0;

                // Directory flag is unreliable on split files — some scene
                // RAR5 archives set 0x01 on continuation file headers even
                // though they carry data. Treat a non-zero data area as
                // proof of an actual file.
                let is_directory = file_flags & RAR5_FILE_FLAG_DIRECTORY != 0 && data_size == 0;

                tracing::trace!(
                    name = %name,
                    file_flags,
                    compression_info,
                    method_bits,
                    is_stored,
                    is_directory,
                    data_size,
                    "RAR5 file header parsed"
                );

                let data_offset = header_end as u64;

                // The extra area lives between the type-specific fields and
                // the data area; we walk it for the encryption record (0x01).
                let extra_start = pos;
                let extra_end = header_end;
                let encryption = parse_rar5_file_extra(bytes, extra_start, extra_end);

                // RAR5 split flags live in extra-area records (e.g. 0x05
                // redirect); we don't parse them. The streamer's
                // slice-total-vs-unpacked-size sanity check is the real gate.
                if !is_directory {
                    out.files.push(RarVolumeFileEntry {
                        name,
                        data_offset,
                        packed_size: data_size,
                        unpacked_size,
                        method: if is_stored { METHOD_STORE } else { 0xFF },
                        split_before: false,
                        split_after: false,
                        encryption,
                    });
                }

                // `header_end` already covers the extra area (header_size
                // includes it per spec). The data area follows, sized by
                // `data_size`. Don't double-count.
                let _ = extra_size;
                pos = match header_end.checked_add(data_size as usize) {
                    Some(p) => p,
                    None => break,
                };
            }
            _ => {
                pos = match header_end.checked_add(data_size as usize) {
                    Some(p) => p,
                    None => break,
                };
            }
        }

        if pos <= block_start {
            break;
        }
    }

    Ok(out)
}

/// Walk the extra-area records of a RAR5 file header, returning the
/// encryption record if present. Each record is:
///     size  (vint) — size of `type + data`, NOT including this size vint
///     type  (vint)
///     data  (size - vint(type) bytes)
fn parse_rar5_file_extra(bytes: &[u8], start: usize, end: usize) -> Option<RarEncryption> {
    let mut pos = start;
    while pos < end {
        let record_size = match read_vint(bytes, &mut pos) {
            Some(v) => v as usize,
            None => return None,
        };
        let record_end = pos.checked_add(record_size)?;
        if record_end > end {
            return None;
        }
        let inner_start = pos;
        let record_type = match read_vint(bytes, &mut pos) {
            Some(v) => v,
            None => return None,
        };
        if record_type == RAR5_EXTRA_ENCRYPTION
            && let Some(enc) = parse_rar5_encryption_record(bytes, pos, record_end)
        {
            return Some(enc);
        }
        pos = inner_start.checked_add(record_size)?;
    }
    None
}

/// Parse an RAR5 file-encryption record body. Returns None if the bytes
/// don't conform (truncated, wrong version, etc.).
fn parse_rar5_encryption_record(bytes: &[u8], start: usize, end: usize) -> Option<RarEncryption> {
    let mut pos = start;
    let version = read_vint(bytes, &mut pos)?;
    let flags = read_vint(bytes, &mut pos)?;
    if pos >= end {
        return None;
    }
    let log2_count = bytes[pos];
    pos += 1;
    // 16-byte salt + 16-byte IV
    if pos.checked_add(32)? > end {
        return None;
    }
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&bytes[pos..pos + 16]);
    pos += 16;
    let mut iv = [0u8; 16];
    iv.copy_from_slice(&bytes[pos..pos + 16]);
    let has_check_value = flags & 0x01 != 0;
    Some(RarEncryption {
        version,
        log2_count,
        salt,
        iv,
        has_check_value,
    })
}

/// Decode an RAR5 variable-length integer. 7 data bits per byte, low bits
/// first, MSB is continuation. Returns the decoded value and advances `pos`.
fn read_vint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut shift: u32 = 0;
    let mut value: u64 = 0;
    while *pos < bytes.len() && shift < 64 {
        let b = bytes[*pos];
        *pos += 1;
        value |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
    }
    None
}
