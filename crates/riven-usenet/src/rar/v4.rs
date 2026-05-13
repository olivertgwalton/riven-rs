use std::io::{Cursor, Read};

use super::{
    RAR4_SIGNATURE, RarError, RarVolumeFileEntry, RarVolumeHeader,
};

pub(super) const MAIN_HEAD: u8 = 0x73;
pub(super) const FILE_HEAD: u8 = 0x74;
const ENDARC_HEAD: u8 = 0x7B;

pub(super) const FLAG_LONG_BLOCK: u16 = 0x8000;

const FILE_FLAG_SPLIT_BEFORE: u16 = 0x0001;
const FILE_FLAG_SPLIT_AFTER: u16 = 0x0002;
const FILE_FLAG_HIGH_SIZE: u16 = 0x0100;
const FILE_FLAG_UNICODE: u16 = 0x0200;
const FILE_FLAG_SALT: u16 = 0x0400;
const FILE_FLAG_EXT_TIME: u16 = 0x1000;

pub(super) fn parse_volume_header_v4(bytes: &[u8]) -> Result<RarVolumeHeader, RarError> {
    let mut cursor = Cursor::new(bytes);
    cursor.set_position(RAR4_SIGNATURE.len() as u64);

    let mut out = RarVolumeHeader::default();

    loop {
        let pos_block_start = cursor.position();
        let Some(common) = read_common_header(&mut cursor, bytes.len() as u64) else {
            break;
        };

        match common.head_type {
            MAIN_HEAD => {
                out.is_volume = (common.head_flags & 0x0001) != 0;
                cursor.set_position(pos_block_start + common.head_size as u64);
            }
            FILE_HEAD => {
                let entry =
                    match read_file_head(&mut cursor, &common, pos_block_start, bytes.len() as u64)
                    {
                        Ok(Some(e)) => e,
                        Ok(None) => break,
                        Err(_) => break,
                    };

                // Skip over this file's data payload to land at the next block,
                // if it fits. If the data extends past our buffer, we're at
                // the natural end of header-only parsing.
                let data_end = entry
                    .data_offset
                    .saturating_add(entry.packed_size);
                out.files.push(entry);

                if data_end > bytes.len() as u64 {
                    break;
                }
                cursor.set_position(data_end);
            }
            ENDARC_HEAD => {
                break;
            }
            _ => {
                let mut skip = common.head_size as u64;
                if (common.head_flags & FLAG_LONG_BLOCK) != 0 {
                    skip = skip.saturating_add(common.add_size as u64);
                }
                let next = pos_block_start.saturating_add(skip);
                if next <= cursor.position() || next > bytes.len() as u64 {
                    break;
                }
                cursor.set_position(next);
            }
        }
    }

    Ok(out)
}

#[derive(Debug)]
struct CommonHeader {
    head_type: u8,
    head_flags: u16,
    head_size: u16,
    /// Only meaningful when FLAG_LONG_BLOCK is set.
    add_size: u32,
}

fn read_common_header(cur: &mut Cursor<&[u8]>, total_len: u64) -> Option<CommonHeader> {
    if cur.position() + 7 > total_len {
        return None;
    }
    let _crc = read_u16(cur)?;
    let head_type = read_u8(cur)?;
    let head_flags = read_u16(cur)?;
    let head_size = read_u16(cur)?;
    let add_size = if (head_flags & FLAG_LONG_BLOCK) != 0 {
        read_u32(cur)?
    } else {
        0
    };
    Some(CommonHeader {
        head_type,
        head_flags,
        head_size,
        add_size,
    })
}

fn read_file_head(
    cur: &mut Cursor<&[u8]>,
    common: &CommonHeader,
    block_start: u64,
    total_len: u64,
) -> Result<Option<RarVolumeFileEntry>, RarError> {
    if (common.head_flags & FLAG_LONG_BLOCK) == 0 {
        return Err(RarError::InvalidBlock("FILE_HEAD without LONG_BLOCK"));
    }
    let pack_lo = common.add_size;
    let unp_lo = read_u32(cur).ok_or(truncated(4, total_len, cur.position()))?;
    let _host_os = read_u8(cur).ok_or(truncated(1, total_len, cur.position()))?;
    let _file_crc = read_u32(cur).ok_or(truncated(4, total_len, cur.position()))?;
    let _ftime = read_u32(cur).ok_or(truncated(4, total_len, cur.position()))?;
    let _unp_ver = read_u8(cur).ok_or(truncated(1, total_len, cur.position()))?;
    let method = read_u8(cur).ok_or(truncated(1, total_len, cur.position()))?;
    let name_size = read_u16(cur).ok_or(truncated(2, total_len, cur.position()))?;
    let _attr = read_u32(cur).ok_or(truncated(4, total_len, cur.position()))?;

    let (pack_size, unpacked_size) = if (common.head_flags & FILE_FLAG_HIGH_SIZE) != 0 {
        let high_pack = read_u32(cur).ok_or(truncated(4, total_len, cur.position()))?;
        let high_unp = read_u32(cur).ok_or(truncated(4, total_len, cur.position()))?;
        (
            ((high_pack as u64) << 32) | (pack_lo as u64),
            ((high_unp as u64) << 32) | (unp_lo as u64),
        )
    } else {
        (pack_lo as u64, unp_lo as u64)
    };

    let mut name_bytes = vec![0u8; name_size as usize];
    if cur.read_exact(&mut name_bytes).is_err() {
        return Ok(None);
    }
    let name = decode_filename(&name_bytes, common.head_flags & FILE_FLAG_UNICODE != 0);

    if (common.head_flags & FILE_FLAG_SALT) != 0 {
        cur.set_position(cur.position() + 8);
    }
    if (common.head_flags & FILE_FLAG_EXT_TIME) != 0 {
        // Variable-length ext-time block; HEAD_SIZE points past it to the
        // data section so no skip needed.
    }

    let data_offset = block_start + common.head_size as u64;
    let split_before = (common.head_flags & FILE_FLAG_SPLIT_BEFORE) != 0;
    let split_after = (common.head_flags & FILE_FLAG_SPLIT_AFTER) != 0;

    Ok(Some(RarVolumeFileEntry {
        name,
        data_offset,
        packed_size: pack_size,
        unpacked_size,
        method,
        split_before,
        split_after,
        // RAR4 encryption isn't parsed; modern encrypted releases use RAR5.
        encryption: None,
    }))
}

fn decode_filename(bytes: &[u8], _unicode_flag: bool) -> String {
    // For unicode-flagged names the layout is `ascii\0unicode-encoded` where
    // the second half uses a quirky compressed encoding. Most modern releases
    // use 7-bit ASCII filenames, so taking everything up to the first NUL
    // works for the common case. Falls back to lossy UTF-8 otherwise.
    let cut = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..cut]).into_owned()
}

fn read_u8(cur: &mut Cursor<&[u8]>) -> Option<u8> {
    let mut buf = [0u8; 1];
    cur.read_exact(&mut buf).ok()?;
    Some(buf[0])
}
fn read_u16(cur: &mut Cursor<&[u8]>) -> Option<u16> {
    let mut buf = [0u8; 2];
    cur.read_exact(&mut buf).ok()?;
    Some(u16::from_le_bytes(buf))
}
fn read_u32(cur: &mut Cursor<&[u8]>) -> Option<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf).ok()?;
    Some(u32::from_le_bytes(buf))
}

fn truncated(needed: usize, total: u64, pos: u64) -> RarError {
    let available = total.saturating_sub(pos) as usize;
    RarError::Truncated { needed, available }
}
