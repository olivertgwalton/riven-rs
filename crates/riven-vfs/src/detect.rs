use riven_core::config::vfs::*;

use crate::cache::{cache_get, RangeCache};
use crate::chunks::{ChunkRange, FileLayout};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadType {
    HeaderScan,
    FooterScan,
    FooterRead,
    GeneralScan,
    BodyRead,
    CacheHit,
}

fn request_fully_cached(cache: &RangeCache, ino: u64, chunks: &[ChunkRange]) -> bool {
    chunks
        .iter()
        .all(|chunk| cache_get(cache, (ino, chunk.start, chunk.end)).is_some())
}

pub fn detect_read_type(
    ino: u64,
    start: u64,
    end: u64,
    request_size: usize,
    previous_position: Option<u64>,
    layout: &FileLayout,
    chunks: &[ChunkRange],
    cache: &RangeCache,
) -> ReadType {
    if request_fully_cached(cache, ino, chunks) {
        return ReadType::CacheHit;
    }

    if start < end && end < layout.header_end {
        return ReadType::HeaderScan;
    }

    if previous_position.unwrap_or(0) < start.saturating_sub(SEQUENTIAL_READ_TOLERANCE_BYTES)
        && layout.footer_start <= start
    {
        return ReadType::FooterScan;
    }

    let is_general_scan = (previous_position
        .is_some_and(|previous| previous.abs_diff(start) > SCAN_TOLERANCE_BYTES)
        && start != layout.header_end
        && request_size < BLOCK_SIZE as usize)
        || (start > layout.header_end && previous_position.is_none());

    if is_general_scan {
        return ReadType::GeneralScan;
    }

    if start < layout.footer_start {
        return ReadType::BodyRead;
    }

    ReadType::FooterRead
}
