use riven_core::config::vfs::*;

use crate::cache::{RangeCache, cache_get};
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
    previous_read_end: Option<u64>,
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

    if previous_read_end.unwrap_or(0) < start.saturating_sub(SEQUENTIAL_READ_TOLERANCE_BYTES)
        && layout.footer_start <= start
    {
        return ReadType::FooterScan;
    }

    let is_general_scan = (previous_read_end
        .is_some_and(|previous_end| previous_end.abs_diff(start) > SCAN_TOLERANCE_BYTES)
        && start != layout.header_end
        && request_size <= BLOCK_SIZE as usize)
        || (start > layout.header_end && previous_read_end.is_none());

    if is_general_scan {
        return ReadType::GeneralScan;
    }

    if start < layout.footer_start {
        return ReadType::BodyRead;
    }

    ReadType::FooterRead
}

#[cfg(test)]
mod tests {
    use super::{ReadType, detect_read_type};
    use crate::cache::{RangeCache, cache_put};
    use crate::chunks::FileLayout;
    use bytes::Bytes;
    use lru::LruCache;
    use parking_lot::Mutex;
    use std::num::NonZeroUsize;

    fn empty_cache() -> RangeCache {
        Mutex::new(LruCache::new(NonZeroUsize::new(16).unwrap()))
    }

    #[test]
    fn detects_cache_hit_when_all_chunks_are_cached() {
        let layout = FileLayout::new(2_000_000);
        let chunks = layout.request_chunks(0, 1024);
        let cache = empty_cache();
        for chunk in &chunks {
            cache_put(
                &cache,
                (1, chunk.start, chunk.end),
                Bytes::from_static(b"x"),
            );
        }

        let read_type = detect_read_type(1, 0, 1024, 1025, None, &layout, &chunks, &cache);

        assert_eq!(read_type, ReadType::CacheHit);
    }

    #[test]
    fn detects_header_scan_for_small_initial_reads() {
        let layout = FileLayout::new(2_000_000);
        let chunks = layout.request_chunks(0, 512);
        let cache = empty_cache();

        let read_type = detect_read_type(1, 0, 512, 513, None, &layout, &chunks, &cache);

        assert_eq!(read_type, ReadType::HeaderScan);
    }

    #[test]
    fn detects_general_scan_for_non_sequential_body_reads() {
        let layout = FileLayout::new(10_000_000);
        let start = layout.header_end + riven_core::config::vfs::SCAN_TOLERANCE_BYTES + 1;
        let end = start + 1023;
        let chunks = layout.request_chunks(start, end);
        let cache = empty_cache();

        let read_type = detect_read_type(1, start, end, 1024, Some(0), &layout, &chunks, &cache);

        assert_eq!(read_type, ReadType::GeneralScan);
    }

    #[test]
    fn detects_footer_read_for_sequential_end_reads() {
        let layout = FileLayout::new(10_000_000);
        let start = layout.footer_start;
        let end = layout.file_size - 1;
        let chunks = layout.request_chunks(start, end);
        let cache = empty_cache();

        let read_type = detect_read_type(
            1,
            start,
            end,
            (end - start + 1) as usize,
            Some(start.saturating_sub(1)),
            &layout,
            &chunks,
            &cache,
        );

        assert_eq!(read_type, ReadType::FooterRead);
    }
}
