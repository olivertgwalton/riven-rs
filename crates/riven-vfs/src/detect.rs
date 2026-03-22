use riven_core::config::vfs::*;

/// Types of reads detected by the VFS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadType {
    /// Reading file header (first 256 KB).
    HeaderScan,
    /// Detecting footer (jump to near end).
    FooterScan,
    /// Sequential footer read after scan.
    FooterRead,
    /// Sequential body playback.
    BodyRead,
    /// All chunks already cached.
    CacheHit,
    /// Random access / scan read.
    GeneralScan,
}

/// Determine the read type based on position and history.
pub fn detect_read_type(
    start: u64,
    _size: u64,
    previous_position: Option<u64>,
    _file_size: u64,
    footer_start: u64,
    all_chunks_cached: bool,
    has_scanned_footer: bool,
) -> ReadType {
    if all_chunks_cached {
        return ReadType::CacheHit;
    }

    if start <= HEADER_SIZE {
        return ReadType::HeaderScan;
    }

    let jump = previous_position
        .map(|prev| {
            if start > prev {
                start - prev
            } else {
                prev - start
            }
        })
        .unwrap_or(u64::MAX);

    if jump > scan_tolerance_bytes() && start >= footer_start && !has_scanned_footer {
        return ReadType::FooterScan;
    }

    if jump > scan_tolerance_bytes() {
        return ReadType::GeneralScan;
    }

    if start >= footer_start {
        return ReadType::FooterRead;
    }

    ReadType::BodyRead
}
