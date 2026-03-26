use riven_core::config::vfs::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadType {
    /// Sequential read — served from the stream reader.
    Sequential,
    /// Non-sequential read (header, footer, seek) — single range fetch, cached.
    RangeFetch,
}

pub fn detect_read_type(
    start: u64,
    previous_position: Option<u64>,
    header_end: u64,
) -> ReadType {
    // Header reads are always range fetches (players re-read headers).
    if start < header_end {
        return ReadType::RangeFetch;
    }

    // First read or large jump = range fetch.
    let jump = previous_position
        .map(|prev| start.abs_diff(prev))
        .unwrap_or(u64::MAX);

    if jump > SCAN_TOLERANCE_BYTES {
        return ReadType::RangeFetch;
    }

    ReadType::Sequential
}
