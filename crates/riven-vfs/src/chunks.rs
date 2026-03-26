use riven_core::config::vfs::*;

/// Pre-calculated file layout — only header and footer boundaries.
/// Body reads go through the stream reader and don't need chunk tracking.
#[derive(Debug, Clone)]
pub struct FileLayout {
    pub file_size: u64,
    pub header_end: u64,
    pub footer_start: u64,
}

impl FileLayout {
    pub fn new(file_size: u64) -> Self {
        let footer_sz = footer_size(file_size);
        let footer_start = file_size.saturating_sub(footer_sz);
        let header_end = HEADER_SIZE.min(file_size);

        Self {
            file_size,
            header_end,
            footer_start,
        }
    }
}
