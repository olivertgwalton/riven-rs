use riven_core::config::vfs::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkRange {
    pub start: u64,
    pub end: u64,
}

impl ChunkRange {
    pub const fn len(self) -> usize {
        (self.end - self.start + 1) as usize
    }

    pub const fn is_empty(self) -> bool {
        self.end < self.start
    }
}

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

    pub fn header_chunk(&self) -> ChunkRange {
        ChunkRange {
            start: 0,
            end: self.header_end.saturating_sub(1),
        }
    }

    pub fn footer_chunk(&self) -> ChunkRange {
        ChunkRange {
            start: self.footer_start.min(self.file_size.saturating_sub(1)),
            end: self.file_size.saturating_sub(1),
        }
    }

    pub fn request_chunks(&self, start: u64, end: u64) -> Vec<ChunkRange> {
        let mut chunks = Vec::new();

        if self.file_size == 0 {
            return chunks;
        }

        if start < self.header_end {
            chunks.push(self.header_chunk());
        }

        if self.header_end < self.footer_start {
            let body_start = start.max(self.header_end);
            let body_end = end.min(self.footer_start.saturating_sub(1));

            if body_start <= body_end {
                let first_index = (body_start - self.header_end) / CHUNK_SIZE;
                let last_index = (body_end - self.header_end) / CHUNK_SIZE;

                for index in first_index..=last_index {
                    let chunk_start = self.header_end + index * CHUNK_SIZE;
                    let chunk_end = (chunk_start + CHUNK_SIZE - 1).min(self.footer_start - 1);
                    chunks.push(ChunkRange {
                        start: chunk_start,
                        end: chunk_end,
                    });
                }
            }
        }

        if end >= self.footer_start {
            let footer = self.footer_chunk();
            if chunks.last().copied() != Some(footer) {
                chunks.push(footer);
            }
        }

        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::{ChunkRange, FileLayout};
    use riven_core::config::vfs::HEADER_SIZE;

    #[test]
    fn chunk_range_reports_length_and_empty_state() {
        let range = ChunkRange { start: 10, end: 19 };

        assert_eq!(range.len(), 10);
        assert!(!range.is_empty());
        assert!(ChunkRange { start: 5, end: 4 }.is_empty());
    }

    #[test]
    fn file_layout_calculates_header_and_footer_chunks() {
        let file_size = 10_000_000;
        let layout = FileLayout::new(file_size);

        assert_eq!(layout.header_chunk().start, 0);
        assert_eq!(layout.header_chunk().end, HEADER_SIZE - 1);
        assert_eq!(layout.footer_chunk().end, layout.file_size - 1);
        assert_eq!(layout.footer_chunk().start, layout.footer_start);
    }

    #[test]
    fn request_chunks_includes_header_body_and_footer_without_duplicates() {
        let file_size = 10_000_000;
        let layout = FileLayout::new(file_size);

        let chunks = layout.request_chunks(0, file_size - 1);

        assert_eq!(chunks.first().copied(), Some(layout.header_chunk()));
        assert_eq!(chunks.last().copied(), Some(layout.footer_chunk()));
        assert!(chunks.len() >= 3);
    }

    #[test]
    fn request_chunks_returns_empty_for_empty_files() {
        let layout = FileLayout::new(0);

        assert!(layout.request_chunks(0, 0).is_empty());
    }
}
