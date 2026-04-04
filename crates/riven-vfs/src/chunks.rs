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
