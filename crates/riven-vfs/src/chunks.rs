use riven_core::config::vfs::*;

/// A chunk of a file, identified by byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Chunk {
    pub start: u64,
    pub end: u64, // inclusive
}

impl Chunk {
    pub fn cache_key(&self, filename: &str) -> String {
        format!("{filename}:{}-{}", self.start, self.end)
    }
}

/// Pre-calculated chunk layout for a file.
#[derive(Debug, Clone)]
pub struct FileChunks {
    pub file_size: u64,
    pub header_chunk: Chunk,
    pub body_chunks: Vec<Chunk>,
    pub footer_chunk: Chunk,
    pub footer_start: u64,
}

impl FileChunks {
    /// Find all chunks that overlap the given byte range.
    pub fn chunks_for_range(&self, start: u64, end: u64) -> Vec<Chunk> {
        let mut result = Vec::new();

        if start <= self.header_chunk.end && end >= self.header_chunk.start {
            result.push(self.header_chunk);
        }

        for chunk in &self.body_chunks {
            if start <= chunk.end && end >= chunk.start {
                result.push(*chunk);
            }
        }

        if start <= self.footer_chunk.end && end >= self.footer_chunk.start {
            result.push(self.footer_chunk);
        }

        result
    }
}

/// Calculate the chunk layout for a file.
pub fn calculate_file_chunks(file_size: u64) -> FileChunks {
    let footer_sz = footer_size(file_size);
    let footer_start = file_size.saturating_sub(footer_sz);

    let header_end = HEADER_SIZE.min(file_size).saturating_sub(1);
    let header_chunk = Chunk {
        start: 0,
        end: header_end,
    };

    let mut body_chunks = Vec::new();
    let body_start = HEADER_SIZE;
    let body_end = footer_start;

    if body_start < body_end {
        let mut pos = body_start;
        while pos < body_end {
            let chunk_end = (pos + CHUNK_SIZE - 1).min(body_end - 1);
            body_chunks.push(Chunk {
                start: pos,
                end: chunk_end,
            });
            pos = chunk_end + 1;
        }
    }

    let footer_chunk = Chunk {
        start: footer_start.max(header_end + 1),
        end: file_size.saturating_sub(1),
    };

    FileChunks {
        file_size,
        header_chunk,
        body_chunks,
        footer_chunk,
        footer_start,
    }
}
