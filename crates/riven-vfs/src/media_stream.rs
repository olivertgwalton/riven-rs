use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};

use crate::cache::{RangeCache, cache_get, cache_put};
use crate::chunks::{ChunkRange, FileLayout};
use crate::detect::{ReadType, detect_read_type};
use crate::prefetch::Prefetch;
use crate::stream::fetch_range;

pub enum ReadOutcome {
    Data(Bytes),
    Error(i32),
}

pub struct MediaStream {
    ino: u64,
    file_size: u64,
    layout: FileLayout,
    last_read_end: Option<u64>,
    prefetch: Option<Prefetch>,
    stream_buffer: SequentialBuffer,
}

struct ReadContext<'a> {
    stream_url: &'a str,
    cache: &'a RangeCache,
    client: &'a reqwest::Client,
    runtime: &'a tokio::runtime::Handle,
    debug_logging: bool,
}

struct BufferedChunk {
    range: ChunkRange,
    data: Bytes,
}

struct SequentialBuffer {
    chunks: VecDeque<BufferedChunk>,
    capacity_bytes: usize,
    total_bytes: usize,
}

impl SequentialBuffer {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            capacity_bytes: capacity_bytes.max(1),
            total_bytes: 0,
        }
    }

    fn clear(&mut self) {
        self.chunks.clear();
        self.total_bytes = 0;
    }

    fn get_chunk(&self, range: ChunkRange) -> Option<Bytes> {
        self.chunks
            .iter()
            .find(|chunk| chunk.range == range)
            .map(|chunk| chunk.data.clone())
    }

    fn read_range(&self, start: u64, end: u64) -> Option<Bytes> {
        let mut cursor = start;
        let mut segments = Vec::new();

        for chunk in &self.chunks {
            if cursor < chunk.range.start {
                break;
            }
            if cursor > chunk.range.end {
                continue;
            }

            let slice_start = (cursor - chunk.range.start) as usize;
            let slice_end = ((end.min(chunk.range.end) - chunk.range.start) + 1) as usize;
            segments.push(chunk.data.slice(slice_start..slice_end));
            cursor = chunk.range.start + slice_end as u64;

            if cursor > end {
                break;
            }
        }

        if cursor <= end {
            return None;
        }

        if segments.len() == 1 {
            return segments.pop();
        }

        let total_len = (end - start + 1) as usize;
        let mut full = BytesMut::with_capacity(total_len);
        for segment in segments {
            full.extend_from_slice(&segment);
        }
        Some(full.freeze())
    }

    fn insert(&mut self, range: ChunkRange, data: Bytes) {
        if self.get_chunk(range).is_some() {
            return;
        }

        if let Some(back) = self.chunks.back()
            && range.start > back.range.end.saturating_add(1)
        {
            self.clear();
        }

        self.total_bytes += data.len();
        self.chunks.push_back(BufferedChunk { range, data });

        while self.total_bytes > self.capacity_bytes {
            if let Some(evicted) = self.chunks.pop_front() {
                self.total_bytes = self.total_bytes.saturating_sub(evicted.data.len());
            } else {
                break;
            }
        }
    }
}

impl MediaStream {
    pub fn new(ino: u64, file_size: u64) -> Self {
        Self {
            ino,
            file_size,
            layout: FileLayout::new(file_size),
            last_read_end: None,
            prefetch: None,
            stream_buffer: SequentialBuffer::new(
                riven_core::config::vfs::STREAM_BUFFER_SIZE as usize,
            ),
        }
    }

    pub fn read(
        &mut self,
        start: u64,
        end: u64,
        stream_url: &str,
        cache: &RangeCache,
        client: &reqwest::Client,
        runtime: &tokio::runtime::Handle,
        debug_logging: bool,
    ) -> ReadOutcome {
        let chunks = self.layout.request_chunks(start, end);
        let ctx = ReadContext {
            stream_url,
            cache,
            client,
            runtime,
            debug_logging,
        };
        let read_type = detect_read_type(
            self.ino,
            start,
            end,
            (end - start + 1) as usize,
            self.last_read_end,
            &self.layout,
            &chunks,
            ctx.cache,
        );

        if ctx.debug_logging {
            tracing::debug!(
                ino = self.ino,
                offset = start,
                size = end - start + 1,
                read_type = ?read_type,
                chunks = chunks.len(),
                "media stream read"
            );
        }

        let outcome = match read_type {
            ReadType::HeaderScan => self.read_scan_range(start, end, chunks[0], true, &ctx),
            ReadType::FooterScan | ReadType::FooterRead => {
                let chunk = *chunks.last().unwrap_or(&chunks[0]);
                self.read_scan_range(start, end, chunk, true, &ctx)
            }
            ReadType::GeneralScan => {
                self.read_scan_range(start, end, ChunkRange { start, end }, false, &ctx)
            }
            ReadType::BodyRead => self.read_body(&chunks, start, end, &ctx),
            ReadType::CacheHit => self.read_cached_chunks(start, end, &chunks, ctx.cache),
        };

        if matches!(outcome, ReadOutcome::Data(_)) {
            self.last_read_end = Some(end);
        }

        outcome
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    fn fetch_and_cache_range(
        &self,
        chunk: ChunkRange,
        stream_url: &str,
        cache: &RangeCache,
        client: &reqwest::Client,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Bytes, ()> {
        let key = (self.ino, chunk.start, chunk.end);
        cache.get_or_fetch(key, || {
            match runtime.block_on(fetch_range(client, stream_url, chunk.start, chunk.end)) {
                Ok(data) => Ok(data),
                Err(e) => {
                    tracing::error!(ino = self.ino, error = %e, "range fetch failed");
                    Err(())
                }
            }
        })
    }

    fn read_cached_chunks(
        &self,
        start: u64,
        end: u64,
        chunks: &[ChunkRange],
        cache: &RangeCache,
    ) -> ReadOutcome {
        if let Some(buffered) = self.stream_buffer.read_range(start, end) {
            return ReadOutcome::Data(buffered);
        }

        let total_len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
        let mut full = BytesMut::with_capacity(total_len);

        for chunk in chunks {
            let Some(data) = cache_get(cache, (self.ino, chunk.start, chunk.end)) else {
                return ReadOutcome::Error(libc::EIO);
            };
            full.extend_from_slice(&data);
        }

        let offset = (start - chunks[0].start) as usize;
        let slice_len = (end - start + 1) as usize;
        let full = full.freeze();
        if offset + slice_len > full.len() {
            tracing::error!(
                ino = self.ino,
                start,
                end,
                cached_len = full.len(),
                "cached chunk set shorter than requested range"
            );
            return ReadOutcome::Error(libc::EIO);
        }

        ReadOutcome::Data(full.slice(offset..offset + slice_len))
    }

    fn read_scan_range(
        &mut self,
        start: u64,
        end: u64,
        chunk: ChunkRange,
        should_cache: bool,
        ctx: &ReadContext<'_>,
    ) -> ReadOutcome {
        self.prefetch = None;
        self.stream_buffer.clear();

        let full = if should_cache {
            match self.fetch_and_cache_range(
                chunk,
                ctx.stream_url,
                ctx.cache,
                ctx.client,
                ctx.runtime,
            ) {
                Ok(data) => data,
                Err(()) => return ReadOutcome::Error(libc::EIO),
            }
        } else {
            match ctx
                .runtime
                .block_on(fetch_range(ctx.client, ctx.stream_url, start, end))
            {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!(ino = self.ino, error = %e, "range fetch failed");
                    return ReadOutcome::Error(libc::EIO);
                }
            }
        };

        if !should_cache {
            return ReadOutcome::Data(full);
        }

        let slice_start = (start - chunk.start) as usize;
        let slice_end = slice_start + (end - start + 1) as usize;
        if slice_end > full.len() {
            tracing::error!(
                ino = self.ino,
                start,
                end,
                chunk_start = chunk.start,
                chunk_end = chunk.end,
                fetched_len = full.len(),
                "scan range shorter than requested"
            );
            return ReadOutcome::Error(libc::EIO);
        }

        ReadOutcome::Data(full.slice(slice_start..slice_end))
    }

    fn ensure_prefetch(&mut self, start: u64, ctx: &ReadContext<'_>) -> bool {
        let need_restart = self
            .prefetch
            .as_ref()
            .is_none_or(|prefetch| !prefetch.is_valid_for(start));

        if need_restart {
            if ctx.debug_logging {
                tracing::debug!(ino = self.ino, position = start, "starting stream reader");
            }
            self.prefetch = Prefetch::start(
                ctx.client.clone(),
                ctx.stream_url.to_string(),
                start,
                ctx.runtime,
            );
        }

        self.prefetch.is_some()
    }

    fn read_body(
        &mut self,
        chunks: &[ChunkRange],
        start: u64,
        end: u64,
        ctx: &ReadContext<'_>,
    ) -> ReadOutcome {
        if let Some(buffered) = self.stream_buffer.read_range(start, end) {
            return ReadOutcome::Data(buffered);
        }

        let Some(first_missing) = chunks
            .iter()
            .find(|chunk| {
                self.stream_buffer.get_chunk(**chunk).is_none()
                    && cache_get(ctx.cache, (self.ino, chunk.start, chunk.end)).is_none()
            })
            .copied()
        else {
            return self.read_cached_chunks(start, end, chunks, ctx.cache);
        };

        for attempt in 0..2 {
            if !self.ensure_prefetch(first_missing.start, ctx) {
                tracing::error!(ino = self.ino, "failed to start stream reader");
                return ReadOutcome::Error(libc::EIO);
            }

            let mut full = BytesMut::with_capacity(chunks.iter().map(|chunk| chunk.len()).sum());
            let mut failed = false;

            for chunk in chunks {
                let key = (self.ino, chunk.start, chunk.end);
                let data = if let Some(buffered) = self.stream_buffer.get_chunk(*chunk) {
                    buffered
                } else if let Some(cached) = cache_get(ctx.cache, key) {
                    self.stream_buffer.insert(*chunk, cached.clone());
                    cached
                } else {
                    match self
                        .prefetch
                        .as_mut()
                        .expect("prefetch must exist after ensure_prefetch")
                        .read(chunk.start, chunk.len(), ctx.runtime)
                    {
                        Ok(data) => {
                            cache_put(ctx.cache, key, data.clone());
                            self.stream_buffer.insert(*chunk, data.clone());
                            data
                        }
                        Err(e) => {
                            if attempt == 0 {
                                tracing::warn!(
                                    ino = self.ino,
                                    error = %e,
                                    "stream read failed, retrying once"
                                );
                                self.prefetch = None;
                                failed = true;
                                break;
                            }
                            tracing::error!(
                                ino = self.ino,
                                error = %e,
                                "stream read failed after retry"
                            );
                            self.prefetch = None;
                            return ReadOutcome::Error(libc::EIO);
                        }
                    }
                };

                full.extend_from_slice(&data);
            }

            if failed {
                continue;
            }

            let offset = (start - chunks[0].start) as usize;
            let slice_len = (end - start + 1) as usize;
            let full = full.freeze();
            if offset + slice_len > full.len() {
                tracing::error!(
                    ino = self.ino,
                    start,
                    end,
                    body_len = full.len(),
                    "body read shorter than requested"
                );
                return ReadOutcome::Error(libc::EIO);
            }

            return ReadOutcome::Data(full.slice(offset..offset + slice_len));
        }

        ReadOutcome::Error(libc::EIO)
    }
}
