use bytes::{Bytes, BytesMut};

use crate::cache::{cache_get, cache_put, RangeCache};
use crate::chunks::{ChunkRange, FileLayout};
use crate::detect::{detect_read_type, ReadType};
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
}

impl MediaStream {
    pub fn new(ino: u64, file_size: u64) -> Self {
        Self {
            ino,
            file_size,
            layout: FileLayout::new(file_size),
            last_read_end: None,
            prefetch: None,
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
        let read_type = detect_read_type(
            self.ino,
            start,
            end,
            (end - start + 1) as usize,
            self.last_read_end,
            &self.layout,
            &chunks,
            cache,
        );

        if debug_logging {
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
            ReadType::HeaderScan => self.read_scan_range(
                start, end, chunks[0], true, stream_url, cache, client, runtime,
            ),
            ReadType::FooterScan | ReadType::FooterRead => {
                let chunk = *chunks.last().unwrap_or(&chunks[0]);
                self.read_scan_range(start, end, chunk, true, stream_url, cache, client, runtime)
            }
            ReadType::GeneralScan => self.read_scan_range(
                start,
                end,
                ChunkRange { start, end },
                false,
                stream_url,
                cache,
                client,
                runtime,
            ),
            ReadType::BodyRead => self.read_body(
                &chunks,
                start,
                end,
                stream_url,
                cache,
                client,
                runtime,
                debug_logging,
            ),
            ReadType::CacheHit => self.read_cached_chunks(start, end, &chunks, cache),
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
        if let Some(data) = cache_get(cache, key) {
            return Ok(data);
        }

        match runtime.block_on(fetch_range(client, stream_url, chunk.start, chunk.end)) {
            Ok(data) => {
                cache_put(cache, key, data.clone());
                Ok(data)
            }
            Err(e) => {
                tracing::error!(ino = self.ino, error = %e, "range fetch failed");
                Err(())
            }
        }
    }

    fn read_cached_chunks(
        &self,
        start: u64,
        end: u64,
        chunks: &[ChunkRange],
        cache: &RangeCache,
    ) -> ReadOutcome {
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

    #[allow(clippy::too_many_arguments)]
    fn read_scan_range(
        &mut self,
        start: u64,
        end: u64,
        chunk: ChunkRange,
        should_cache: bool,
        stream_url: &str,
        cache: &RangeCache,
        client: &reqwest::Client,
        runtime: &tokio::runtime::Handle,
    ) -> ReadOutcome {
        self.prefetch = None;

        let full = if should_cache {
            match self.fetch_and_cache_range(chunk, stream_url, cache, client, runtime) {
                Ok(data) => data,
                Err(()) => return ReadOutcome::Error(libc::EIO),
            }
        } else {
            match runtime.block_on(fetch_range(client, stream_url, start, end)) {
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

    fn ensure_prefetch(
        &mut self,
        start: u64,
        client: &reqwest::Client,
        stream_url: &str,
        runtime: &tokio::runtime::Handle,
        debug_logging: bool,
    ) -> bool {
        let need_restart = self
            .prefetch
            .as_ref()
            .map(|prefetch| !prefetch.is_valid_for(start))
            .unwrap_or(true);

        if need_restart {
            if debug_logging {
                tracing::debug!(ino = self.ino, position = start, "starting stream reader");
            }
            self.prefetch = Prefetch::start(client.clone(), stream_url.to_string(), start, runtime);
        }

        self.prefetch.is_some()
    }

    #[allow(clippy::too_many_arguments)]
    fn read_body(
        &mut self,
        chunks: &[ChunkRange],
        start: u64,
        end: u64,
        stream_url: &str,
        cache: &RangeCache,
        client: &reqwest::Client,
        runtime: &tokio::runtime::Handle,
        debug_logging: bool,
    ) -> ReadOutcome {
        let Some(first_missing) = chunks
            .iter()
            .find(|chunk| cache_get(cache, (self.ino, chunk.start, chunk.end)).is_none())
            .copied()
        else {
            return self.read_cached_chunks(start, end, chunks, cache);
        };

        for attempt in 0..2 {
            if !self.ensure_prefetch(
                first_missing.start,
                client,
                stream_url,
                runtime,
                debug_logging,
            ) {
                tracing::error!(ino = self.ino, "failed to start stream reader");
                return ReadOutcome::Error(libc::EIO);
            }

            let mut full = BytesMut::with_capacity(chunks.iter().map(|chunk| chunk.len()).sum());
            let mut failed = false;

            for chunk in chunks {
                let key = (self.ino, chunk.start, chunk.end);
                let data = if let Some(cached) = cache_get(cache, key) {
                    cached
                } else {
                    match self
                        .prefetch
                        .as_mut()
                        .expect("prefetch must exist after ensure_prefetch")
                        .read(chunk.start, chunk.len(), runtime)
                    {
                        Ok(data) => {
                            cache_put(cache, key, data.clone());
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
