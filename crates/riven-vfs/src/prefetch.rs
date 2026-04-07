use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use std::io;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use crate::stream::open_stream;

/// Forward seek window: positions up to this far ahead reuse the existing stream.
const LOOKAHEAD_WINDOW: u64 = 32 * 1024 * 1024;
const DISCARD_BUFFER_SIZE: usize = 64 * 1024;

type HttpByteStream = BoxStream<'static, Result<Bytes, io::Error>>;
type ResponseReader = BufReader<StreamReader<HttpByteStream, Bytes>>;

pub struct Prefetch {
    pub read_pos: u64,
    reader: ResponseReader,
}

impl Prefetch {
    pub fn start(
        client: reqwest::Client,
        url: String,
        start_pos: u64,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Self> {
        let response = runtime
            .block_on(open_stream(&client, &url, start_pos))
            .ok()?;
        let stream = response.bytes_stream().map_err(io::Error::other).boxed();
        let reader = BufReader::with_capacity(
            riven_core::config::vfs::CHUNK_SIZE as usize,
            StreamReader::new(stream),
        );

        Some(Self {
            read_pos: start_pos,
            reader,
        })
    }

    pub fn is_valid_for(&self, pos: u64) -> bool {
        pos >= self.read_pos && pos <= self.read_pos + LOOKAHEAD_WINDOW
    }

    pub fn read(
        &mut self,
        pos: u64,
        size: usize,
        runtime: &tokio::runtime::Handle,
    ) -> io::Result<Bytes> {
        runtime.block_on(self.read_exact_at(pos, size))
    }

    async fn read_exact_at(&mut self, pos: u64, size: usize) -> io::Result<Bytes> {
        if pos < self.read_pos {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot rewind active stream from {} to {}",
                    self.read_pos, pos
                ),
            ));
        }

        if pos > self.read_pos {
            self.discard(pos - self.read_pos).await?;
            self.read_pos = pos;
        }

        let mut buf = vec![0; size];
        self.reader.read_exact(&mut buf).await?;
        self.read_pos += size as u64;
        Ok(Bytes::from(buf))
    }

    async fn discard(&mut self, bytes: u64) -> io::Result<()> {
        let mut remaining = bytes as usize;
        let mut scratch = vec![0; DISCARD_BUFFER_SIZE.min(remaining.max(1))];

        while remaining > 0 {
            let n = scratch.len().min(remaining);
            self.reader.read_exact(&mut scratch[..n]).await?;
            remaining -= n;
        }

        Ok(())
    }
}
