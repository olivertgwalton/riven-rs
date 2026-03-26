use bytes::Bytes;
use futures::TryStreamExt;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

const BUF_CAPACITY: usize = 8 * 1024 * 1024; // 8 MB read-ahead

pub struct Prefetch {
    pub read_pos: u64,
    reader: Box<dyn AsyncRead + Send + Unpin>,
}

impl Prefetch {
    pub fn start(
        client: reqwest::Client,
        url: String,
        start_pos: u64,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Self> {
        let resp = runtime.block_on(async {
            client
                .get(&url)
                .header("range", format!("bytes={start_pos}-"))
                .header("accept-encoding", "identity")
                .header("connection", "keep-alive")
                .timeout(std::time::Duration::from_secs(
                    riven_core::config::vfs::ACTIVITY_TIMEOUT_SECS,
                ))
                .send()
                .await
        });

        let resp = match resp {
            Ok(r) if r.status().is_success() || r.status() == reqwest::StatusCode::PARTIAL_CONTENT => r,
            _ => return None,
        };

        let stream = resp
            .bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e));
        let reader = BufReader::with_capacity(BUF_CAPACITY, StreamReader::new(stream));

        Some(Self {
            read_pos: start_pos,
            reader: Box::new(reader),
        })
    }

    pub fn is_valid_for(&self, pos: u64) -> bool {
        pos >= self.read_pos && pos <= self.read_pos + BUF_CAPACITY as u64
    }

    pub fn read(
        &mut self,
        pos: u64,
        size: usize,
        runtime: &tokio::runtime::Handle,
    ) -> io::Result<Bytes> {
        if pos > self.read_pos {
            let skip = (pos - self.read_pos) as usize;
            runtime.block_on(discard(&mut self.reader, skip))?;
            self.read_pos = pos;
        }

        let mut buf = vec![0u8; size];
        let n = runtime.block_on(read_full(&mut self.reader, &mut buf))?;
        buf.truncate(n);
        self.read_pos += n as u64;
        Ok(Bytes::from(buf))
    }
}

async fn discard(reader: &mut (dyn AsyncRead + Unpin), mut n: usize) -> io::Result<()> {
    let mut trash = [0u8; 8192];
    while n > 0 {
        let to_read = n.min(trash.len());
        let read = reader.read(&mut trash[..to_read]).await?;
        if read == 0 {
            break;
        }
        n -= read;
    }
    Ok(())
}

async fn read_full(reader: &mut (dyn AsyncRead + Unpin), buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}
