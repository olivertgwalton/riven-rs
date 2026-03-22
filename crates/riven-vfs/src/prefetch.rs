use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

/// Number of channel slots. Each slot holds one TCP chunk (~8–64 KB from reqwest).
/// At 64 KB average, 512 slots ≈ 32 MB buffered ahead of the player.
const CHANNEL_CAPACITY: usize = 512;

/// How far ahead (in bytes) the prefetch is allowed to be positioned and still
/// considered valid for a given read offset. Beyond this we reconnect.
const MAX_SKIP_BYTES: u64 = 4 * 1024 * 1024; // 4 MB

pub struct Prefetch {
    /// File offset of the next byte that `read()` will return.
    pub read_pos: u64,
    /// Bytes received from the task but not yet returned to the caller.
    local_buf: Vec<u8>,
    /// Stream of byte chunks from the background HTTP task.
    rx: mpsc::Receiver<Result<Vec<u8>, String>>,
    /// Aborts the background streaming task when dropped.
    _task: AbortHandle,
}

impl Prefetch {
    /// Spawn a background task that opens an HTTP stream at `start_pos` and
    /// feeds data into a channel for `read()` to consume.
    pub fn start(
        client: reqwest::Client,
        url: String,
        start_pos: u64,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);

        let task = runtime.spawn(async move {
            run(client, url, start_pos, tx).await;
        });

        Self {
            read_pos: start_pos,
            local_buf: Vec::new(),
            rx,
            _task: task.abort_handle(),
        }
    }

    /// Returns true if this prefetch can serve a read at `pos` without reconnecting.
    pub fn is_valid_for(&self, pos: u64) -> bool {
        let buffered_end = self.read_pos + self.local_buf.len() as u64;
        pos >= self.read_pos && pos <= buffered_end + MAX_SKIP_BYTES
    }

    /// Read `size` bytes at file offset `pos`.
    ///
    /// Blocks until enough data has arrived from the background task.
    /// For sequential reads this returns instantly once the buffer is primed.
    pub fn read(
        &mut self,
        pos: u64,
        size: usize,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Vec<u8>> {
        // Skip forward if there is a small gap (e.g. FUSE alignment reads).
        if pos > self.read_pos {
            let skip = (pos - self.read_pos) as usize;
            self.fill_at_least(skip, runtime)?;
            let actual_skip = skip.min(self.local_buf.len());
            self.local_buf.drain(..actual_skip);
            self.read_pos += actual_skip as u64;
        }

        self.fill_at_least(size, runtime)?;

        let available = self.local_buf.len().min(size);
        let data = self.local_buf[..available].to_vec();
        self.local_buf.drain(..available);
        self.read_pos += available as u64;
        Ok(data)
    }

    /// Pull from the channel until `local_buf` has at least `needed` bytes or EOF.
    fn fill_at_least(
        &mut self,
        needed: usize,
        runtime: &tokio::runtime::Handle,
    ) -> Result<()> {
        while self.local_buf.len() < needed {
            match runtime.block_on(self.rx.recv()) {
                Some(Ok(chunk)) => self.local_buf.extend_from_slice(&chunk),
                Some(Err(e)) => anyhow::bail!("{e}"),
                None => break, // background task finished (EOF or dropped)
            }
        }
        Ok(())
    }
}

/// Background task: open an HTTP range stream from `start_pos` to EOF and
/// push every incoming TCP chunk into the channel.
///
/// Exits when the channel receiver is dropped (file closed / seek) or on EOF / error.
async fn run(
    client: reqwest::Client,
    url: String,
    start_pos: u64,
    tx: mpsc::Sender<Result<Vec<u8>, String>>,
) {
    use crate::stream::create_stream_request;

    let mut response = match create_stream_request(&client, &url, start_pos, None).await {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.send(Err(e.to_string())).await;
            return;
        }
    };

    loop {
        match response.chunk().await {
            Ok(Some(bytes)) => {
                if tx.send(Ok(bytes.to_vec())).await.is_err() {
                    // Receiver dropped — file was closed or a seek triggered a restart.
                    break;
                }
            }
            Ok(None) => break, // EOF
            Err(e) => {
                let _ = tx.send(Err(e.to_string())).await;
                break;
            }
        }
    }
}
