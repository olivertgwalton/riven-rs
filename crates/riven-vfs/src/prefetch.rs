use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;

/// How far ahead to buffer. Background task saturates the network until this
/// many bytes are pending, then pauses until the consumer catches up.
/// 64 MB ≈ 11 seconds of buffer for a 44 Mbps stream.
const MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Forward seek window: positions up to this far ahead reuse the existing stream.
const LOOKAHEAD_WINDOW: u64 = 32 * 1024 * 1024;

pub struct Prefetch {
    pub read_pos: u64,
    /// Unbounded — backpressure is handled entirely by `sem`, not channel capacity.
    rx: tokio::sync::mpsc::UnboundedReceiver<io::Result<Bytes>>,
    /// Byte-precise backpressure. Starts with MAX_BUFFER_BYTES permits.
    /// Background task acquires chunk.len() permits before sending each chunk.
    /// Consumer releases permits as bytes are consumed.
    /// Closed on Drop so the background task can exit cleanly.
    sem: Arc<tokio::sync::Semaphore>,
    pending: VecDeque<Bytes>,
    pending_bytes: usize,
    /// File byte offset of pending.front()[0].
    pending_start: u64,
}

impl Drop for Prefetch {
    fn drop(&mut self) {
        // Unblock the background task if it's waiting on a semaphore acquire.
        self.sem.close();
    }
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
                .timeout(Duration::from_secs(
                    riven_core::config::vfs::ACTIVITY_TIMEOUT_SECS,
                ))
                .send()
                .await
        });

        let resp = match resp {
            Ok(r)
                if r.status().is_success()
                    || r.status() == reqwest::StatusCode::PARTIAL_CONTENT =>
            {
                r
            }
            _ => return None,
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let sem = Arc::new(tokio::sync::Semaphore::new(MAX_BUFFER_BYTES));
        let sem_bg = Arc::clone(&sem);

        let chunk_timeout = Duration::from_secs(riven_core::config::vfs::CHUNK_TIMEOUT_SECS);

        // Background task: saturate the network up to MAX_BUFFER_BYTES ahead.
        // Acquiring permits before each send means it stalls when the buffer is
        // full and resumes the instant the consumer frees space — byte-precise.
        runtime.spawn(async move {
            let mut stream = resp.bytes_stream();
            loop {
                let chunk = match tokio::time::timeout(chunk_timeout, stream.next()).await {
                    Ok(Some(Ok(c))) => c,
                    Ok(Some(Err(e))) => {
                        let _ = tx.send(Err(io::Error::new(io::ErrorKind::Other, e)));
                        break;
                    }
                    Ok(None) => break, // EOF
                    Err(_) => {
                        let _ = tx.send(Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "stream stalled",
                        )));
                        break;
                    }
                };

                // Acquire permits proportional to this chunk's byte count.
                // Blocks here (not on the channel) when MAX_BUFFER_BYTES is buffered.
                let n = chunk.len() as u32;
                match sem_bg.acquire_many(n).await {
                    Ok(permit) => permit.forget(), // Released manually by consumer.
                    Err(_) => break,               // Semaphore closed: Prefetch dropped.
                }

                if tx.send(Ok(chunk)).is_err() {
                    break; // Receiver dropped.
                }
            }
        });

        Some(Self {
            read_pos: start_pos,
            rx,
            sem,
            pending: VecDeque::new(),
            pending_bytes: 0,
            pending_start: start_pos,
        })
    }

    pub fn is_valid_for(&self, pos: u64) -> bool {
        let buf_end = self.pending_start + self.pending_bytes as u64;
        pos >= self.pending_start && pos <= buf_end + LOOKAHEAD_WINDOW
    }

    pub fn read(&mut self, pos: u64, size: usize) -> io::Result<Bytes> {
        // Discard chunks that precede the requested position, releasing permits.
        while pos > self.pending_start {
            match self.pending.front() {
                None => break,
                Some(front) => {
                    let gap = (pos - self.pending_start) as usize;
                    if gap >= front.len() {
                        let dropped = self.pending.pop_front().unwrap();
                        let released = dropped.len();
                        self.pending_start += released as u64;
                        self.pending_bytes -= released;
                        self.sem.add_permits(released);
                    } else {
                        let front = self.pending.front_mut().unwrap();
                        *front = front.slice(gap..);
                        self.sem.add_permits(gap);
                        self.pending_start = pos;
                        self.pending_bytes -= gap;
                        break;
                    }
                }
            }
        }

        let needed_end = pos.saturating_add(size as u64);

        // Pull from the background task until we have enough data.
        while self.pending_start + (self.pending_bytes as u64) < needed_end {
            let result = match self.rx.try_recv() {
                Ok(item) => item,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    match self.rx.blocking_recv() {
                        Some(item) => item,
                        None => break,
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            };
            match result {
                Ok(chunk) => {
                    self.pending_bytes += chunk.len();
                    self.pending.push_back(chunk);
                }
                Err(e) => return Err(e),
            }
        }

        // Assemble result and release permits for consumed bytes.
        let available =
            (self.pending_start + self.pending_bytes as u64).saturating_sub(pos) as usize;
        let n = size.min(available);
        let mut out = BytesMut::with_capacity(n);
        let mut remaining = n;

        while remaining > 0 {
            let front = match self.pending.front_mut() {
                Some(f) => f,
                None => break,
            };
            let take = remaining.min(front.len());
            out.extend_from_slice(&front[..take]);
            if take == front.len() {
                self.pending.pop_front();
            } else {
                *front = front.slice(take..);
            }
            self.pending_bytes -= take;
            self.pending_start += take as u64;
            remaining -= take;
        }

        // Release permits for bytes handed to the FUSE layer.
        // This lets the background task download the next n bytes.
        self.sem.add_permits(n);
        self.read_pos = self.pending_start;

        Ok(out.freeze())
    }
}
