//! Article-availability sweeps.
//!
//! A sweep STATs a list of message-ids and tallies what is missing. Commands
//! are pipelined in batches over a single connection rather than one
//! connection per article — a sample of a few hundred ids then costs a handful
//! of round trips instead of a few hundred.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::stream;

use crate::pool::SegmentPool;

/// Commands written to one connection before its replies are read.
const PIPELINE_DEPTH: usize = 6;
/// Per-wave budget. `waves = ceil(count / concurrency)`, so a genuine stall is
/// bounded by the work actually outstanding rather than one flat timeout
/// regardless of batch size.
const SWEEP_WAVE_TIMEOUT: Duration = Duration::from_secs(15);

/// Counts from a sweep: confirmed missing, errored (provider trouble rather
/// than a clean "not found"), and how many completed.
#[derive(Debug, Default, Clone, Copy)]
pub struct SweepCounts {
    pub missing: usize,
    pub errors: usize,
    pub checked: usize,
}

fn sweep_timeout(count: usize, concurrency: usize) -> Duration {
    if count == 0 {
        return SWEEP_WAVE_TIMEOUT;
    }
    SWEEP_WAVE_TIMEOUT * count.div_ceil(concurrency.max(1)).max(1) as u32
}

/// STAT every id in `message_ids` across the pool. `concurrency` bounds
/// articles in flight, not connections — they are pipelined
/// [`PIPELINE_DEPTH`] at a time per connection.
///
/// Counts live outside the timed future so a deadline still reports how far
/// the sweep got. A sweep that confirms nothing at all returns all zeros and
/// deliberately fails open: it has proven nothing about the release, and the
/// real cause will surface as a proper error on the next network call rather
/// than being misreported as "release confirmed missing".
pub async fn sweep_all(
    pool: &SegmentPool,
    message_ids: Vec<String>,
    concurrency: usize,
    stop_on_first_miss: bool,
    label: &str,
) -> SweepCounts {
    let total = message_ids.len();
    if total == 0 {
        return SweepCounts::default();
    }
    let concurrency = concurrency.max(1);
    let deadline = sweep_timeout(total, concurrency);

    let missing = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    let checked = AtomicUsize::new(0);

    let batches: Vec<Vec<String>> = message_ids
        .chunks(PIPELINE_DEPTH)
        .map(<[String]>::to_vec)
        .collect();
    let concurrent_batches = concurrency.div_ceil(PIPELINE_DEPTH).max(1);

    let sweep = async {
        let mut probes = stream::iter(batches)
            .map(|batch| async move {
                let result = pool.stat_batch(&batch).await;
                (batch.len(), result)
            })
            .buffer_unordered(concurrent_batches);

        while let Some((size, result)) = probes.next().await {
            match result {
                Ok(found) => {
                    checked.fetch_add(found.len(), Ordering::Relaxed);
                    let absent = found.iter().filter(|present| !**present).count();
                    if absent > 0 {
                        missing.fetch_add(absent, Ordering::Relaxed);
                        if stop_on_first_miss {
                            break;
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(%error, label, "availability probe batch errored");
                    errors.fetch_add(size, Ordering::Relaxed);
                    checked.fetch_add(size, Ordering::Relaxed);
                }
            }
        }
    };

    if tokio::time::timeout(deadline, sweep).await.is_err() {
        tracing::debug!(
            label,
            total,
            concurrency,
            deadline_secs = deadline.as_secs(),
            checked = checked.load(Ordering::Relaxed),
            "availability sweep hit its deadline before finishing"
        );
    }

    SweepCounts {
        missing: missing.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        checked: checked.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::nntp::tests::spawn_fake_nntp_server;
    use crate::nntp::{NntpProvider, NntpServerConfig};

    #[test]
    fn sweep_timeout_scales_with_wave_count() {
        assert_eq!(sweep_timeout(10, 10), SWEEP_WAVE_TIMEOUT);
        assert_eq!(sweep_timeout(25, 10), SWEEP_WAVE_TIMEOUT * 3);
        assert_eq!(sweep_timeout(5, 0), SWEEP_WAVE_TIMEOUT * 5);
    }

    fn provider(addr: std::net::SocketAddr) -> NntpProvider {
        NntpProvider {
            config: NntpServerConfig {
                host: addr.ip().to_string(),
                port: addr.port(),
                user: None,
                pass: None,
                use_tls: false,
                max_connections: 8,
                article_timeout: Duration::from_millis(200),
            },
            priority: 0,
            is_backup: false,
        }
    }

    #[tokio::test]
    async fn sweep_pipelines_instead_of_taking_a_connection_per_article() {
        let (addr, _server) = spawn_fake_nntp_server().await;
        let pool = SegmentPool::new(vec![provider(addr)]);
        let ids: Vec<String> = (0..24).map(|i| format!("seg-{i}@test")).collect();

        let counts = sweep_all(&pool, ids, 6, false, "test.mkv").await;
        assert_eq!(counts.checked, 24);
        assert_eq!(counts.missing, 0);
        assert_eq!(
            pool.health()[0].open_connections,
            1,
            "a 24-article sweep at concurrency 6 must not open a connection per article"
        );
    }

    #[tokio::test]
    async fn sweep_reports_articles_missing_on_every_provider() {
        let (addr, _server) = spawn_missing_server().await;
        let pool = SegmentPool::new(vec![provider(addr)]);
        let ids: Vec<String> = (0..6).map(|i| format!("gone-{i}@test")).collect();

        let counts = sweep_all(&pool, ids, 6, false, "test.mkv").await;
        assert_eq!(counts.checked, 6);
        assert_eq!(counts.missing, 6);
    }

    /// Loopback listener that answers `430` to everything article-shaped.
    async fn spawn_missing_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let (read_half, mut write_half) = socket.into_split();
                    if write_half.write_all(b"200 fake\r\n").await.is_err() {
                        return;
                    }
                    let mut lines = BufReader::new(read_half).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let reply: &[u8] = if line.starts_with("STAT") || line.starts_with("BODY") {
                            b"430 no such article\r\n"
                        } else if line.starts_with("QUIT") {
                            return;
                        } else {
                            b"111 20260101000000\r\n"
                        };
                        if write_half.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, handle)
    }
}
