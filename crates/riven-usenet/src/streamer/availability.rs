//! Bounded STAT sweeps over a batch of segments.
//!
//! Shared by `ingest.rs::probe_availability`, `scan_availability`, and
//! `verify_release_complete` — all three STAT a sample of message-ids at
//! bounded concurrency and tally missing/error/checked counts. Modelled on
//! altmount's `FastFailReleaseProbe`/`StatMany` (javi11/altmount): the sweep
//! carries its own deadline instead of depending solely on the caller's outer
//! timeout, and a zero-tolerance caller can cancel the sweep the instant a
//! miss is confirmed instead of draining the whole sample.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::stream;

use crate::nntp::NntpClient;

/// Per-wave STAT budget: mirrors altmount's `pool.StatManyTimeout`
/// (`javi11/altmount/internal/pool/stat_timeout.go`) — a sweep's deadline
/// scales with how many concurrency-limited waves the batch needs rather than
/// one flat timeout regardless of batch size, so a genuine stall is bounded
/// by the work actually outstanding. STAT is a single-line request/reply;
/// 15s is generous headroom over a normal round-trip (observed sub-2s even
/// under contention) while staying well under the caller's outer deadline.
const STAT_WAVE_TIMEOUT: Duration = Duration::from_secs(15);

/// See [`STAT_WAVE_TIMEOUT`]. `waves = ceil(count / concurrency)`.
pub(crate) fn stat_sweep_timeout(count: usize, concurrency: usize) -> Duration {
    if count == 0 {
        return STAT_WAVE_TIMEOUT;
    }
    let waves = count.div_ceil(concurrency.max(1)).max(1);
    STAT_WAVE_TIMEOUT * waves as u32
}

/// Outcome of a bounded STAT sweep: how many probed segments were confirmed
/// missing, how many probes errored (connection/provider trouble rather than
/// a clean "not found"), and how many completed before the sweep finished or
/// hit its deadline.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SweepCounts {
    pub missing: usize,
    pub errors: usize,
    pub checked: usize,
}

/// STAT `mids` against `pool` at bounded concurrency, capped by
/// [`stat_sweep_timeout`] instead of the caller's outer deadline alone. When
/// `stop_on_first_miss` is set, the sweep also cancels itself the moment a
/// missing segment is confirmed — for a zero-tolerance gate, continuing to
/// probe a candidate already known to have a hole is wasted work (mirrors
/// altmount's `FastFailReleaseProbe`, which cancels its `StatMany` context on
/// the first miss rather than draining the whole sample).
///
/// Counts live outside the timed future so a deadline hit still reports how
/// far the sweep got instead of discarding partial progress. If the sweep
/// times out having confirmed nothing at all (`checked == 0`), the caller
/// gets an all-zero result — deliberately fails open, same as altmount: a
/// sweep that couldn't complete a single round-trip has proven nothing about
/// the release, and the real cause (provider/pool trouble) will surface with
/// a proper error on the next stage's own network calls rather than being
/// misreported as "release confirmed missing."
pub(crate) async fn stat_sweep(
    client: &NntpClient,
    mids: Vec<String>,
    concurrency: usize,
    stop_on_first_miss: bool,
) -> SweepCounts {
    let n = mids.len();
    if n == 0 {
        return SweepCounts::default();
    }
    let concurrency = concurrency.max(1);
    let deadline = stat_sweep_timeout(n, concurrency);

    let missing = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    let checked = AtomicUsize::new(0);

    let sweep = async {
        let mut probes = stream::iter(mids)
            .map(|mid| async move { client.stat(&mid).await })
            .buffer_unordered(concurrency);

        while let Some(result) = probes.next().await {
            checked.fetch_add(1, Ordering::Relaxed);
            match result {
                Ok(true) => {}
                Ok(false) => {
                    missing.fetch_add(1, Ordering::Relaxed);
                    if stop_on_first_miss {
                        // Dropping `probes` cancels the still-in-flight STATs.
                        break;
                    }
                }
                Err(error) => {
                    tracing::debug!(error = %error, "availability probe error");
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    };

    if tokio::time::timeout(deadline, sweep).await.is_err() {
        tracing::debug!(
            total = n,
            concurrency,
            deadline_secs = deadline.as_secs(),
            checked = checked.load(Ordering::Relaxed),
            "availability probe sweep hit its deadline before finishing"
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
    use crate::nntp::NntpPool;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    use super::*;
    use crate::nntp::{NntpProvider, NntpServerConfig};

    #[test]
    fn timeout_scales_with_wave_count() {
        // A batch that fits in one wave (count <= concurrency) gets exactly
        // one wave's budget, matching altmount's `StatManyTimeout` contract.
        assert_eq!(stat_sweep_timeout(10, 10), STAT_WAVE_TIMEOUT);
        assert_eq!(stat_sweep_timeout(1, 10), STAT_WAVE_TIMEOUT);
        // ceil(25 / 10) = 3 waves.
        assert_eq!(stat_sweep_timeout(25, 10), STAT_WAVE_TIMEOUT * 3);
        // Zero concurrency is clamped to 1 wave-worth-per-item, not division
        // by zero.
        assert_eq!(stat_sweep_timeout(5, 0), STAT_WAVE_TIMEOUT * 5);
    }

    /// Spawns a loopback NNTP server that greets, then answers every `STAT
    /// <id>` with `223` unless `id` is in `missing_ids`, in which case it
    /// answers `430`. Enough to drive `stat_sweep` end-to-end without a real
    /// provider.
    async fn spawn_stat_server(
        missing_ids: std::collections::HashSet<String>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let missing_ids = missing_ids.clone();
                tokio::spawn(async move {
                    let (read_half, mut write_half) = socket.into_split();
                    let mut reader = BufReader::new(read_half);
                    if write_half
                        .write_all(b"200 fake nntp ready\r\n")
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let mut line = String::new();
                    loop {
                        line.clear();
                        match reader.read_line(&mut line).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                        let Some(rest) = line.trim_end().strip_prefix("STAT ") else {
                            continue;
                        };
                        let id = rest.trim_matches(['<', '>']);
                        let reply = if missing_ids.contains(id) {
                            "430 No such article\r\n".to_string()
                        } else {
                            format!("223 0 <{id}>\r\n")
                        };
                        if write_half.write_all(reply.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, handle)
    }

    fn test_provider(addr: std::net::SocketAddr, max_connections: u32) -> NntpProvider {
        NntpProvider {
            config: NntpServerConfig {
                host: addr.ip().to_string(),
                port: addr.port(),
                user: None,
                pass: None,
                use_tls: false,
                max_connections,
                timeout: Duration::from_secs(5),
            },
            priority: 0,
            is_backup: false,
        }
    }

    /// Regression test for the early-cancel behavior ported from altmount's
    /// `FastFailReleaseProbe`: once a sweep confirms one missing segment, it
    /// must stop rather than draining the rest of the sample. With 10 ids and
    /// concurrency 3, at most a handful of the trailing ids can ever be
    /// dispatched before the loop sees the miss and breaks — `checked` must
    /// land well short of the full batch.
    #[tokio::test]
    async fn stop_on_first_miss_cancels_remaining_probes() {
        let missing_id = "seg-0".to_string();
        let (addr, _server) =
            spawn_stat_server(std::collections::HashSet::from([missing_id.clone()])).await;
        let pool = NntpPool::new_multi(vec![test_provider(addr, 8)]);

        let mids: Vec<String> = (0..10).map(|i| format!("seg-{i}")).collect();
        let client = pool.bulk_client();
        let counts = stat_sweep(&client, mids, 3, true).await;

        assert_eq!(counts.missing, 1);
        assert!(
            counts.checked < 10,
            "expected early cancellation to skip some of the batch, checked {}",
            counts.checked
        );
    }

    /// Without `stop_on_first_miss`, the sweep drains the whole sample and
    /// reports every miss — the behavior `scan_availability` and
    /// `verify_release_complete` rely on for accurate counts.
    #[tokio::test]
    async fn full_sweep_reports_every_miss_when_not_stopping_early() {
        let missing_ids: std::collections::HashSet<String> = ["seg-1", "seg-4", "seg-7"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (addr, _server) = spawn_stat_server(missing_ids).await;
        let pool = NntpPool::new_multi(vec![test_provider(addr, 8)]);

        let mids: Vec<String> = (0..10).map(|i| format!("seg-{i}")).collect();
        let client = pool.bulk_client();
        let counts = stat_sweep(&client, mids, 4, false).await;

        assert_eq!(counts.checked, 10);
        assert_eq!(counts.missing, 3);
        assert_eq!(counts.errors, 0);
    }
}
