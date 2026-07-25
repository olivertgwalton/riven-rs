//! Closed-loop playback benchmark for the usenet streaming stack.
//!
//! Drives the real production path — `NntpPool` -> `UsenetStreamer` ->
//! `UsenetSource` -> `Prefetcher` — against a real release, and reports the
//! numbers that decide whether a player buffers:
//!
//! - **delivered throughput**, versus the bitrate the title needs;
//! - **per-read latency**, since a FUSE read that blocks *is* a stutter; and
//! - **stall time**, the share of wall clock the reader spent waiting.
//!
//! `nntp_bench.py` measures the provider in isolation and says it is fast.
//! This measures what riven does with that provider, so the two can be
//! compared directly.
//!
//! Run against a local port-forward of the compose Postgres:
//!
//! ```text
//! PKG_CONFIG_PATH=/usr/local/lib/pkgconfig \
//! RIVEN_BENCH_DB=postgresql://riven:riven@127.0.0.1:55432/riven \
//! cargo run --release -p riven-vfs --example stream_bench -- \
//!     --info-hash nzb-... --seconds 60 --handles 2
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use riven_usenet::nntp::{NntpConfig, NntpProvider, NntpServerConfig};
use riven_usenet::streamer::UsenetStreamer;
use riven_vfs::prefetch::Prefetcher;
use riven_vfs::source::{ByteSource, UsenetSource};

/// The unit a player's FUSE reads arrive in.
const READ_SIZE: usize = 128 * 1024;
/// Production default (`cache_max_size_mb == 0`) in `RivenFsInner::new`.
const DEFAULT_MAX_WINDOW: u64 = 50 * 1024 * 1024;

struct Args {
    info_hash: String,
    file_index: usize,
    /// Where to start, as a fraction of the file. Deliberately not 0: the
    /// head is pinned and precached, so starting there measures the one part
    /// of the file that is never the problem.
    start_frac: f64,
    seconds: u64,
    /// Concurrent readers on the same file. Players really do open more than
    /// one handle, and each one used to get its own independent read-ahead
    /// budget, so this is the knob that reproduces the overload.
    handles: usize,
    max_connections: u32,
    max_window: u64,
    /// Title bitrate to judge against, in Mbps. Reported as a verdict.
    bitrate_mbps: f64,
    label: String,
}

fn parse_args() -> Args {
    let mut args = Args {
        info_hash: String::new(),
        file_index: 0,
        start_frac: 0.35,
        seconds: 60,
        handles: 1,
        max_connections: 100,
        max_window: DEFAULT_MAX_WINDOW,
        bitrate_mbps: 0.0,
        label: String::new(),
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].clone();
        let value = argv.get(i + 1).cloned().unwrap_or_default();
        match flag.as_str() {
            "--info-hash" => args.info_hash = value,
            "--file-index" => args.file_index = value.parse().unwrap_or(0),
            "--start-frac" => args.start_frac = value.parse().unwrap_or(0.35),
            "--seconds" => args.seconds = value.parse().unwrap_or(60),
            "--handles" => args.handles = value.parse().unwrap_or(1),
            "--max-connections" => args.max_connections = value.parse().unwrap_or(100),
            "--max-window-mb" => args.max_window = value.parse().unwrap_or(50) * 1024 * 1024,
            "--bitrate-mbps" => args.bitrate_mbps = value.parse().unwrap_or(0.0),
            "--label" => args.label = value,
            other => panic!("unknown argument: {other}"),
        }
        i += 2;
    }
    assert!(!args.info_hash.is_empty(), "--info-hash is required");
    args
}

/// Latency samples for one reader, in microseconds.
#[derive(Default)]
struct Samples(Vec<u64>);

impl Samples {
    fn pct(&mut self, p: f64) -> f64 {
        if self.0.is_empty() {
            return 0.0;
        }
        self.0.sort_unstable();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let idx = ((self.0.len() as f64 - 1.0) * p) as usize;
        self.0[idx] as f64 / 1000.0
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_target(true)
        .init();

    let args = parse_args();
    let db_url = std::env::var("RIVEN_BENCH_DB")
        .unwrap_or_else(|_| "postgresql://riven:riven@127.0.0.1:55432/riven".to_string());
    let user = std::env::var("RIVEN_BENCH_NNTP_USER").ok();
    let pass = std::env::var("RIVEN_BENCH_NNTP_PASS").ok();
    let host =
        std::env::var("RIVEN_BENCH_NNTP_HOST").unwrap_or_else(|_| "news.newshosting.com".into());
    let port: u16 = std::env::var("RIVEN_BENCH_NNTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(563);

    let db = sea_orm::Database::connect(&db_url).await?;

    let cfg = NntpConfig {
        providers: vec![NntpProvider {
            config: NntpServerConfig {
                host,
                port,
                user,
                pass,
                use_tls: true,
                max_connections: args.max_connections,
                timeout: Duration::from_secs(30),
            },
            priority: 0,
            is_backup: false,
        }],
    };

    let streamer = UsenetStreamer::new(cfg, db);
    let meta = streamer.load_meta(&args.info_hash).await?;
    let file = meta
        .files
        .get(args.file_index)
        .ok_or_else(|| anyhow::anyhow!("file index {} not in meta", args.file_index))?;
    let size = file.total_size;
    let filename = file.filename.clone();

    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let start = ((size as f64) * args.start_frac) as u64 & !(READ_SIZE as u64 - 1);

    println!("=====================================================================");
    if !args.label.is_empty() {
        println!("label        : {}", args.label);
    }
    println!("file         : {filename}");
    println!(
        "size         : {:.1} GB   start at {:.0}% ({:.1} GB)",
        size as f64 / 1e9,
        args.start_frac * 100.0,
        start as f64 / 1e9
    );
    println!(
        "config       : handles={} max_connections={} window={} MiB duration={}s",
        args.handles,
        args.max_connections,
        args.max_window >> 20,
        args.seconds
    );
    println!("=====================================================================");

    let streamer: Arc<dyn riven_core::local_source::LocalByteSource> = Arc::new(streamer);
    let deadline = Instant::now() + Duration::from_secs(args.seconds);
    let bytes_total = Arc::new(AtomicU64::new(0));

    // Each reader is an independent open handle on the same file, exactly as
    // a player that opens the video plus a probe stream would produce.
    let mut tasks = Vec::new();
    for handle_idx in 0..args.handles {
        let source: Arc<dyn ByteSource> = Arc::new(UsenetSource::new(
            Arc::clone(&streamer),
            Arc::from(args.info_hash.as_str()),
            args.file_index,
            size,
            &filename,
        ));
        let prefetcher = Arc::new(Prefetcher::new(source, args.max_window));
        let bytes_total = Arc::clone(&bytes_total);
        // Stagger the readers slightly so they do not march in lockstep.
        let mut position = start + (handle_idx as u64 * 4 * READ_SIZE as u64);

        tasks.push(tokio::spawn(async move {
            let mut samples = Samples::default();
            let mut bytes: u64 = 0;
            let mut first_byte_ms = 0.0;
            let began = Instant::now();
            while Instant::now() < deadline && position < size {
                let read_began = Instant::now();
                let data = match prefetcher.read(position, READ_SIZE).await {
                    Ok(data) => data,
                    Err(error) => {
                        eprintln!("reader {handle_idx}: read failed at {position}: {error}");
                        break;
                    }
                };
                let elapsed = read_began.elapsed();
                if bytes == 0 {
                    first_byte_ms = began.elapsed().as_secs_f64() * 1000.0;
                }
                if data.is_empty() {
                    break;
                }
                samples.0.push(elapsed.as_micros() as u64);
                position += data.len() as u64;
                bytes += data.len() as u64;
                bytes_total.fetch_add(data.len() as u64, Ordering::Relaxed);
            }
            (handle_idx, samples, bytes, began.elapsed(), first_byte_ms)
        }));
    }

    let mut rows = Vec::new();
    for task in tasks {
        rows.push(task.await?);
    }

    println!();
    println!(
        "{:<4} {:>10} {:>10} {:>9} {:>9} {:>9} {:>9} {:>10} {:>8}",
        "rdr", "MB", "MB/s", "ttfb ms", "p50 ms", "p90 ms", "p99 ms", "max ms", "stall%"
    );
    println!("{}", "-".repeat(88));

    let mut all = Samples::default();
    let mut wall = Duration::ZERO;
    for (idx, mut samples, bytes, elapsed, ttfb) in rows {
        let secs = elapsed.as_secs_f64();
        wall = wall.max(elapsed);
        // Anything over one 128 KiB read-time at the target rate is the
        // reader waiting on the origin rather than being served from buffer.
        let stall_us: u64 = samples.0.iter().filter(|us| **us > 50_000).sum();
        #[allow(clippy::cast_precision_loss)]
        let stall_pct = (stall_us as f64 / 1e6) / secs * 100.0;
        println!(
            "{:<4} {:>10.1} {:>10.2} {:>9.0} {:>9.1} {:>9.1} {:>9.1} {:>10.1} {:>8.1}",
            idx,
            bytes as f64 / 1e6,
            bytes as f64 / 1e6 / secs,
            ttfb,
            samples.pct(0.50),
            samples.pct(0.90),
            samples.pct(0.99),
            samples.pct(1.0),
            stall_pct,
        );
        all.0.extend_from_slice(&samples.0);
    }

    let total = bytes_total.load(Ordering::Relaxed);
    let secs = wall.as_secs_f64();
    let mb_s = total as f64 / 1e6 / secs;
    let mbps = mb_s * 8.0;
    println!("{}", "-".repeat(88));
    println!(
        "AGGREGATE    {:.1} MB in {:.1}s = {:.2} MB/s ({:.0} Mbps)   reads={}  p50={:.1}ms p99={:.1}ms",
        total as f64 / 1e6,
        secs,
        mb_s,
        mbps,
        all.0.len(),
        all.pct(0.50),
        all.pct(0.99),
    );

    if args.bitrate_mbps > 0.0 {
        let headroom = mbps / args.bitrate_mbps;
        println!();
        println!(
            "VERDICT      title needs {:.0} Mbps; delivered {:.0} Mbps = {headroom:.1}x headroom -> {}",
            args.bitrate_mbps,
            mbps,
            if headroom >= 2.0 {
                "streams cleanly"
            } else if headroom >= 1.0 {
                "marginal, will buffer on bitrate peaks"
            } else {
                "BUFFERS"
            }
        );
    }
    println!();

    Ok(())
}
