use std::io::{self, Read};
use std::time::Instant;

use riven_rank::parse;
use serde::Serialize;

#[derive(Serialize)]
struct BenchResult {
    parser: &'static str,
    n: usize,
    total_s: f64,
    throughput: f64,
    p50_ms: f64,
    p95_ms: f64,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }

    let rank = (sorted.len() - 1) as f64 * p;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;

    if lower == upper {
        sorted[lower]
    } else {
        let weight = rank - lower as f64;
        sorted[lower] + (sorted[upper] - sorted[lower]) * weight
    }
}

fn main() -> anyhow::Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let titles: Vec<String> = serde_json::from_str(&input)?;

    let mut lat_ms = Vec::with_capacity(titles.len());
    let start = Instant::now();

    for title in &titles {
        let t0 = Instant::now();
        let _ = parse(title);
        lat_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
    }

    let total_s = start.elapsed().as_secs_f64();
    lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let result = BenchResult {
        parser: "riven-rank.parse",
        n: titles.len(),
        total_s,
        throughput: if total_s > 0.0 {
            titles.len() as f64 / total_s
        } else {
            0.0
        },
        p50_ms: percentile(&lat_ms, 0.50),
        p95_ms: percentile(&lat_ms, 0.95),
    };

    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
