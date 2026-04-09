# Benchmarks (2026-04-09)

## Environment

- Host: `macOS-26.3-arm64-arm-64bit-Mach-O`
- CPU: `arm`
- Python: `3.14.2`
- Rust: `1.92.0 (ded5c06cf 2025-12-08) (Homebrew)`

## Python API: upstream PTT vs this repo

Source data: [`ptt_vs_rust_2026-04-09.csv`](ptt_vs_rust_2026-04-09.csv)

| Parser | N | Upstream (items/s) | Rust port (items/s) | Speedup | Upstream p50 (ms) | Rust p50 (ms) | Upstream p95 (ms) | Rust p95 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `PTT.parse_title` | 1,000 | 1,257.2 | 13,276.8 | 10.56x | 0.592 | 0.020 | 1.660 | 0.028 |
| `PTT.parse_title` | 10,000 | 2,581.7 | 37,486.4 | 14.52x | 0.380 | 0.020 | 0.463 | 0.026 |
| `PTT.parse_title` | 30,000 | 2,576.4 | 37,374.1 | 14.51x | 0.382 | 0.021 | 0.457 | 0.041 |

Geometric mean throughput speedup (all rows): **13.05x**.

## Commands Used

```bash
python3 benchmarks/bench_ptt_compare.py
cargo bench -p riven-rank --bench parse_bench
```
