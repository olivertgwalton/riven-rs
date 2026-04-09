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

## Python API: upstream RTN vs Rust port

Source data: `/Users/oliverwalton/Desktop/rank-torrent-name-main/benchmarks/rtn_vs_rust_2026-04-09.csv`

| Parser | N | Upstream (items/s) | Rust port (items/s) | Speedup | Upstream p50 (ms) | Rust p50 (ms) | Upstream p95 (ms) | Rust p95 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `RTN.parse` | 1,000 | 2,186.3 | 10,935.7 | 5.00x | 0.440 | 0.022 | 0.624 | 0.046 |
| `RTN.parse` | 10,000 | 1,899.8 | 32,583.6 | 17.15x | 0.470 | 0.021 | 0.828 | 0.043 |
| `RTN.parse` | 30,000 | 1,734.7 | 35,495.9 | 20.46x | 0.501 | 0.022 | 0.966 | 0.050 |

Geometric mean throughput speedup (all rows): **12.06x**.

## Commands Used

```bash
python3 benchmarks/bench_ptt_compare.py
python3 benchmarks/bench_rtn_compare.py
cargo bench -p riven-rank --bench parse_bench
```
