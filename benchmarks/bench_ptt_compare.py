#!/usr/bin/env python3
import csv
import json
import os
import platform
import random
import statistics
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass
class BenchResult:
    mode: str
    parser: str
    n: int
    total_s: float
    throughput: float
    p50_ms: float
    p95_ms: float


ROOT = Path(__file__).resolve().parents[1]
PTT_ROOT = Path("/Users/oliverwalton/Desktop/PTT-main")
BENCH_DATE = date.today().isoformat()
CSV_PATH = ROOT / "benchmarks" / f"ptt_vs_rust_{BENCH_DATE}.csv"
README_PATH = ROOT / "benchmarks" / "README.md"


def make_titles(n: int) -> list[str]:
    base = [
        "The.Walking.Dead.S05E03.1080p.WEB-DL.DD5.1.H264-ASAP",
        "Oppenheimer.2023.2160p.REMUX.DV.HDR10Plus.TrueHD.7.1.HEVC",
        "Game.of.Thrones.S01E01.720p.HDTV.x264",
        "The.Simpsons.S01E01E02.1080p.BluRay.x265.10bit.AAC.5.1",
        "House.MD.All.Seasons.1-8.720p.Ultra-Compressed",
    ]
    random.seed(1337)
    return [f"{random.choice(base)}.{i:05d}" for i in range(n)]


def bench_upstream_ptt(n: int) -> BenchResult:
    sys.path.insert(0, str(PTT_ROOT))
    try:
        from PTT import parse_title
    finally:
        sys.path.pop(0)

    titles = make_titles(n)
    lat = []
    start = statistics.fmean([0.0])  # keep import used consistently
    del start

    import time

    total_start = time.perf_counter()
    for title in titles:
        t0 = time.perf_counter()
        parse_title(title, False)
        lat.append((time.perf_counter() - t0) * 1000.0)
    total_s = time.perf_counter() - total_start

    return BenchResult(
        mode="upstream",
        parser="PTT.parse_title",
        n=n,
        total_s=total_s,
        throughput=n / total_s if total_s > 0 else 0.0,
        p50_ms=statistics.median(lat),
        p95_ms=statistics.quantiles(lat, n=20)[18] if len(lat) >= 20 else max(lat),
    )


def bench_rust_port(n: int) -> BenchResult:
    titles = make_titles(n)
    proc = subprocess.run(
        [
            "cargo",
            "run",
            "--quiet",
            "--release",
            "-p",
            "riven-rank",
            "--bin",
            "bench_parse_json",
        ],
        cwd=ROOT,
        input=json.dumps(titles),
        text=True,
        capture_output=True,
        check=True,
    )
    row = json.loads(proc.stdout)
    return BenchResult(
        mode="rust",
        parser=row["parser"],
        n=row["n"],
        total_s=row["total_s"],
        throughput=row["throughput"],
        p50_ms=row["p50_ms"],
        p95_ms=row["p95_ms"],
    )


def write_csv(rows: list[BenchResult]) -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["mode", "parser", "n", "total_s", "throughput", "p50_ms", "p95_ms"])
        for row in rows:
            writer.writerow(
                [
                    row.mode,
                    row.parser,
                    row.n,
                    f"{row.total_s:.6f}",
                    f"{row.throughput:.1f}",
                    f"{row.p50_ms:.3f}",
                    f"{row.p95_ms:.3f}",
                ]
            )


def rust_version() -> str:
    return subprocess.check_output(["rustc", "--version"], text=True, cwd=ROOT).strip()


def environment_block() -> str:
    return "\n".join(
        [
            f"- Host: `{platform.platform()}`",
            f"- CPU: `{platform.processor() or 'unknown'}`",
            f"- Python: `{sys.version.split()[0]}`",
            f"- Rust: `{rust_version().replace('rustc ', '')}`",
        ]
    )


def render_readme(rows: list[BenchResult]) -> str:
    upstream_by_n = {row.n: row for row in rows if row.mode == "upstream"}
    rust_by_n = {row.n: row for row in rows if row.mode == "rust"}

    lines = [f"# Benchmarks ({BENCH_DATE})", "", "## Environment", "", environment_block(), ""]
    lines.extend(
        [
            "## Python API: upstream PTT vs this repo",
            "",
            f"Source data: [`{CSV_PATH.name}`]({CSV_PATH.name})",
            "",
            "| Parser | N | Upstream (items/s) | Rust port (items/s) | Speedup | Upstream p50 (ms) | Rust p50 (ms) | Upstream p95 (ms) | Rust p95 (ms) |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )

    speedups = []
    for n in sorted(upstream_by_n):
        upstream = upstream_by_n[n]
        rust = rust_by_n[n]
        speedup = rust.throughput / upstream.throughput if upstream.throughput else 0.0
        speedups.append(speedup)
        lines.append(
            f"| `PTT.parse_title` | {n:,} | {upstream.throughput:,.1f} | {rust.throughput:,.1f} | {speedup:.2f}x | {upstream.p50_ms:.3f} | {rust.p50_ms:.3f} | {upstream.p95_ms:.3f} | {rust.p95_ms:.3f} |"
        )

    if speedups:
        geometric_mean = 1.0
        for value in speedups:
            geometric_mean *= value
        geometric_mean **= 1.0 / len(speedups)
        lines.extend(["", f"Geometric mean throughput speedup (all rows): **{geometric_mean:.2f}x**.", ""])

    lines.extend(
        [
            "## Commands Used",
            "",
            "```bash",
            "python3 benchmarks/bench_ptt_compare.py",
            "cargo bench -p riven-rank --bench parse_bench",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    if not PTT_ROOT.exists():
        raise SystemExit(f"PTT repo not found at {PTT_ROOT}")

    sizes = [1000, 10000, 30000]
    rows: list[BenchResult] = []
    for n in sizes:
        rows.append(bench_upstream_ptt(n))
        rows.append(bench_rust_port(n))

    write_csv(rows)
    README_PATH.write_text(render_readme(rows))
    print(f"Wrote {CSV_PATH}")
    print(f"Wrote {README_PATH}")


if __name__ == "__main__":
    main()
