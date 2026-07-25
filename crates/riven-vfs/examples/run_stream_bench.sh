#!/usr/bin/env bash
# Re-runnable matrix for `stream_bench`, so a change can be compared against
# the run before it on identical conditions.
#
# Usage: run_stream_bench.sh <label>
set -uo pipefail

LABEL="${1:-run}"
cd "$(dirname "$0")/../../.." || exit 1

export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH:-}
export RIVEN_BENCH_NNTP_USER="${RIVEN_BENCH_NNTP_USER:?set RIVEN_BENCH_NNTP_USER}"
export RIVEN_BENCH_NNTP_PASS="${RIVEN_BENCH_NNTP_PASS:?set RIVEN_BENCH_NNTP_PASS}"
export RUST_LOG="${RUST_LOG:-error}"

SPR=nzb-c3aeb4528f3be698f743d4b9a911c0aef49821f7   # Saving Private Ryan UHD, 44.7 GB, ~35 Mbps
ENDGAME=nzb-9e69d3e0f77bf100aa000961000c378d09ac3710 # Avengers: Endgame UHD, 57.9 GB, ~43 Mbps

cargo build --release -p riven-vfs --example stream_bench 2>&1 | tail -2
BIN=./target/release/examples/stream_bench

run() { # title hash bitrate handles start_frac
    "$BIN" --info-hash "$2" --file-index 0 --seconds 45 \
        --handles "$4" --start-frac "$5" --bitrate-mbps "$3" \
        --label "$LABEL | $1 | handles=$4 | start=$5" 2>&1 | grep -Ev '^\s*$'
}

run "Saving Private Ryan UHD" "$SPR"     35 1 0.35
run "Saving Private Ryan UHD" "$SPR"     35 2 0.55
run "Saving Private Ryan UHD" "$SPR"     35 3 0.70
run "Avengers Endgame UHD"    "$ENDGAME" 43 2 0.40
