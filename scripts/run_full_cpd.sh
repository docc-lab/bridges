#!/usr/bin/env bash
# Run trace_prep + the three bridge handlers (PB / CGPB / SBridge) at CPD=2
# over the full sanitized Uber corpus. Outputs per-trace bagsize metrics.
#
# Idempotent: re-running rebuilds the binaries and overwrites results_full/.
# The corpus is reused if /mydata/uber/corpus_full/ already exists and isn't
# explicitly cleared.
set -euo pipefail

INPUT_DIR=/mydata/uber/traces/traces-sanitized/
CORPUS_DIR=/mydata/uber/corpus_full/
RESULTS_DIR=/mydata/uber/results_full/
CPD=$1

# Path to a Go installation; adjust if you've installed elsewhere.
export PATH="$HOME/.local/go/bin:$PATH"

mkdir -p "$CORPUS_DIR" "$RESULTS_DIR"

echo "=== STAGE 0: build binaries ==="
cd /users/tomislav/bridges
go build -o /tmp/trace_prep ./cmd/trace_prep/
go build -o /tmp/trace_sim_go ./cmd/trace_sim/
echo "binaries: /tmp/trace_prep, /tmp/trace_sim_go"
date

# echo
# echo "=== STAGE 1: prep corpus from $INPUT_DIR ==="
# date
# /tmp/trace_prep --input-dir "$INPUT_DIR" --output-dir "$CORPUS_DIR" --progress 1000
# echo "corpus contents:"
# ls -lah "$CORPUS_DIR"
# date

echo
echo "=== STAGE 2: run PB / CGPB / SBridge at CPD=$CPD ==="
for mode in pb cgpb sbridge; do
  out="$RESULTS_DIR/full_${mode}_cpd${CPD}.json"
  echo "--- $mode cpd=$CPD -> $out ---"
  t1=$(date +%s)
  /tmp/trace_sim_go --corpus "$CORPUS_DIR" --bagsize --mode "$mode" --checkpoint-distance "$CPD" -o "$out"
  t2=$(date +%s)
  echo "  $mode took $((t2 - t1))s"
done

echo
echo "=== ALL DONE ==="
ls -lah "$RESULTS_DIR"
date
