#!/usr/bin/env bash
# Exact timing rerun for the prime-up/drop-1 CGP0 fanout-correlation experiment.
# Usage: script WORKER_LABEL CPD [CPD ...]
set -u

if (( $# < 2 )); then
  echo "usage: $0 WORKER_LABEL CPD [CPD ...]" >&2
  exit 2
fi

label=$1
shift

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
bin="$repo_dir/output/trace_recon_fanout_stats"
out_dir="$repo_dir/output/cgp0_fanout_correlation_drop1_first100k_prime_up"
timing_dir="$out_dir/timing"
corpus=/mydata/uber/bignode_state/day1_unfilt_corpus
trace_store=/mydata/uber/day1.store
workers=${FANOUT_WORKERS:-8}

mkdir -p "$timing_dir"
status="$timing_dir/worker_${label}.status"
: > "$status"

echo "WORKER $label START $(date --iso-8601=seconds) CPDS $*" | tee -a "$status"
for cpd in "$@"; do
  stem="$timing_dir/cgp0_prime_up_drop1_cpd${cpd}"
  echo "START CPD $cpd $(date --iso-8601=seconds)" | tee -a "$status"
  TRACE_RECON_PROGRESS=10000 TRACE_RECON_WATCHDOG=120 /usr/bin/time -v "$bin" \
    --corpus "$corpus" \
    --trace-store "$trace_store" \
    --mode cgp0 \
    --checkpoint-distance "$cpd" \
    --prefix-len 8 \
    --bloom-fp 0.0001 \
    --prime-m \
    --drop-rate 1 \
    --seed 42 \
    --per-trace-drop-seed \
    --trace-count 100000 \
    --workers "$workers" \
    --timing "$stem.csv" \
    --output "$stem.json" \
    > "$stem.stdout" 2> "$stem.log"
  rc=$?
  echo "$rc" > "$stem.exit"
  echo "DONE CPD $cpd RC $rc $(date --iso-8601=seconds)" | tee -a "$status"
  if (( rc != 0 )); then
    exit "$rc"
  fi
done
echo "WORKER $label ALLDONE $(date --iso-8601=seconds)" | tee -a "$status"
