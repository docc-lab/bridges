#!/usr/bin/env bash
# Exact timing rerun of a detached-worker slice of the canonical first-100k matrix.
# Usage: run_first100k_recon_timing.sh WORKER_LABEL CPD [CPD ...]
set -u

if (( $# < 2 )); then
  echo "usage: $0 WORKER_LABEL CPD [CPD ...]" >&2
  exit 2
fi

label=$1
shift

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
bin="$repo_dir/output/trace_recon_first100k_matrix"
matrix_dir="$repo_dir/output/reconstruction_matrix_first100k_prime_up_ha_safe"
timing_dir="$matrix_dir/timing"
corpus=/mydata/uber/bignode_state/day1_unfilt_corpus
trace_store=/mydata/uber/day1.store
rates=0.05,0.25,0.5,0.75,0.95,1
workers=${MATRIX_WORKERS:-8}
export TRACE_RECON_SB3_HARD_TIDS=1
export TRACE_RECON_GREEDY_HARD_TIDS=1

mkdir -p "$timing_dir"
status="$timing_dir/worker_${label}.status"
: > "$status"

echo "WORKER $label START $(date --iso-8601=seconds) CPDS $*" | tee -a "$status"

for cpd in "$@"; do
  for mode in sb3 cgp0 pb0; do
    stem="$timing_dir/${mode}_cpd${cpd}"
    extra=()
    if [[ $mode == sb3 ]]; then
      extra=(--fp-bits 64 --lehmer-ee)
    fi

    echo "START $mode CPD $cpd $(date --iso-8601=seconds)" | tee -a "$status"
    /usr/bin/time -v "$bin" \
      --corpus "$corpus" \
      --trace-store "$trace_store" \
      --mode "$mode" \
      --checkpoint-distance "$cpd" \
      --prefix-len 8 \
      --bloom-fp 0.0001 \
      --prime-m \
      --drop-rates "$rates" \
      --seed 42 \
      --per-trace-drop-seed \
      --trace-count 100000 \
      --workers "$workers" \
      "${extra[@]}" \
      --timing "$stem"'_{dc}.csv' \
      --output "$stem.json" \
      > "$stem.stdout" 2> "$stem.log"
    rc=$?
    echo "$rc" > "$stem.exit"
    echo "DONE $mode CPD $cpd RC $rc $(date --iso-8601=seconds)" | tee -a "$status"
    if (( rc != 0 )); then
      echo "WORKER $label FAILED $mode CPD $cpd" | tee -a "$status"
      exit "$rc"
    fi
  done
done

echo "WORKER $label ALLDONE $(date --iso-8601=seconds)" | tee -a "$status"
