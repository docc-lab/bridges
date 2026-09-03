#!/usr/bin/env bash
# Prime-up, high-drop CGP0 chain-evidence experiment on the first 100k Day-1 traces.
# Usage: run_cgp0_chain_evidence.sh WORKER_LABEL CPD [CPD ...]
set -u

if (( $# < 2 )); then
  echo "usage: $0 WORKER_LABEL CPD [CPD ...]" >&2
  exit 2
fi

label=$1
shift

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
bin="$repo_dir/output/trace_recon_chain_stats"
out_dir="$repo_dir/output/cgp0_chain_evidence_first100k_prime_up"
result_dir="$out_dir/results"
corpus=/mydata/uber/bignode_state/day1_unfilt_corpus
trace_store=/mydata/uber/day1.store
workers=${MATRIX_WORKERS:-8}

mkdir -p "$result_dir"
status="$out_dir/worker_${label}.status"
: > "$status"

echo "WORKER $label START $(date --iso-8601=seconds) CPDS $*" | tee -a "$status"
for cpd in "$@"; do
  stem="$result_dir/cgp0_prime_up_cpd${cpd}"
  echo "START CPD $cpd $(date --iso-8601=seconds)" | tee -a "$status"
  /usr/bin/time -v "$bin" \
    --corpus "$corpus" \
    --trace-store "$trace_store" \
    --mode cgp0 \
    --checkpoint-distance "$cpd" \
    --prefix-len 8 \
    --bloom-fp 0.0001 \
    --prime-m \
    --drop-rates 0.75,0.95,1 \
    --seed 42 \
    --per-trace-drop-seed \
    --trace-count 100000 \
    --workers "$workers" \
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
