#!/usr/bin/env bash
# Controlled first-100k CGP0 comparison at high drop rates only.
# Usage: run_cgp0_highdrop_score_comparison.sh WORKER_LABEL CPD [CPD ...]
set -u

if (( $# < 2 )); then
  echo "usage: $0 WORKER_LABEL CPD [CPD ...]" >&2
  exit 2
fi

label=$1
shift

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
bin="$repo_dir/output/trace_recon_score_compare"
out_dir="$repo_dir/output/cgp0_highdrop_score_comparison_first100k"
result_dir="$out_dir/results"
corpus=/mydata/uber/bignode_state/day1_unfilt_corpus
trace_store=/mydata/uber/day1.store
rates=0.75,0.95,1
workers=${MATRIX_WORKERS:-8}

mkdir -p "$result_dir"
status="$out_dir/worker_${label}.status"
: > "$status"

echo "WORKER $label START $(date --iso-8601=seconds) CPDS $*" | tee -a "$status"

for cpd in "$@"; do
  for prime in up none; do
    prime_opt=()
    if [[ $prime == up ]]; then
      prime_opt=(--prime-m)
    fi
    for recon_mode in maximal legacy; do
      recon_opt=()
      if [[ $recon_mode == legacy ]]; then
        recon_opt=(--cgp0-legacy)
      fi
      stem="$result_dir/${recon_mode}_${prime}_cpd${cpd}"
      echo "START $recon_mode $prime CPD $cpd $(date --iso-8601=seconds)" | tee -a "$status"
      /usr/bin/time -v "$bin" \
        --corpus "$corpus" \
        --trace-store "$trace_store" \
        --mode cgp0 \
        --checkpoint-distance "$cpd" \
        --prefix-len 8 \
        --bloom-fp 0.0001 \
        "${prime_opt[@]}" \
        "${recon_opt[@]}" \
        --drop-rates "$rates" \
        --seed 42 \
        --per-trace-drop-seed \
        --trace-count 100000 \
        --workers "$workers" \
        --compare-scorers \
        --output "$stem.json" \
        > "$stem.stdout" 2> "$stem.log"
      rc=$?
      echo "$rc" > "$stem.exit"
      echo "DONE $recon_mode $prime CPD $cpd RC $rc $(date --iso-8601=seconds)" | tee -a "$status"
      if (( rc != 0 )); then
        echo "WORKER $label FAILED $recon_mode $prime CPD $cpd" | tee -a "$status"
        exit "$rc"
      fi
    done
  done
done

echo "WORKER $label ALLDONE $(date --iso-8601=seconds)" | tee -a "$status"
