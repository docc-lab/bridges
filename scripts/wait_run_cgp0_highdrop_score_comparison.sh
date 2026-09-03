#!/usr/bin/env bash
# Preserve the in-flight timing experiment, then run the controlled high-drop
# score/reconstructor decomposition in three detached worker slices.
set -euo pipefail

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
timing_dir="$repo_dir/output/reconstruction_matrix_first100k_prime_up_ha_safe/timing"
out_dir="$repo_dir/output/cgp0_highdrop_score_comparison_first100k"
status="$out_dir/orchestrator.status"

mkdir -p "$out_dir"
: > "$status"
echo "WAITING_FOR_TIMING $(date --iso-8601=seconds)" | tee -a "$status"
sha256sum "$repo_dir/output/trace_recon_score_compare" | tee -a "$status"

while true; do
  if grep -q FAILED "$timing_dir"/worker_*.status 2>/dev/null; then
    echo "TIMING_FAILED $(date --iso-8601=seconds)" | tee -a "$status"
    exit 1
  fi
  complete=0
  for label in a b c; do
    if grep -q ALLDONE "$timing_dir/worker_${label}.status" 2>/dev/null; then
      complete=$((complete + 1))
    fi
  done
  if (( complete == 3 )); then
    break
  fi
  sleep 60
done

echo "COMPARISON_START $(date --iso-8601=seconds)" | tee -a "$status"
MATRIX_WORKERS=8 "$repo_dir/scripts/run_cgp0_highdrop_score_comparison.sh" a 8 3 \
  > "$out_dir/worker_a.launch.log" 2>&1 &
pid_a=$!
MATRIX_WORKERS=8 "$repo_dir/scripts/run_cgp0_highdrop_score_comparison.sh" b 7 4 \
  > "$out_dir/worker_b.launch.log" 2>&1 &
pid_b=$!
MATRIX_WORKERS=8 "$repo_dir/scripts/run_cgp0_highdrop_score_comparison.sh" c 6 5 \
  > "$out_dir/worker_c.launch.log" 2>&1 &
pid_c=$!
printf '%s\n' "$pid_a" > "$out_dir/worker_a.pid"
printf '%s\n' "$pid_b" > "$out_dir/worker_b.pid"
printf '%s\n' "$pid_c" > "$out_dir/worker_c.pid"

rc=0
wait "$pid_a" || rc=1
wait "$pid_b" || rc=1
wait "$pid_c" || rc=1
if (( rc != 0 )); then
  echo "COMPARISON_FAILED $(date --iso-8601=seconds)" | tee -a "$status"
  exit 1
fi

python3 "$repo_dir/scripts/summarize_cgp0_highdrop_score_comparison.py" "$out_dir" \
  > "$out_dir/summarize.log" 2>&1
echo "ALLDONE $(date --iso-8601=seconds)" | tee -a "$status"
