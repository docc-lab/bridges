#!/usr/bin/env bash
# Wait for the three detached timing workers, then validate and render figures.
set -euo pipefail

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
timing_dir="$repo_dir/output/reconstruction_matrix_first100k_prime_up_ha_safe/timing"

while true; do
  if grep -q FAILED "$timing_dir"/worker_*.status 2>/dev/null; then
    echo "A timing worker failed; not rendering figures." >&2
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

exec "$repo_dir/scripts/finalize_first100k_recon_plots.sh"
