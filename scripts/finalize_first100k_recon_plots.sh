#!/usr/bin/env bash
# Validate exact timing reruns and render all first-100k reconstruction figures.
set -euo pipefail

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
matrix_dir="$repo_dir/output/reconstruction_matrix_first100k_prime_up_ha_safe"
timing_dir="$matrix_dir/timing"
figure_dir="$matrix_dir/figures"

for model in pb0 cgp0 sb3; do
  for cpd in 3 4 5 6 7 8; do
    test "$(tr -d '\n' < "$timing_dir/${model}_cpd${cpd}.exit")" = 0
    cmp "$matrix_dir/${model}_cpd${cpd}.json" "$timing_dir/${model}_cpd${cpd}.json"
  done
done

python3 "$repo_dir/scripts/repro/plot_recon_matrix_error_bydrop.py" \
  "$matrix_dir" "$figure_dir"
python3 "$repo_dir/scripts/repro/plot_recon_matrix_time_violin.py" \
  --rebuild "$timing_dir" "$figure_dir"

echo "DONE $(date --iso-8601=seconds)" > "$timing_dir/finalize.status"
