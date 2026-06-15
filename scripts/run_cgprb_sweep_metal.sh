#!/usr/bin/env bash
# Full CGPRB sweep on a single big box (e.g. AWS c6a.metal, 192 vCPU).
# For each requested day it builds a trace-store directly from the .arc archive
# (bounded memory, no events.bin) and runs the cpd x drop grid, wave-scheduled so
# each cell gets real parallelism. Per-cell TOPO lines are collected into summary.log.
#
# Prereqs on the box:
#   - Go (for `go build`)
#   - OR-Tools prebuilt extracted to the path baked into recon/cpsat_cgo.go, i.e.
#     /users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/  (x86 Ubuntu 22.04).
#     If your path differs, fix the #cgo lines or symlink it.
#   - The two .arc archives + their meta.bin copied locally (see ARCHIVES below).
#
# Usage: scripts/run_cgprb_sweep_metal.sh [day1] [day2]   (default: both)
set -u
cd "$(dirname "$0")/.."
REPO=$(pwd)

DATA=${DATA:-/mydata/uber}          # where the .arc + meta.bin live and stores get built
OUT=${OUT:-$HOME/cgprb_sweep_metal} # results
NPROC=${NPROC:-$(nproc)}
WORKERS=${WORKERS:-32}               # workers per cell
CONC=${CONC:-$(( NPROC / WORKERS ))} # concurrent cells (so CONC*WORKERS ~= NPROC)
[ "$CONC" -lt 1 ] && CONC=1
CPDS=(3 4 5 6 7 8)
DROPS=(0.05 0.25 0.5 0.75 0.95)

# label : archive : input-meta.bin  (edit if your filenames differ)
declare -A ARC=( [day1]="$DATA/corpus_full_unfiltered.arc" [day2]="$DATA/corpus_day2_unfiltered.arc" )
declare -A META=( [day1]="$DATA/corpus_full_unfiltered/meta.bin" [day2]="$DATA/corpus_day2_unfiltered/meta.bin" )

DAYS=("$@"); [ "${#DAYS[@]}" -eq 0 ] && DAYS=(day1 day2)
mkdir -p "$OUT"

echo "=== build binaries ==="
go build -tags cpsat -o bin/trace_recon_cgprb ./cmd/trace_recon || { echo "cpsat build failed (OR-Tools path?)"; exit 1; }
go build -o bin/archive_to_store ./cmd/archive_to_store || exit 1
echo "NPROC=$NPROC  WORKERS/cell=$WORKERS  concurrent cells=$CONC"

for day in "${DAYS[@]}"; do
  arc=${ARC[$day]}; metain=${META[$day]}
  store=$DATA/${day}.store; metadir=$DATA/${day}_meta
  if [ ! -f "$store" ]; then
    echo "=== [$day] build store from $arc ==="
    ./bin/archive_to_store --archive "$arc" --meta "$metain" --store "$store" --meta-out "$metadir" --progress 50000 || exit 1
  else
    echo "=== [$day] store exists, reusing $store ==="
  fi

  echo "=== [$day] sweep ($((${#CPDS[@]}*${#DROPS[@]})) cells, $CONC at a time) ==="
  for cpd in "${CPDS[@]}"; do
    for r in "${DROPS[@]}"; do
      (
        TRACE_RECON_TOPO=1 TRACE_RECON_CPSAT=1 ./bin/trace_recon_cgprb \
          --corpus "$metadir" --trace-store "$store" --mode cgprb \
          --checkpoint-distance "$cpd" --drop-rate "$r" --tie-policy aware --per-trace-drop-seed \
          --workers "$WORKERS" -o "$OUT/${day}_cpd${cpd}_r${r}.json" \
          2> "$OUT/${day}_cpd${cpd}_r${r}.err"
        topo=$(grep '^TOPO' "$OUT/${day}_cpd${cpd}_r${r}.err")
        echo "DONE $day cpd=$cpd r=$r | $topo" >> "$OUT/summary.log"
      ) &
      while [ "$(jobs -rp | wc -l)" -ge "$CONC" ]; do wait -n; done
    done
  done
  wait
  echo "=== [$day] done ==="
done
echo "SWEEP COMPLETE -> $OUT/summary.log"
sort "$OUT/summary.log"
