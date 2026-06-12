#!/bin/bash
# Run the standard 4-cell evaluation grid (cpd x drop-rate) for one engine
# version. Usage: run_grid.sh <outdir-suffix>   e.g. run_grid.sh v8h2
# Results land in ~/pcrs_10k_<suffix>/; stderr (diagnostics) per cell.
set -u
V=${1:?usage: run_grid.sh <version-suffix>}
OUT=~/pcrs_10k_$V
mkdir -p "$OUT"
cd "$(dirname "$0")/.."
T0=$SECONDS
for cell in "3 0.5" "6 0.5" "3 0.05" "6 0.05"; do
  set -- $cell
  t0=$SECONDS
  TRACE_RECON_DEBUG=1 ./bin/trace_recon --corpus /mydata/uber/corpus_full --mode pcrs \
    --checkpoint-distance $1 --drop-rate $2 --bloom-fp 1e-4 --trace-count 10000 --workers 32 \
    -o "$OUT/cpd${1}_fp1e-4_r${2}.json" 2> "$OUT/cpd${1}_r${2}.err"
  echo "done cpd=$1 r=$2 in $((SECONDS-t0))s"
done
echo "$V grid complete in $((SECONDS-T0))s total"
